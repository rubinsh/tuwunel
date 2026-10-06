use std::{collections::HashSet, sync::Arc};

use futures::{FutureExt, StreamExt, future::join};
use ruma::{
	EventId, RoomId, UserId,
	api::client::push::ProfileTag,
	events::{
		AnySyncTimelineEvent, GlobalAccountDataEventType, TimelineEventType,
		push_rules::PushRulesEvent, room::power_levels::RoomPowerLevels,
	},
	push::{Action, Actions, HighlightTweakValue, Ruleset, Tweak},
	serde::Raw,
};
use serde::{Deserialize, Serialize};
use tracing::Level;
use tuwunel_core::{
	Result, implement,
	matrix::{
		event::Event,
		pdu::{Count, Pdu, PduId, RawPduId},
	},
	trace,
	utils::{BoolExt, ReadyExt, future::TryExtExt, result::ErrLog, time::now_millis},
};
use tuwunel_database::{Deserialized, Json, Map};

use super::{Evaluate, RelatedEvents};
use crate::rooms::short::ShortRoomId;

/// Compact metadata stored for each notified event.
///
/// The database key supplies the user and PDU count. The stored `ShortRoomId`
/// combines with that count to reconstruct the event's `PduId`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Notified {
	/// Milliseconds time at which the event notification was sent.
	pub ts: u64,

	/// ShortRoomId
	pub sroomid: ShortRoomId,

	/// The profile tag of the rule that matched this event.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tag: Option<ProfileTag>,

	/// Actions vector
	pub actions: Actions,
}

/// The values of one appended event shared by every recipient of it.
///
/// Each is resolved once per event and read once per recipient, so grouping
/// them keeps the per-recipient call from growing an argument per lookup.
#[derive(Clone, Copy)]
struct Appended<'a> {
	pdu_id: &'a RawPduId,
	pdu: &'a Pdu,
	power_levels: Option<&'a RoomPowerLevels>,
	serialized: &'a Raw<AnySyncTimelineEvent>,
	thread_root: Option<&'a EventId>,
	related_events: Option<&'a Arc<RelatedEvents>>,
}

/// Called by timeline append_pdu.
#[implement(super::Service)]
#[tracing::instrument(name = "append", level = "debug", skip_all)]
pub(crate) async fn append_pdu(&self, pdu_id: RawPduId, pdu: &Pdu) -> Result {
	let push_target = self
		.services
		.state_cache
		.active_local_users_in_room(pdu.room_id())
		.map(ToOwned::to_owned)
		.ready_filter(|user| *user != pdu.sender())
		.filter_map(async |recipient_user| {
			self.services
				.users
				.user_is_ignored(pdu.sender(), &recipient_user)
				.await
				.is_false()
				.then_some(recipient_user)
		})
		.collect::<HashSet<_>>();

	let power_levels = self
		.services
		.state_accessor
		.get_power_levels(pdu.room_id())
		.ok();

	let (mut push_target, power_levels) = join(push_target, power_levels).boxed().await;

	if *pdu.kind() == TimelineEventType::RoomMember
		&& let Some(Ok(target_user_id)) = pdu.state_key().map(UserId::parse)
		&& self
			.services
			.users
			.is_active_local(&target_user_id)
			.await
	{
		push_target.insert(target_user_id);
	}

	if push_target.is_empty() {
		return Ok(());
	}

	let serialized = pdu.to_format();
	let (thread_root, related_events) =
		join(self.services.threads.get_thread_id(pdu), self.related_events(pdu)).await;

	let appended = Appended {
		pdu_id: &pdu_id,
		pdu,
		power_levels: power_levels.as_ref(),
		serialized: &serialized,
		thread_root: thread_root.as_deref(),
		related_events: related_events.as_ref(),
	};

	let _cork = self.db.db.cork();
	for user in &push_target {
		self.append_pdu_for_user(user, appended).await;
	}

	Ok(())
}

#[implement(super::Service)]
async fn append_pdu_for_user(
	&self,
	user: &UserId,
	Appended {
		pdu_id,
		pdu,
		power_levels,
		serialized,
		thread_root,
		related_events,
	}: Appended<'_>,
) {
	let rules_for_user = self
		.services
		.account_data
		.get_global(user, GlobalAccountDataEventType::PushRules)
		.await
		.log_err(Level::TRACE)
		.map_or_else(|_| Ruleset::server_default(user), |ev: PushRulesEvent| ev.content.global);

	let actions = self
		.get_actions(Evaluate {
			user,
			ruleset: &rules_for_user,
			power_levels,
			pdu: serialized,
			room_id: pdu.room_id(),
			related_events,
		})
		.await;

	let (notify, highlight) = notify_and_highlight(actions);

	trace!(
		%user,
		event_id = %pdu.event_id(),
		actions = %actions.len(),
		notify,
		highlight,
		"Push rules evaluated",
	);

	if notify || highlight {
		self.count_notified(
			user,
			pdu_id,
			pdu.room_id(),
			thread_root,
			actions,
			(notify, highlight),
		)
		.await;
	}

	if notify || highlight || self.services.config.push_everything {
		self.get_pushkeys(user)
			.map(ToOwned::to_owned)
			.ready_for_each(|push_key| {
				self.services
					.sending
					.send_pdu_push(pdu_id, user, push_key)
					.log_err(Level::TRACE)
					.ok();
			})
			.await;
	}
}

/// Whether push actions notify, and whether they highlight.
pub(super) fn notify_and_highlight(actions: &[Action]) -> (bool, bool) {
	let notify = actions.iter().any(Action::should_notify);

	let highlight = actions.iter().any(|action| {
		matches!(action, Action::SetTweak(Tweak::Highlight(HighlightTweakValue::Yes)))
	});

	(notify, highlight)
}

/// Records a notified event and counts it, in its thread's bucket or the
/// room's.
///
/// Both happen under the room's count locks, so a read that recounts from the
/// records (`notification.rs`) sees every counted event or none of it.
#[implement(super::Service)]
async fn count_notified(
	&self,
	user: &UserId,
	pdu_id: &RawPduId,
	room_id: &RoomId,
	thread_root: Option<&EventId>,
	actions: &[Action],
	(notify, highlight): (bool, bool),
) {
	let key = (room_id.to_owned(), user.to_owned());
	let _notification = self.notification_increment_mutex.lock(&key).await;
	let _highlight = self.highlight_increment_mutex.lock(&key).await;

	let id: PduId = (*pdu_id).into();
	if matches!(id.count, Count::Normal(_)) {
		let notified = Notified {
			ts: now_millis(),
			sroomid: id.shortroomid,
			tag: None,
			actions: actions.into(),
		};

		self.db
			.useridcount_notification
			.put((user, id.count.into_unsigned()), Json(notified));
	}

	// Mutually-exclusive partition: each notify (and each highlight)
	// lands in either the room-level or thread bucket, never both.
	let notifications = &self.db.userroomid_notificationcount;
	let highlights = &self.db.userroomid_highlightcount;
	match thread_root {
		| None => {
			let main_notify = notify.then_async(|| increment(notifications, (user, room_id)));
			let main_highlight = highlight.then_async(|| increment(highlights, (user, room_id)));
			join(main_notify, main_highlight).await;
		},
		| Some(root) => {
			let thread_notify =
				notify.then_async(|| increment_thread(notifications, (user, room_id, root)));
			let thread_highlight =
				highlight.then_async(|| increment_thread(highlights, (user, room_id, root)));
			join(thread_notify, thread_highlight).await;
		},
	}
}

async fn increment(db: &Arc<Map>, key: (&UserId, &RoomId)) {
	let old: u64 = db.qry(&key).await.deserialized().unwrap_or(0);
	let new = old.saturating_add(1);
	db.put(key, new);
}

async fn increment_thread(db: &Arc<Map>, key: (&UserId, &RoomId, &EventId)) {
	let old: u64 = db.qry(&key).await.deserialized().unwrap_or(0);
	let new = old.saturating_add(1);
	db.put(key, new);
}
