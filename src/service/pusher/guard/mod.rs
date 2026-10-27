//! Notification guard: per account, per device and per room alert rules.
//!
//! Runs in [`send_push_notice`](super::Service::send_push_notice) after the
//! push rules decide to notify and before the gateway is called, so it sees
//! every alert, including the flush of deferred pushes. Badge-only pushes and
//! unread counts never pass through it. A drop is final for that pusher and
//! event: no alert is sent and nothing retries it. In its place the pusher
//! gets a counts-only refresh, so the badge still follows the arrival. The
//! guard fails open: a
//! setting it cannot read falls back to the defaults, which deliver as the
//! server did before the guard existed.

mod activity;
mod memory;
mod schedule;
mod settings;
#[cfg(test)]
mod tests;

use futures::{StreamExt, future::join4};
use ruma::{DeviceId, RoomId, UserId, api::client::push::Pusher};
use serde::{Deserialize, Deserializer, de::DeserializeOwned};
use serde_json::Value;
use tuwunel_core::{
	debug, implement,
	matrix::Event,
	utils::{millis_since_unix_epoch, stream::ReadyExt},
};

pub use self::activity::ACTIVITY_TTL_MS;
use self::{
	activity::Activity,
	memory::{Burst, Memory},
	settings::{
		BOTS_KEY, DEFAULTS_KEY, Level, Parsed, ROOMS_KEY, SCHEDULE_KEY, Settings, parse_stored,
	},
};

#[derive(Default)]
pub(super) struct Guard {
	memory: Memory,
	activity: Activity,
}

/// Why an alert was not sent; logged as its code, never with content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Reason {
	/// Dropped earlier for this pusher; a retry stays dropped.
	DroppedBefore,
	ViewedElsewhere,
	RoomMuted,
	RoomSnoozed,
	ThreadMuted,
	BotFiltered,
	NotMentioned,
	Paused,
	QuietHours,
	Burst,
}

/// The guard's decision for one event and one pusher.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Verdict {
	/// Send it. `burst` means it holds its room's burst slot, which must be
	/// released if the send fails.
	Send {
		burst: bool,
	},
	Drop(Reason),
}

/// The parts of an event's content the rules read.
#[derive(Debug, Default, Deserialize)]
struct Content {
	/// `Some` whenever the key is present, even as `null`.
	#[serde(default, rename = "m.mentions", deserialize_with = "present")]
	mentions: Option<Value>,

	#[serde(default)]
	body: Option<Value>,

	#[serde(default, rename = "com.chat-harness.sender_kind")]
	sender_kind: Option<Value>,
}

impl Reason {
	pub(super) const fn code(self) -> &'static str {
		match self {
			| Self::DroppedBefore => "dropped_before",
			| Self::ViewedElsewhere => "viewed_elsewhere",
			| Self::RoomMuted => "room_muted",
			| Self::RoomSnoozed => "room_snoozed",
			| Self::ThreadMuted => "thread_muted",
			| Self::BotFiltered => "bot_filtered",
			| Self::NotMentioned => "not_mentioned",
			| Self::Paused => "paused",
			| Self::QuietHours => "quiet_hours",
			| Self::Burst => "burst",
		}
	}
}

/// Decide whether `pusher` alerts `user_id` about `event`, judged now.
#[implement(super::Service)]
pub(super) async fn guard_notice<E: Event>(
	&self,
	user_id: &UserId,
	pusher: &Pusher,
	event: &E,
) -> Verdict {
	let now_ms = millis_since_unix_epoch();
	let pushkey = pusher.ids.pushkey.as_str();
	let memory = &self.guard.memory;

	if memory.was_dropped(user_id, pushkey, event.event_id(), now_ms) {
		return Verdict::Drop(Reason::DroppedBefore);
	}

	let settings = self.guard_settings(user_id).await;
	let verdict = self
		.guard_verdict(user_id, pushkey, &settings, event, now_ms)
		.await;

	if let Verdict::Drop(reason) = verdict {
		memory.record_drop(user_id, pushkey, event.event_id(), now_ms);
		debug!(
			%user_id,
			room_id = %event.room_id(),
			event_id = %event.event_id(),
			reason = reason.code(),
			"Push guard dropped a notice",
		);
	}

	verdict
}

/// Give back the burst slot of a notice whose send failed, so its retry can
/// alert.
#[implement(super::Service)]
pub(super) fn guard_send_failed<E: Event>(&self, user_id: &UserId, pusher: &Pusher, event: &E) {
	self.guard.memory.release_burst(
		user_id,
		&pusher.ids.pushkey,
		event.room_id(),
		event.event_id(),
	);
}

/// Forget the guard's decisions about a pusher that was deleted or replaced.
#[implement(super::Service)]
pub(super) fn guard_forget_pusher(&self, user_id: &UserId, pushkey: &str) {
	self.guard.memory.forget_pusher(user_id, pushkey);
}

/// Record that `device_id` views `room_id` (a heartbeat), or no room (a
/// clear). Returns how long a heartbeat counts, in ms.
#[implement(super::Service)]
pub fn report_activity(
	&self,
	user_id: &UserId,
	device_id: &DeviceId,
	room_id: Option<&RoomId>,
	seq: u64,
) -> u64 {
	self.guard
		.activity
		.report(user_id, device_id, room_id, seq, millis_since_unix_epoch());

	ACTIVITY_TTL_MS
}

/// The rules in order; the first that drops wins (spec §4).
#[implement(super::Service)]
async fn guard_verdict<E: Event>(
	&self,
	user_id: &UserId,
	pushkey: &str,
	settings: &Settings,
	event: &E,
	now_ms: u64,
) -> Verdict {
	let room_id = event.room_id();
	let sender = event.sender().as_str();

	if settings.active_device_suppression()
		&& let Ok(device_id) = self.get_pusher_device(pushkey).await
		&& self
			.guard
			.activity
			.viewed_elsewhere(user_id, room_id, &device_id, now_ms)
	{
		return Verdict::Drop(Reason::ViewedElsewhere);
	}

	let room = settings.room(room_id.as_str());
	let level = match room.and_then(|room| room.level) {
		| Some(level) => level,
		| None => match settings.bot_room_level() {
			| Some(level) if self.is_bot_room(user_id, room_id, settings).await => level,
			| _ => settings.account_level(),
		},
	};

	if level == Level::Muted {
		return Verdict::Drop(Reason::RoomMuted);
	}

	if let Some(room) = room {
		if room
			.snooze_until
			.is_some_and(|until| until > now_ms)
		{
			return Verdict::Drop(Reason::RoomSnoozed);
		}

		if (room.mute_all_threads || !room.muted_threads.is_empty())
			&& let Some(thread) = self.services.threads.get_thread_id(event).await
			&& (room.mute_all_threads || room.muted_threads.contains(thread.as_str()))
		{
			return Verdict::Drop(Reason::ThreadMuted);
		}
	}

	let content: Content = event.get_content().unwrap_or_default();
	match level {
		| Level::Humans if is_bot_sender(settings, sender, &content) =>
			return Verdict::Drop(Reason::BotFiltered),
		| Level::Mentions if !is_mentioned(user_id, &content) =>
			return Verdict::Drop(Reason::NotMentioned),
		| _ => {},
	}

	let schedule = &settings.schedule;
	if schedule.paused(now_ms) {
		return Verdict::Drop(Reason::Paused);
	}

	if schedule.quiet(now_ms) && !schedule.breaks_through(room_id.as_str(), sender) {
		return Verdict::Drop(Reason::QuietHours);
	}

	let Some(window_ms) = settings.burst_window_ms() else {
		return Verdict::Send { burst: false };
	};

	match self.guard.memory.take_burst(
		user_id,
		pushkey,
		room_id,
		event.event_id(),
		window_ms,
		now_ms,
	) {
		| Burst::Taken => Verdict::Send { burst: true },
		| Burst::Held => Verdict::Drop(Reason::Burst),
	}
}

/// Read the four settings keys, each falling back to its defaults on its own.
#[implement(super::Service)]
async fn guard_settings(&self, user_id: &UserId) -> Settings {
	let (defaults, rooms, schedule, bots) = join4(
		self.guard_key(user_id, DEFAULTS_KEY),
		self.guard_key(user_id, ROOMS_KEY),
		self.guard_key(user_id, SCHEDULE_KEY),
		self.guard_key(user_id, BOTS_KEY),
	)
	.await;

	let settings = Settings {
		defaults: defaults.into_option(),
		rooms: rooms.into_option().unwrap_or_default(),
		schedule: schedule.into_option().unwrap_or_default(),
		bots: bots.into_option().unwrap_or_default(),
	};

	if settings.schedule.timezone_dropped {
		debug!(%user_id, key = SCHEDULE_KEY, reason = "timezone", "guard_degraded");
	}

	settings
}

#[implement(super::Service)]
async fn guard_key<T: DeserializeOwned>(&self, user_id: &UserId, key: &'static str) -> Parsed<T> {
	let parsed = match self
		.services
		.account_data
		.get_raw(None, user_id, key)
		.await
	{
		| Ok(stored) => parse_stored(Some(&stored)),
		| Err(e) if e.is_not_found() => Parsed::Absent,
		| Err(_) => Parsed::Degraded("read_error"),
	};

	if let Some(reason) = parsed.degraded() {
		debug!(%user_id, key, reason, "guard_degraded");
	}

	parsed
}

/// A bot room (D11): every other joined member is on the bot list. A room
/// with more other members than the list has names cannot be one, so the scan
/// reads at most one more member than that: 501 at the list's cap.
#[implement(super::Service)]
async fn is_bot_room(&self, user_id: &UserId, room_id: &RoomId, settings: &Settings) -> bool {
	let bots = settings.bots.users.len();
	if bots == 0 {
		return false;
	}

	let others: Vec<_> = self
		.services
		.state_cache
		.room_members(room_id)
		.ready_filter(|member| *member != user_id)
		.map(ToOwned::to_owned)
		.take(bots.saturating_add(1))
		.collect()
		.await;

	!others.is_empty()
		&& others.len() <= bots
		&& others
			.iter()
			.all(|member| settings.is_bot(member.as_str()))
}

/// D3: on the bot list, or marked as a bot by the sender itself.
fn is_bot_sender(settings: &Settings, sender: &str, content: &Content) -> bool {
	settings.is_bot(sender)
		|| content
			.sender_kind
			.as_ref()
			.and_then(Value::as_str)
			== Some("bot")
}

/// D4 (spec §5.1): named in `m.mentions.user_ids`. Only an event without
/// `m.mentions` falls back to the full user ID in the body, outside quoted
/// reply lines. `@room` is not a mention.
fn is_mentioned(user_id: &UserId, content: &Content) -> bool {
	let user_id = user_id.as_str();

	if let Some(mentions) = &content.mentions {
		return mentions
			.get("user_ids")
			.and_then(Value::as_array)
			.is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(user_id)));
	}

	content
		.body
		.as_ref()
		.and_then(Value::as_str)
		.is_some_and(|body| {
			body.lines()
				.filter(|line| !line.starts_with("> "))
				.any(|line| line.contains(user_id))
		})
}

fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
	Value::deserialize(deserializer).map(Some)
}
