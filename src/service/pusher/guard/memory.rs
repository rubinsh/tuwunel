//! The guard's in-memory decisions: terminal drops and burst slots.
//!
//! Neither survives a restart, and both are bounded. Losing a drop record
//! lets a later retry of that event be judged again; losing a burst slot
//! allows at most one extra alert. Both err towards delivering.

use std::{
	collections::{HashMap, VecDeque},
	sync::{Mutex, MutexGuard, PoisonError},
};

use ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId};

/// How long a drop stays terminal.
pub(super) const DROP_TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// The most drops remembered at once; the oldest go first.
pub(super) const DROP_CAP: usize = 65_536;

/// The most burst slots held at once. Past it, slots older than the longest
/// window a client may set are reaped, and if that is not enough every slot is
/// released.
pub(super) const BURST_CAP: usize = 16_384;

/// The longest burst window a client may set, in ms.
const MAX_BURST_WINDOW_MS: u64 = 86_400_000;

/// One pusher of one user: `(user, pushkey)`.
type PusherKey = (OwnedUserId, String);

#[derive(Default)]
pub(super) struct Memory {
	dropped: Mutex<Dropped>,
	bursts: Mutex<HashMap<(PusherKey, OwnedRoomId), Slot>>,
}

#[derive(Default)]
struct Dropped {
	at: HashMap<(PusherKey, OwnedEventId), u64>,
	order: VecDeque<((PusherKey, OwnedEventId), u64)>,
}

/// The event that last alerted for one pusher and room, and when. Its age is
/// judged against the window in force at each decision, so a changed window
/// applies at once.
#[derive(Clone, Debug)]
struct Slot {
	event_id: OwnedEventId,
	at_ms: u64,
}

/// Whether a burst check let the event through.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Burst {
	/// The event holds the slot now, and must release it if the send fails.
	Taken,
	/// Another event alerted from this room inside the window.
	Held,
}

impl Memory {
	pub(super) fn was_dropped(
		&self,
		user_id: &UserId,
		pushkey: &str,
		event_id: &EventId,
		now_ms: u64,
	) -> bool {
		let key = ((user_id.to_owned(), pushkey.to_owned()), event_id.to_owned());

		lock(&self.dropped)
			.at
			.get(&key)
			.is_some_and(|&at| now_ms.saturating_sub(at) < DROP_TTL_MS)
	}

	pub(super) fn record_drop(
		&self,
		user_id: &UserId,
		pushkey: &str,
		event_id: &EventId,
		now_ms: u64,
	) {
		let key = ((user_id.to_owned(), pushkey.to_owned()), event_id.to_owned());
		let mut dropped = lock(&self.dropped);

		dropped.at.insert(key.clone(), now_ms);
		dropped.order.push_back((key, now_ms));

		while let Some((key, at)) = dropped.order.front().cloned() {
			let expired = now_ms.saturating_sub(at) >= DROP_TTL_MS;
			if !expired && dropped.order.len() <= DROP_CAP {
				break;
			}

			dropped.order.pop_front();

			// A key dropped again later has a newer entry further back.
			if dropped.at.get(&key) == Some(&at) {
				dropped.at.remove(&key);
			}
		}
	}

	/// Take this room's burst slot for `event_id`, unless another event holds
	/// it inside its window. The same event always passes its own slot, so a
	/// retry of a failed send is not stopped by the attempt it repeats.
	pub(super) fn take_burst(
		&self,
		user_id: &UserId,
		pushkey: &str,
		room_id: &RoomId,
		event_id: &EventId,
		window_ms: u64,
		now_ms: u64,
	) -> Burst {
		let key = ((user_id.to_owned(), pushkey.to_owned()), room_id.to_owned());
		let mut bursts = lock(&self.bursts);

		if let Some(slot) = bursts.get(&key)
			&& now_ms.saturating_sub(slot.at_ms) < window_ms
		{
			// The holder keeps its slot as it was: a retry does not extend it.
			return match slot.event_id == event_id {
				| true => Burst::Taken,
				| false => Burst::Held,
			};
		}

		if bursts.len() >= BURST_CAP && !bursts.contains_key(&key) {
			bursts.retain(|_, slot| now_ms.saturating_sub(slot.at_ms) < MAX_BURST_WINDOW_MS);
			if bursts.len() >= BURST_CAP {
				bursts.clear();
			}
		}

		bursts.insert(key, Slot {
			event_id: event_id.to_owned(),
			at_ms: now_ms,
		});

		Burst::Taken
	}

	/// Give back a slot `event_id` took, after its send failed.
	pub(super) fn release_burst(
		&self,
		user_id: &UserId,
		pushkey: &str,
		room_id: &RoomId,
		event_id: &EventId,
	) {
		let key = ((user_id.to_owned(), pushkey.to_owned()), room_id.to_owned());
		let mut bursts = lock(&self.bursts);

		if bursts
			.get(&key)
			.is_some_and(|slot| slot.event_id == event_id)
		{
			bursts.remove(&key);
		}
	}

	/// Forget every decision about one pusher, when it is deleted or replaced.
	pub(super) fn forget_pusher(&self, user_id: &UserId, pushkey: &str) {
		let matches = |(owner, key): &PusherKey| owner == user_id && key == pushkey;

		lock(&self.bursts).retain(|(pusher, _), _| !matches(pusher));

		let mut dropped = lock(&self.dropped);
		dropped
			.at
			.retain(|(pusher, _), _| !matches(pusher));
		dropped
			.order
			.retain(|((pusher, _), _)| !matches(pusher));
	}

	#[cfg(test)]
	pub(super) fn sizes(&self) -> (usize, usize, usize) {
		let dropped = lock(&self.dropped);
		(dropped.at.len(), dropped.order.len(), lock(&self.bursts).len())
	}
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
	mutex
		.lock()
		.unwrap_or_else(PoisonError::into_inner)
}
