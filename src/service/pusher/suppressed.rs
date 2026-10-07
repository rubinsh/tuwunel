//! Deferred push suppression queues.
//!
//! Stores suppressed push events in memory until they can be flushed. This is
//! intentionally in-memory only: suppressed events are discarded on restart.

use std::{
	collections::{HashMap, HashSet, VecDeque},
	sync::Mutex,
};

use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};
use tuwunel_core::{debug, implement, trace, utils};

use crate::rooms::timeline::RawPduId;

const SUPPRESSED_MAX_EVENTS_PER_ROOM: usize = 512;
const SUPPRESSED_MAX_EVENTS_PER_PUSHKEY: usize = 4096;
const SUPPRESSED_MAX_ROOMS_PER_PUSHKEY: usize = 256;

type SuppressedRooms = Vec<(OwnedRoomId, Vec<RawPduId>)>;
type SuppressedPushes = Vec<(String, SuppressedRooms)>;

#[derive(Default)]
pub(super) struct SuppressedQueue {
	inner: Mutex<HashMap<OwnedUserId, HashMap<String, PushkeyQueue>>>,
}

#[derive(Default)]
struct PushkeyQueue {
	rooms: HashMap<OwnedRoomId, VecDeque<SuppressedEvent>>,
	total_events: usize,
}

#[derive(Clone, Debug)]
struct SuppressedEvent {
	pdu_id: RawPduId,
	_inserted_at_ms: u64,
}

impl SuppressedQueue {
	fn lock(
		&self,
	) -> std::sync::MutexGuard<'_, HashMap<OwnedUserId, HashMap<String, PushkeyQueue>>> {
		self.inner
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	fn drain_room(queue: VecDeque<SuppressedEvent>) -> Vec<RawPduId> {
		queue
			.into_iter()
			.map(|event| event.pdu_id)
			.collect()
	}

	/// Removes the room's queued PDUs that `read` selects, across all of the
	/// user's pushkeys, and returns how many went.
	fn clear_room(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
		read: impl Fn(&RawPduId) -> bool,
	) -> usize {
		let mut inner = self.lock();
		let Some(user_entry) = inner.get_mut(user_id) else {
			return 0;
		};

		let mut removed: usize = 0;
		user_entry.retain(|_, push_entry| {
			if let Some(queue) = push_entry.rooms.get_mut(room_id) {
				let before = queue.len();
				queue.retain(|event| !read(&event.pdu_id));
				let cleared = before.saturating_sub(queue.len());

				removed = removed.saturating_add(cleared);
				push_entry.total_events = push_entry.total_events.saturating_sub(cleared);
				if queue.is_empty() {
					push_entry.rooms.remove(room_id);
				}
			}

			!push_entry.rooms.is_empty()
		});

		if user_entry.is_empty() {
			inner.remove(user_id);
		}

		removed
	}

	fn drop_one_front(queue: &mut VecDeque<SuppressedEvent>, total_events: &mut usize) -> bool {
		if queue.pop_front().is_some() {
			*total_events = total_events.saturating_sub(1);
			return true;
		}

		false
	}
}

/// Enqueue a PDU for later push delivery when suppression is active.
#[implement(super::Service)]
pub fn queue_suppressed_push(
	&self,
	user_id: &UserId,
	pushkey: &str,
	room_id: &RoomId,
	pdu_id: RawPduId,
) -> bool {
	let mut inner = self.suppressed.lock();
	let user_entry = inner.entry(user_id.to_owned()).or_default();
	let push_entry = user_entry.entry(pushkey.to_owned()).or_default();

	if !push_entry.rooms.contains_key(room_id)
		&& push_entry.rooms.len() >= SUPPRESSED_MAX_ROOMS_PER_PUSHKEY
	{
		debug!(
			?user_id,
			?room_id,
			pushkey,
			max_rooms = SUPPRESSED_MAX_ROOMS_PER_PUSHKEY,
			"Suppressed push queue full (rooms); dropping event"
		);
		return false;
	}

	let queue = push_entry
		.rooms
		.entry(room_id.to_owned())
		.or_default();

	if queue
		.back()
		.is_some_and(|event| event.pdu_id == pdu_id)
	{
		trace!(?user_id, ?room_id, pushkey, "Suppressed push event is duplicate; skipping");
		return false;
	}

	if push_entry.total_events >= SUPPRESSED_MAX_EVENTS_PER_PUSHKEY && queue.is_empty() {
		debug!(
			?user_id,
			?room_id,
			pushkey,
			max_events = SUPPRESSED_MAX_EVENTS_PER_PUSHKEY,
			"Suppressed push queue full (total); dropping event"
		);
		return false;
	}

	while queue.len() >= SUPPRESSED_MAX_EVENTS_PER_ROOM
		|| push_entry.total_events >= SUPPRESSED_MAX_EVENTS_PER_PUSHKEY
	{
		if !SuppressedQueue::drop_one_front(queue, &mut push_entry.total_events) {
			break;
		}
	}

	queue.push_back(SuppressedEvent {
		pdu_id,
		_inserted_at_ms: utils::millis_since_unix_epoch(),
	});
	push_entry.total_events = push_entry.total_events.saturating_add(1);

	true
}

/// Take and remove all suppressed PDUs for a given user + pushkey.
#[implement(super::Service)]
pub fn take_suppressed_for_pushkey(
	&self,
	user_id: &UserId,
	pushkey: &str,
) -> Vec<(OwnedRoomId, Vec<RawPduId>)> {
	let mut inner = self.suppressed.lock();
	let Some(user_entry) = inner.get_mut(user_id) else {
		return Vec::new();
	};

	let Some(push_entry) = user_entry.remove(pushkey) else {
		return Vec::new();
	};

	if user_entry.is_empty() {
		inner.remove(user_id);
	}

	push_entry
		.rooms
		.into_iter()
		.map(|(room_id, queue)| (room_id, SuppressedQueue::drain_room(queue)))
		.collect()
}

/// Take and remove all suppressed PDUs for a given user across all pushkeys.
#[implement(super::Service)]
pub fn take_suppressed_for_user(&self, user_id: &UserId) -> SuppressedPushes {
	let mut inner = self.suppressed.lock();
	let Some(user_entry) = inner.remove(user_id) else {
		return Vec::new();
	};

	user_entry
		.into_iter()
		.map(|(pushkey, queue)| {
			let rooms = queue
				.rooms
				.into_iter()
				.map(|(room_id, q)| (room_id, SuppressedQueue::drain_room(q)))
				.collect();
			(pushkey, rooms)
		})
		.collect()
}

/// Clear suppressed PDUs for a specific room (across all pushkeys).
#[implement(super::Service)]
pub fn clear_suppressed_room(&self, user_id: &UserId, room_id: &RoomId) -> usize {
	self.suppressed
		.clear_room(user_id, room_id, |_| true)
}

/// The room's suppressed PDUs, across all pushkeys, each once.
#[implement(super::Service)]
pub fn suppressed_room_pdus(&self, user_id: &UserId, room_id: &RoomId) -> Vec<RawPduId> {
	let inner = self.suppressed.lock();
	let mut seen = HashSet::new();
	inner
		.get(user_id)
		.into_iter()
		.flat_map(HashMap::values)
		.filter_map(|push_entry| push_entry.rooms.get(room_id))
		.flatten()
		.map(|event| event.pdu_id)
		.filter(|pdu_id| seen.insert(*pdu_id))
		.collect()
}

/// Clear the given suppressed PDUs of a room (across all pushkeys), keeping
/// the rest.
#[implement(super::Service)]
pub fn clear_suppressed_room_pdus(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	read: &[RawPduId],
) -> usize {
	self.suppressed
		.clear_room(user_id, room_id, |pdu_id| read.contains(pdu_id))
}

/// Clear suppressed PDUs for a specific pushkey.
#[implement(super::Service)]
pub fn clear_suppressed_pushkey(&self, user_id: &UserId, pushkey: &str) -> usize {
	let mut inner = self.suppressed.lock();
	let Some(user_entry) = inner.get_mut(user_id) else {
		return 0;
	};

	let removed = user_entry
		.remove(pushkey)
		.map(|queue| queue.total_events)
		.unwrap_or(0);

	if user_entry.is_empty() {
		inner.remove(user_id);
	}

	removed
}

#[cfg(test)]
mod tests {
	use ruma::{RoomId, UserId};
	use tuwunel_core::matrix::pdu::{Count, PduId};

	use super::{SuppressedEvent, SuppressedQueue};
	use crate::rooms::timeline::RawPduId;

	const ROOM: &str = "!room:example.org";
	const OTHER_ROOM: &str = "!other:example.org";
	const USER: &str = "@reader:example.org";

	fn pdu_id(count: u64) -> RawPduId {
		PduId {
			shortroomid: 7,
			count: Count::Normal(count),
		}
		.into()
	}

	fn queued(pushkeys: &[&str], rooms: &[(&str, &[u64])]) -> SuppressedQueue {
		let queue = SuppressedQueue::default();
		{
			let mut inner = queue.lock();
			let user_entry = inner
				.entry(UserId::parse(USER).unwrap())
				.or_default();

			for pushkey in pushkeys {
				let push_entry = user_entry
					.entry((*pushkey).to_owned())
					.or_default();
				for (room, counts) in rooms {
					let events = counts.iter().map(|&count| SuppressedEvent {
						pdu_id: pdu_id(count),
						_inserted_at_ms: 0,
					});

					push_entry
						.rooms
						.entry(RoomId::parse(*room).unwrap())
						.or_default()
						.extend(events);

					push_entry.total_events = push_entry
						.total_events
						.saturating_add(counts.len());
				}
			}
		}

		queue
	}

	fn remaining(queue: &SuppressedQueue, pushkey: &str, room: &str) -> Vec<RawPduId> {
		queue
			.lock()
			.get(&UserId::parse(USER).unwrap())
			.and_then(|user_entry| user_entry.get(pushkey))
			.and_then(|push_entry| {
				push_entry
					.rooms
					.get(&RoomId::parse(room).unwrap())
			})
			.map(|events| events.iter().map(|event| event.pdu_id).collect())
			.unwrap_or_default()
	}

	/// A read drops only the pushes it covers, on every pushkey; the rest of
	/// the room, and other rooms, stay queued.
	#[test]
	fn partial_read_keeps_later_pushes() {
		let queue = queued(&["phone", "laptop"], &[(ROOM, &[10, 20, 30]), (OTHER_ROOM, &[15])]);
		let user = UserId::parse(USER).unwrap();
		let room = RoomId::parse(ROOM).unwrap();

		let read = [pdu_id(10), pdu_id(20)];
		let removed = queue.clear_room(&user, &room, |id| read.contains(id));

		assert_eq!(removed, 4);
		for pushkey in ["phone", "laptop"] {
			assert_eq!(remaining(&queue, pushkey, ROOM), vec![pdu_id(30)]);
			assert_eq!(remaining(&queue, pushkey, OTHER_ROOM), vec![pdu_id(15)]);
		}
	}

	/// A full read leaves nothing of the room, and no empty entries behind.
	#[test]
	fn full_read_clears_the_room() {
		let queue = queued(&["phone"], &[(ROOM, &[10, 20])]);
		let user = UserId::parse(USER).unwrap();
		let room = RoomId::parse(ROOM).unwrap();

		assert_eq!(queue.clear_room(&user, &room, |_| true), 2);
		assert!(queue.lock().is_empty());
	}
}
