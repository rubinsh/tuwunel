//! In-memory store for matrix-channel custom ephemeral events.
//!
//! Companion to `_tuwunel/ephemeral/{event_type}/{room_id}` PUT
//! endpoint and the v3 `/sync` `room.ephemeral.events` emission.
//! Modeled on the typing service: in-memory state with a per-room
//! counter watermark and a broadcast wakeup for syncing clients. No DB
//! persistence (fresh after restart), no federation (local delivery
//! only).
//!
//! # Why a queue instead of last-write-wins
//!
//! The first cut keyed by `(room, event_type)` and stored only the
//! latest content. Pi flagged two blockers in review (PR #210
//! analogue):
//!   - sender identity was discarded, breaking multi-bot rooms where the
//!     web-client filters by `event.getSender()`;
//!   - fast bursts (`Thinking → Bash → Thinking`) collapsed to whichever state
//!     happened to be current when /sync next fired.
//!
//! v2 stores a bounded ring queue per room of
//! `(counter, event_type, sender, content)` entries. `/sync` emits
//! every entry with `counter > since`, so per-sender identity is
//! preserved and intermediate transitions are observable as long as the
//! consumer polls fast enough to keep up with the ring window.
//!
//! Ring size: `MAX_ENTRIES_PER_ROOM` = 1024 — covers ~4 minutes at 4
//! transitions/second, well above the worst-case /sync interval. Older
//! entries are evicted on insert.

use std::{
	collections::{BTreeMap, VecDeque},
	sync::Arc,
};

use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId, serde::Raw};
use serde_json::value::RawValue as RawJsonValue;
use tokio::sync::{RwLock, broadcast};
use tuwunel_core::{Result, debug_info, trace, utils};

const MAX_ENTRIES_PER_ROOM: usize = 1024;

#[derive(Clone)]
pub struct EphemeralEntry {
	pub counter: u64,
	pub event_type: String,
	pub sender: OwnedUserId,
	pub origin_server_ts: u64,
	pub content: Box<RawJsonValue>,
}

pub struct Service {
	services: Arc<crate::services::OnceServices>,
	/// Per-room bounded ring of ephemeral entries, ordered by counter
	/// (oldest first). Eviction happens on push; readers slice by
	/// counter watermark.
	pub entries: RwLock<BTreeMap<OwnedRoomId, VecDeque<EphemeralEntry>>>,
	pub update_sender: broadcast::Sender<OwnedRoomId>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: args.services.clone(),
			entries: RwLock::new(BTreeMap::new()),
			update_sender: broadcast::channel(100).0,
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Records a new ephemeral transition for `(room_id, event_type,
	/// sender)`, evicts oldest entries beyond the ring cap, bumps the
	/// global counter, and wakes /sync waiters.
	pub async fn put(
		&self,
		room_id: &RoomId,
		event_type: &str,
		sender: &UserId,
		content: Box<RawJsonValue>,
	) -> Result {
		debug_info!("ephemeral put {event_type:?} sender:{sender:?} in {room_id:?}",);

		let counter = self.services.globals.next_count();
		let entry = EphemeralEntry {
			counter: *counter,
			event_type: event_type.to_owned(),
			sender: sender.to_owned(),
			origin_server_ts: utils::millis_since_unix_epoch(),
			content,
		};

		let mut map = self.entries.write().await;
		let ring = map.entry(room_id.to_owned()).or_default();
		push_with_eviction(ring, entry);
		drop(map);

		// `next_batch` waits for dispatched counters to retire. Keep the permit
		// until the entry is visible so /sync cannot advance past an in-flight PUT.
		// See `core/utils/two_phase_counter.rs`; rooms/typing uses the same order.
		drop(counter);

		if self
			.update_sender
			.send(room_id.to_owned())
			.is_err()
		{
			trace!("no /sync waiters for ephemeral broadcast");
		}

		Ok(())
	}

	/// Snapshot of entries in `room_id` with `since < counter <=
	/// upper_bound`, preserving insertion order. The upper bound is
	/// `next_batch` from the surrounding /sync — entries newer than
	/// the response's advertised batch must be deferred to the next
	/// /sync, otherwise the same event is delivered twice (once with
	/// the racing PUT's response, once with the next /sync that
	/// passes that token as `since`).
	pub async fn entries_window(
		&self,
		room_id: &RoomId,
		since: u64,
		upper_bound: u64,
	) -> Vec<EphemeralEntry> {
		let map = self.entries.read().await;
		let Some(ring) = map.get(room_id) else {
			return Vec::new();
		};
		entries_window_in(ring, since, upper_bound)
	}

	/// Renders entries in the `(since, upper_bound]` window as
	/// `Raw<AnySyncEphemeralRoomEvent>` shaped
	/// `{type, sender, room_id, origin_server_ts, content}`. Entries
	/// that fail to serialize are dropped so a single bad row can't
	/// break the rest of the sync response.
	pub async fn raw_events_for_sync<E>(
		&self,
		room_id: &RoomId,
		since: u64,
		upper_bound: u64,
	) -> Vec<Raw<E>> {
		self.entries_window(room_id, since, upper_bound)
			.await
			.into_iter()
			.filter_map(|entry| render_entry(&entry, room_id).ok())
			.collect()
	}

	pub async fn wait_for_update(&self, room_id: &RoomId) {
		let mut receiver = self.update_sender.subscribe();
		while let Ok(next) = receiver.recv().await {
			if next == room_id {
				break;
			}
		}
	}
}

/// Build a single `Raw<...>` ephemeral event from an entry. Top-level
/// `sender` and `room_id` are required by matrix-js-sdk consumers:
/// `event.getSender()` reads `sender`; `event.getRoomId()` reads
/// `room_id` (the SDK's `mapSyncEventsFormat(joinObj.ephemeral)` path
/// does not stamp the parent roomId onto custom ephemeral events the
/// way it does for known types like `m.typing`, so it must be in the
/// event JSON itself for `getRoomId()` to resolve). The wire shape
/// otherwise mirrors `{type, content}` ephemeral events.
pub(crate) fn render_entry<E>(entry: &EphemeralEntry, room_id: &RoomId) -> Result<Raw<E>> {
	let wire = serde_json::json!({
		"type": entry.event_type,
		"sender": entry.sender,
		"room_id": room_id,
		"origin_server_ts": entry.origin_server_ts,
		"content": entry.content,
	});
	let json = serde_json::value::to_raw_value(&wire)?;
	Ok(Raw::from_json(json))
}

/// Append `entry` to `ring` and evict the oldest entries past
/// `MAX_ENTRIES_PER_ROOM`. Pure helper so tests can exercise the
/// eviction policy without spinning up a Services instance.
pub(crate) fn push_with_eviction(ring: &mut VecDeque<EphemeralEntry>, entry: EphemeralEntry) {
	ring.push_back(entry);
	while ring.len() > MAX_ENTRIES_PER_ROOM {
		ring.pop_front();
	}
}

/// Filters `ring` to entries in the half-open window
/// `since < counter <= upper_bound`, preserving insertion order.
/// Pure helper mirroring the body of [`Service::entries_window`].
pub(crate) fn entries_window_in(
	ring: &VecDeque<EphemeralEntry>,
	since: u64,
	upper_bound: u64,
) -> Vec<EphemeralEntry> {
	ring.iter()
		.filter(|e| e.counter > since && e.counter <= upper_bound)
		.cloned()
		.collect()
}

#[cfg(test)]
mod tests;
