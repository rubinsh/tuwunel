//! Which room each device is actively viewing (D8), as its client reports.
//!
//! A heartbeat claims a room until the TTL runs out; a clear ends the claim.
//! Reports carry a `seq` the client allocates in order, so a late report can
//! never undo a newer one: the highest `seq` wins, and a clear also fences
//! off every heartbeat up to its own `seq`. Kept in memory: a restart only
//! ends every claim early, which errs towards alerting.

use std::{
	collections::HashMap,
	sync::{Mutex, MutexGuard, PoisonError},
};

use ruma::{DeviceId, OwnedDeviceId, OwnedRoomId, OwnedUserId, RoomId, UserId};

/// How long a heartbeat claims its room. The client is told this value.
pub const ACTIVITY_TTL_MS: u64 = 60_000;

/// How long a device's ordering record outlives its last report, so a report
/// delayed by that much is still recognised as stale.
const RECORD_TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// The most devices tracked at once. Past it, idle records are reaped, and if
/// that is not enough the oldest go.
const DEVICE_CAP: usize = 16_384;

#[derive(Default)]
pub(super) struct Activity {
	devices: Mutex<HashMap<OwnedUserId, HashMap<OwnedDeviceId, Device>>>,
}

#[derive(Clone, Debug, Default)]
struct Device {
	/// The highest `seq` reported.
	seq: u64,
	/// No heartbeat at or below this `seq` counts: a clear covered it.
	fence: u64,
	/// The room claimed, while the claim lasts.
	room: Option<OwnedRoomId>,
	expires_ms: u64,
	reported_ms: u64,
}

impl Activity {
	/// Apply one report from `device`: a heartbeat for `room`, or a clear when
	/// `room` is `None`. Stale reports change nothing.
	pub(super) fn report(
		&self,
		user_id: &UserId,
		device_id: &DeviceId,
		room: Option<&RoomId>,
		seq: u64,
		now_ms: u64,
	) {
		let mut users = lock(&self.devices);
		reap(&mut users, now_ms);

		let device = users
			.entry(user_id.to_owned())
			.or_default()
			.entry(device_id.to_owned())
			.or_default();

		device.reported_ms = now_ms;

		match room {
			| Some(room) => {
				if seq <= device.fence || seq < device.seq {
					return;
				}

				device.seq = seq;
				device.room = Some(room.to_owned());
				device.expires_ms = now_ms.saturating_add(ACTIVITY_TTL_MS);
			},
			| None => {
				device.fence = device.fence.max(seq);
				if seq >= device.seq {
					device.seq = seq;
					device.room = None;
				}
			},
		}
	}

	/// Whether one of the user's devices other than `except` is viewing
	/// `room_id` now.
	pub(super) fn viewed_elsewhere(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
		except: &DeviceId,
		now_ms: u64,
	) -> bool {
		lock(&self.devices)
			.get(user_id)
			.is_some_and(|devices| {
				devices.iter().any(|(device_id, device)| {
					device_id != except
						&& device.expires_ms > now_ms
						&& device.room.as_deref() == Some(room_id)
				})
			})
	}

	#[cfg(test)]
	pub(super) fn len(&self) -> usize {
		lock(&self.devices)
			.values()
			.map(HashMap::len)
			.sum()
	}
}

/// Keep the record count under the cap: first drop records idle past their
/// TTL, then the least recently reported.
fn reap(users: &mut HashMap<OwnedUserId, HashMap<OwnedDeviceId, Device>>, now_ms: u64) {
	let count =
		|users: &HashMap<_, HashMap<_, _>>| users.values().map(HashMap::len).sum::<usize>();
	if count(users) < DEVICE_CAP {
		return;
	}

	for devices in users.values_mut() {
		devices.retain(|_, device| now_ms.saturating_sub(device.reported_ms) < RECORD_TTL_MS);
	}
	users.retain(|_, devices| !devices.is_empty());

	let excess = count(users).saturating_sub(DEVICE_CAP.saturating_sub(1));
	if excess == 0 {
		return;
	}

	let mut by_age: Vec<_> = users
		.iter()
		.flat_map(|(user, devices)| {
			devices
				.iter()
				.map(move |(device, record)| (record.reported_ms, user.clone(), device.clone()))
		})
		.collect();

	by_age.sort_unstable_by_key(|(reported_ms, ..)| *reported_ms);
	for (_, user, device) in by_age.into_iter().take(excess) {
		if let Some(devices) = users.get_mut(&user) {
			devices.remove(&device);
		}
	}
	users.retain(|_, devices| !devices.is_empty());
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
	mutex
		.lock()
		.unwrap_or_else(PoisonError::into_inner)
}
