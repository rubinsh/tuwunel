mod read_markers;
mod receipt;

use ruma::{EventId, MilliSecondsSinceUnixEpoch, RoomId, UserId, events::receipt::ReceiptThread};
use tuwunel_core::{Err, PduCount, Result, err, utils::result::LogErr};
use tuwunel_service::{Services, rooms::read_receipt::PrivateRead};

pub(crate) use self::{read_markers::set_read_marker_route, receipt::create_receipt_route};

/// Resolves `event` to its timeline position and stores the private read
/// marker for `thread` there.
///
/// Returns whether the marker advanced. A backfilled event carries no forward
/// position, so it is rejected rather than stored.
async fn set_private_marker(
	services: &Services,
	room_id: &RoomId,
	user_id: &UserId,
	event: &EventId,
	thread: &ReceiptThread,
) -> Result<bool> {
	let count = services
		.timeline
		.get_pdu_count(event)
		.await
		.map_err(|_| err!(Request(NotFound("Event not found."))))?;

	let PduCount::Normal(count) = count else {
		return Err!(Request(InvalidParam(
			"Event is a backfilled PDU and cannot be marked as read."
		)));
	};

	let advanced = services
		.read_receipt
		.private_read_set(PrivateRead {
			room_id,
			user_id,
			count,
			ts: MilliSecondsSinceUnixEpoch::now(),
			thread,
			announce: true,
		})
		.await;

	Ok(advanced)
}

/// Marks the receipt's notifications read up to the latest of `events` and
/// refreshes the push badge.
///
/// The notified events after it stay unread. The refresh follows every
/// advance because the gateway can hold a stale badge while the stored count
/// is already lower; only a delivery reconciles it.
async fn read_and_refresh_badge(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	thread: &ReceiptThread,
	events: &[&EventId],
) {
	let mut read_up_to = None;
	for event in events {
		read_up_to = read_up_to.max(read_position(services, event).await);
	}

	services
		.pusher
		.read_notification_counts(user_id, room_id, thread, read_up_to)
		.await;

	services
		.sending
		.refresh_push_badge(user_id)
		.await
		.log_err()
		.ok();
}

/// The PDU count a receipt on `event` reads up to.
///
/// A backfilled event sits before every counted one, so nothing counted is
/// read. An event this server cannot place gives no position.
async fn read_position(services: &Services, event: &EventId) -> Option<u64> {
	match services.timeline.get_pdu_count(event).await {
		| Ok(PduCount::Normal(count)) => Some(count),
		| Ok(PduCount::Backfilled(_)) => Some(0),
		| Err(_) => None,
	}
}
