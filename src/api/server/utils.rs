use std::pin::pin;

use futures::{FutureExt, StreamExt, join};
use ruma::{EventId, OwnedRoomId, RoomId, ServerName};
use serde::Deserialize;
use tuwunel_core::{Err, Result, err, implement, is_false, utils::option::OptionExt};
use tuwunel_service::Services;

pub(super) struct AccessCheck<'a> {
	pub(super) services: &'a Services,
	pub(super) origin: &'a ServerName,
	pub(super) room_id: &'a RoomId,
	pub(super) event_id: Option<&'a EventId>,
}

#[implement(AccessCheck, params = "<'_>")]
pub(super) async fn check(&self) -> Result {
	let acl_check = self
		.services
		.event_handler
		.acl_check(self.origin, self.room_id)
		.map(|result| result.is_ok());

	let world_readable = self
		.services
		.state_accessor
		.is_world_readable(self.room_id);

	let server_in_room = self
		.services
		.state_cache
		.server_in_room(self.origin, self.room_id);

	// if any user on our homeserver is trying to knock this room, we'll need to
	// acknowledge bans or leaves
	let user_is_knocking = async {
		let knocked = self
			.services
			.state_cache
			.room_members_knocked(self.room_id);
		let mut knocked = pin!(knocked);

		knocked.next().await.is_some()
	};

	let server_can_see = self.event_id.map_async(|event_id| {
		self.services
			.state_accessor
			.server_can_see_event(self.origin, self.room_id, event_id)
	});

	let (world_readable, server_in_room, server_can_see, acl_check, user_is_knocking) =
		join!(world_readable, server_in_room, server_can_see, acl_check, user_is_knocking);

	if !acl_check {
		return Err!(Request(Forbidden("Server access denied.")));
	}

	if !world_readable && !server_in_room && !user_is_knocking {
		return Err!(Request(Forbidden("Server is not in room.")));
	}

	if server_can_see.is_some_and(is_false!()) {
		return Err!(Request(Forbidden("Server is not allowed to see event.")));
	}

	Ok(())
}

pub(super) async fn require_known_room(
	services: &Services,
	room_id: &RoomId,
	origin: &ServerName,
) -> Result {
	if !services.metadata.exists(room_id).await {
		return Err!(Request(NotFound("Room is unknown to this server.")));
	}

	services
		.event_handler
		.acl_check(origin, room_id)
		.await
}

pub(super) async fn require_event_in_room(
	services: &Services,
	event_id: &EventId,
	room_id: &RoomId,
) -> Result {
	#[derive(Deserialize)]
	struct PduRoomId {
		room_id: OwnedRoomId,
	}

	services
		.timeline
		.get::<PduRoomId>(event_id)
		.await
		.is_ok_and(|pdu| pdu.room_id == room_id)
		.then_some(())
		.ok_or_else(|| err!(Request(NotFound("Event not found."))))
}
