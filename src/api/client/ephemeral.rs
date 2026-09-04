//! `PUT /_tuwunel/ephemeral/{event_type}/{room_id}`
//!
//! Custom Tuwunel endpoint for matrix-channel-style ephemeral events
//! (e.g. `com.shai.matrix-channel.bot_activity`). The body is the event
//! `content` (any JSON object). Each PUT appends an entry carrying its
//! event type and sender to a bounded per-room ring queue; the room's
//! `room.ephemeral.events` on the next `/sync` carries every entry
//! whose counter exceeds the request's `since` (capped at
//! `next_batch`).
//!
//! Requirements:
//!   - Bearer token (user or appservice). Expired user tokens are rejected; the
//!     standard router auth path applies the same check. Locked accounts are
//!     rejected as they are on standard authenticated routes.
//!   - `event_type` is in `config.allowed_ephemeral_types`.
//!   - Sender is joined to `room_id`, including the appservice sender user.
//!
//! No federation, no DB persistence. A clear is just a PUT with whatever
//! "cleared" content the caller wants the next `/sync` to surface
//! (matrix-channel uses `{"working":null}`).

use std::time::SystemTime;

use axum::{
	Json, RequestPartsExt, body,
	extract::{Path, Request, State},
	response::IntoResponse,
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use ruma::{OwnedRoomId, OwnedUserId};
use serde_json::{Value, json, value::RawValue as RawJsonValue};
use tuwunel_core::{Err, Result, err};
use tuwunel_service::Services;

const MAX_BODY_BYTES: usize = 64 * 1024;

pub(crate) async fn put_ephemeral_event_route(
	State(services): State<crate::State>,
	Path((event_type, room_id)): Path<(String, OwnedRoomId)>,
	request: Request,
) -> Result<impl IntoResponse> {
	if !services
		.server
		.config
		.allowed_ephemeral_types
		.iter()
		.any(|allowed| allowed == &event_type)
	{
		return Err!(Request(Forbidden(
			"Ephemeral event type is not in allowed_ephemeral_types"
		)));
	}

	let (mut parts, body) = request.into_parts();

	let bearer: Option<TypedHeader<Authorization<Bearer>>> =
		parts.extract().await.unwrap_or(None);
	let token = bearer
		.map(|TypedHeader(Authorization(b))| b.token().to_owned())
		.ok_or_else(|| err!(Request(MissingToken("Missing access token"))))?;

	let sender_user: OwnedUserId = match resolve_sender(&services, &token).await? {
		| Sender::User(u) => u,
		| Sender::Appservice(asu) => asu,
	};
	services.users.locked_check(&sender_user).await?;

	if !services
		.state_cache
		.is_joined(&sender_user, &room_id)
		.await
	{
		return Err!(Request(Forbidden("You are not in this room.")));
	}

	let bytes = body::to_bytes(body, MAX_BODY_BYTES)
		.await
		.map_err(|_| err!(Request(TooLarge("Ephemeral content body too large"))))?;

	if bytes.is_empty() {
		return Err!(Request(BadJson("Ephemeral content body must be a JSON object")));
	}

	let parsed: Value = serde_json::from_slice(&bytes)
		.map_err(|_| err!(Request(BadJson("Ephemeral content body is not valid JSON"))))?;
	if !parsed.is_object() {
		return Err!(Request(BadJson("Ephemeral content must be a JSON object")));
	}

	let raw: Box<RawJsonValue> = serde_json::value::to_raw_value(&parsed)
		.map_err(|_| err!(Request(BadJson("Failed to re-encode ephemeral content"))))?;

	services
		.ephemeral
		.put(&room_id, &event_type, &sender_user, raw)
		.await?;

	Ok(Json(json!({})))
}

enum Sender {
	User(OwnedUserId),
	Appservice(OwnedUserId),
}

async fn resolve_sender(services: &Services, token: &str) -> Result<Sender> {
	if let Ok((user_id, _device_id, expires_at)) = services.users.find_from_token(token).await {
		// Standard router auth path rejects expired tokens after lookup;
		// mirror that here so this custom endpoint isn't a bypass.
		if let Some(deadline) = expires_at
			&& deadline <= SystemTime::now()
		{
			return Err!(Request(Unauthorized("Expired access token")));
		}
		return Ok(Sender::User(user_id));
	}

	if let Ok(info) = services
		.appservice
		.find_from_access_token(token)
		.await
	{
		// Appservices act on behalf of their sender_localpart user by default.
		return Ok(Sender::Appservice(info.sender));
	}

	Err!(Request(Unauthorized("Invalid access token")))
}
