//! Client-API harness shared by the MSC3664 push condition tests.
//!
//! Each server-booting test is its own binary, so the two MSC3664 tests would
//! otherwise carry one copy of this each. Both use every item here, which is
//! what keeps the module free of dead code in either binary.

use std::time::Duration;

use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel_core::{
	Result, err,
	ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId},
};
use tuwunel_service::{Services, users::Register};

/// The unstable kind of the push condition under test.
pub(crate) const CONDITION_KIND: &str = "im.nheko.msc3664.related_event_match";

/// One user's authenticated view of the client API.
pub(crate) struct Client<'a> {
	pub(crate) services: &'a Services,
	pub(crate) base: &'a str,
	pub(crate) token: &'a str,
}

/// Wait for the listener to answer, which the boot does not itself await.
pub(crate) async fn wait_until_ready(services: &Services, base: &str) -> Result {
	let url = format!("{base}/_matrix/client/versions");

	timeout(Duration::from_secs(10), async {
		while services
			.client
			.clients
			.default
			.get(&url)
			.send()
			.await
			.is_err()
		{
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| err!("server listener did not become ready"))?;

	Ok(())
}

/// Register a local user and give it a device holding `token`.
pub(crate) async fn register(
	services: &Services,
	localpart: &str,
	token: &str,
) -> Result<OwnedUserId> {
	let user_id = UserId::parse_with_server_name(localpart, services.globals.server_name())?;

	services
		.users
		.full_register(Register {
			user_id: Some(&user_id),
			password: Some("msc3664-password"),
			..Default::default()
		})
		.await?;

	services
		.users
		.create_device(&user_id, None, (Some(token), None), None, None, None)
		.await?;

	Ok(user_id)
}

impl Client<'_> {
	pub(crate) async fn create_room(&self) -> Result<OwnedRoomId> {
		let response = self
			.services
			.client
			.clients
			.default
			.post(self.url("createRoom"))
			.bearer_auth(self.token)
			.json(&json!({ "preset": "public_chat" }))
			.send()
			.await?
			.error_for_status()?
			.json::<Value>()
			.await?;

		Ok(field(&response, "room_id")?.try_into()?)
	}

	pub(crate) async fn join(&self, room_id: &RoomId) -> Result {
		self.services
			.client
			.clients
			.default
			.post(self.url(&format!("rooms/{room_id}/join")))
			.bearer_auth(self.token)
			.json(&json!({}))
			.send()
			.await?
			.error_for_status()?;

		Ok(())
	}

	pub(crate) async fn set_push_rule(&self, rule_id: &str, rule: &Value) -> Result {
		self.services
			.client
			.clients
			.default
			.put(self.url(&format!("pushrules/global/override/{rule_id}")))
			.bearer_auth(self.token)
			.json(rule)
			.send()
			.await?
			.error_for_status()?;

		Ok(())
	}

	pub(crate) async fn send(
		&self,
		room_id: &RoomId,
		event_type: &str,
		txn_id: &str,
		content: &Value,
	) -> Result<OwnedEventId> {
		let path = format!("rooms/{room_id}/send/{event_type}/msc3664-{txn_id}");
		let response = self
			.services
			.client
			.clients
			.default
			.put(self.url(&path))
			.bearer_auth(self.token)
			.json(content)
			.send()
			.await?
			.error_for_status()?
			.json::<Value>()
			.await?;

		Ok(field(&response, "event_id")?.try_into()?)
	}

	/// The server's advertised capability for the condition, absent when the
	/// server does not evaluate it.
	pub(crate) async fn condition_capability(&self) -> Result<Option<Value>> {
		let response = self
			.services
			.client
			.clients
			.default
			.get(self.url("capabilities"))
			.bearer_auth(self.token)
			.send()
			.await?
			.error_for_status()?
			.json::<Value>()
			.await?;

		let capability = response
			.get("capabilities")
			.and_then(|capabilities| capabilities.get(CONDITION_KIND))
			.cloned();

		Ok(capability)
	}

	fn url(&self, path: &str) -> String { format!("{}/_matrix/client/v3/{path}", self.base) }
}

/// Read a required string field out of a response body.
fn field<'a>(response: &'a Value, name: &str) -> Result<&'a str> {
	response
		.get(name)
		.and_then(Value::as_str)
		.ok_or_else(|| err!("response omitted {name}"))
}

/// Whether the room's notification count reaches `want` before the deadline.
///
/// Push evaluation trails the send response, so neither outcome can be read off
/// a single sample: a positive case needs the poll, and a negative case is only
/// proven by the whole deadline elapsing.
pub(crate) async fn notified(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	want: u64,
) -> bool {
	timeout(Duration::from_secs(5), async {
		while services
			.pusher
			.notification_count(user_id, room_id)
			.await
			.lt(&want)
		{
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.is_ok()
}

/// Whether the room's highlight count reaches `want` before the deadline.
///
/// A rule asking for the highlight tweak is the way to tell its own match from
/// the default rule that notifies for every message, which would otherwise
/// satisfy an assertion on the notification count alone.
pub(crate) async fn highlighted(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	want: u64,
) -> bool {
	timeout(Duration::from_secs(5), async {
		while services
			.pusher
			.highlight_count(user_id, room_id)
			.await
			.lt(&want)
		{
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.is_ok()
}
