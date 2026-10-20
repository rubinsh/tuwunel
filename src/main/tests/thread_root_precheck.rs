#![cfg(test)]

use std::{
	collections::BTreeMap, env::var, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id as process_id, time::Duration,
};

use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId},
};
use tuwunel_service::{Services, users::Register};

const READER_TOKEN: &str = "thread-root-precheck-reader-token";
const WRITER_TOKEN: &str = "thread-root-precheck-writer-token";

/// A send in a thread whose root is unknown, or in another room, is refused
/// before anything is stored.
///
/// The refusal used to come after the event was appended, so the client got
/// 400 M_INVALID_PARAM while the event stayed in the room and counted as
/// unread under a thread that doesn't exist. A refused send now leaves the
/// room's latest event and every unread count as they were; a send to a
/// valid root still goes through.
#[test]
fn thread_send_with_bad_root_stores_nothing() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path = PathBuf::from(root).join(format!("tuwunel-thread-root-{}", process_id()));

	let mut args = Args::default_test(&["fresh", "cleanup"]);

	args.option.extend([
		format!("database_path={db_path:?}"),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"ip_range_denylist=[]".to_owned(),
	]);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");

		drop(listener);

		let exercise = async {
			let outcome = exercise(&services, &base).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};

		let (run_result, outcome) = tokio::join!(async_run(&server), exercise);

		drop(services);
		async_stop(&server).await?;
		run_result?;

		outcome
	});

	drop(runtime);
	remove_dir_all(&db_path).ok();

	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let reader_id = register(services, "rootreader", READER_TOKEN).await?;

	register(services, "rootwriter", WRITER_TOKEN).await?;

	let reader = Client { services, base, token: READER_TOKEN };
	let writer = Client { services, base, token: WRITER_TOKEN };

	let room = writer.create_room().await?;
	reader.join(&room).await?;

	let other_room = writer.create_room().await?;
	let foreign_root = writer
		.send(&other_room, "foreign", &json!({ "msgtype": "m.text", "body": "elsewhere" }))
		.await?;

	// Control: a reply to a real root in the room is stored and counted.
	let valid_root = writer
		.send(&room, "root", &json!({ "msgtype": "m.text", "body": "root" }))
		.await?;
	let reply = writer
		.send(&room, "reply", &thread_reply(valid_root.as_str(), &reader_id))
		.await?;

	wait_thread(services, &reader_id, &room, &valid_root, (1, 1)).await?;

	let latest = latest_event(services, &room).await?;
	if latest != reply {
		return Err!("the valid thread reply {reply} is not the room's latest event ({latest})");
	}

	let main_before = main_counts(services, &reader_id, &room).await;
	let threads_before = thread_counts(services, &reader_id, &room).await;

	let missing = format!("$missingthreadroot:{}", services.globals.server_name());
	for (txn_id, root, case) in [
		("missing", missing.as_str(), "a root that doesn't exist"),
		("foreign", foreign_root.as_str(), "a root in another room"),
		("malformed", "not-an-event-id", "a malformed root"),
	] {
		let (status, errcode) = writer
			.send_status(&room, &format!("bad-{txn_id}"), &thread_reply(root, &reader_id))
			.await?;

		if status != 400 || errcode.as_deref() != Some("M_INVALID_PARAM") {
			return Err!(
				"a thread send to {case} answered {status} {errcode:?}, not 400 M_INVALID_PARAM"
			);
		}
	}

	// Push evaluation trails the response; give a stored event time to count.
	sleep(Duration::from_millis(500)).await;

	let latest_after = latest_event(services, &room).await?;
	if latest_after != reply {
		return Err!("a refused thread send stored {latest_after} in the room");
	}

	let main_after = main_counts(services, &reader_id, &room).await;
	if main_after != main_before {
		return Err!(
			"the room's counts moved from {main_before:?} to {main_after:?} on refused sends"
		);
	}

	let threads_after = thread_counts(services, &reader_id, &room).await;
	if threads_after != threads_before {
		return Err!(
			"the thread counts moved from {threads_before:?} to {threads_after:?} on refused \
			 sends"
		);
	}

	// The other room is untouched as well: still just its creation and one message.
	let other_latest = latest_event(services, &other_room).await?;
	if other_latest != foreign_root {
		return Err!("a refused send stored {other_latest} in the foreign root's room");
	}

	Ok(())
}

fn thread_reply(root: &str, mention: &UserId) -> Value {
	json!({
		"msgtype": "m.text",
		"body": "in the thread",
		"m.mentions": { "user_ids": [mention] },
		"m.relates_to": { "rel_type": "m.thread", "event_id": root },
	})
}

async fn latest_event(services: &Services, room_id: &RoomId) -> Result<OwnedEventId> {
	Ok(services
		.timeline
		.latest_pdu_in_room(room_id)
		.await?
		.event_id
		.clone())
}

async fn main_counts(services: &Services, user_id: &UserId, room_id: &RoomId) -> (u64, u64) {
	let pusher = &services.pusher;
	let notifications = pusher.notification_count(user_id, room_id).await;
	let highlights = pusher.highlight_count(user_id, room_id).await;

	(notifications, highlights)
}

async fn thread_counts(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
) -> BTreeMap<OwnedEventId, (u64, u64)> {
	services
		.pusher
		.thread_notification_counts(user_id, room_id)
		.await
}

async fn wait_thread(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	root: &EventId,
	want: (u64, u64),
) -> Result {
	timeout(Duration::from_secs(5), async {
		while thread_counts(services, user_id, room_id)
			.await
			.get(root)
			.copied() != Some(want)
		{
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| err!("the thread's counts did not reach {want:?}"))
}

/// One user's authenticated view of the client API.
struct Client<'a> {
	services: &'a Services,
	base: &'a str,
	token: &'a str,
}

impl Client<'_> {
	async fn create_room(&self) -> Result<OwnedRoomId> {
		let response = self
			.post("createRoom", &json!({ "preset": "public_chat" }))
			.await?;

		Ok(field(&response, "room_id")?.try_into()?)
	}

	async fn join(&self, room_id: &RoomId) -> Result {
		self.post(&format!("rooms/{room_id}/join"), &json!({}))
			.await
			.map(|_| ())
	}

	async fn send(
		&self,
		room_id: &RoomId,
		txn_id: &str,
		content: &Value,
	) -> Result<OwnedEventId> {
		let response = self
			.services
			.client
			.clients
			.default
			.put(self.send_url(room_id, txn_id))
			.bearer_auth(self.token)
			.json(content)
			.send()
			.await?
			.error_for_status()?
			.json::<Value>()
			.await?;

		Ok(field(&response, "event_id")?.try_into()?)
	}

	/// A send's status and errcode, without failing on an error status.
	async fn send_status(
		&self,
		room_id: &RoomId,
		txn_id: &str,
		content: &Value,
	) -> Result<(u16, Option<String>)> {
		let response = self
			.services
			.client
			.clients
			.default
			.put(self.send_url(room_id, txn_id))
			.bearer_auth(self.token)
			.json(content)
			.send()
			.await?;

		let status = response.status().as_u16();
		let body = response.json::<Value>().await.unwrap_or_default();
		let errcode = body
			.get("errcode")
			.and_then(Value::as_str)
			.map(ToOwned::to_owned);

		Ok((status, errcode))
	}

	async fn post(&self, path: &str, body: &Value) -> Result<Value> {
		Ok(self
			.services
			.client
			.clients
			.default
			.post(self.url(path))
			.bearer_auth(self.token)
			.json(body)
			.send()
			.await?
			.error_for_status()?
			.json::<Value>()
			.await?)
	}

	fn send_url(&self, room_id: &RoomId, txn_id: &str) -> String {
		self.url(&format!("rooms/{room_id}/send/m.room.message/thread-root-{txn_id}"))
	}

	fn url(&self, path: &str) -> String { format!("{}/_matrix/client/v3/{path}", self.base) }
}

/// Wait for the listener to answer, which the boot does not itself await.
async fn wait_until_ready(services: &Services, base: &str) -> Result {
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
	.map_err(|_| err!("server listener did not become ready"))
}

/// Register a local user and give it a device holding `token`.
async fn register(services: &Services, localpart: &str, token: &str) -> Result<OwnedUserId> {
	let user_id = UserId::parse_with_server_name(localpart, services.globals.server_name())?;

	services
		.users
		.full_register(Register {
			user_id: Some(&user_id),
			password: Some("thread-root-precheck-password"),
			..Default::default()
		})
		.await?;

	services
		.users
		.create_device(&user_id, None, (Some(token), None), None, None, None)
		.await?;

	Ok(user_id)
}

/// Read a required string field out of a response body.
fn field<'a>(response: &'a Value, name: &str) -> Result<&'a str> {
	response
		.get(name)
		.and_then(Value::as_str)
		.ok_or_else(|| err!("response omitted {name}"))
}
