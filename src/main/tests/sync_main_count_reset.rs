#![cfg(test)]

use std::{
	env::var, fs::remove_dir_all, net::TcpListener, path::PathBuf, process::id as process_id,
	time::Duration,
};

use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId},
};
use tuwunel_service::{Services, users::Register};

const READER_TOKEN: &str = "sync-main-count-reset-reader-token";
const WRITER_TOKEN: &str = "sync-main-count-reset-writer-token";

/// A read that resets a room's count to zero is sent, even when the same
/// sync carries timeline events.
///
/// The zero used to be sent only on a sync with an empty timeline. A read
/// that came with a message (the reader's own send, or a read while someone
/// posted) left the count out, and the next quiet sync left the room out, so
/// the client kept showing the old count.
#[test]
fn a_reset_count_is_sent_with_timeline_events() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path =
		PathBuf::from(root).join(format!("tuwunel-sync-main-count-reset-{}", process_id()));

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

	let reader_id = register(services, "countreader", READER_TOKEN).await?;
	register(services, "countwriter", WRITER_TOKEN).await?;

	let reader = Client { services, base, token: READER_TOKEN };
	let writer = Client { services, base, token: WRITER_TOKEN };

	let room = writer.create_room().await?;
	reader.join(&room).await?;

	writer.text(&room, "m1", "one").await?;
	wait_count(services, &reader_id, &room, 1).await?;
	let (response, since) = reader.sync(None).await?;
	expect_count(&response, &room, Some(1), "the initial sync")?;

	// The reader's own send reads the room: the zero comes with that message.
	reader.text(&room, "r1", "my reply").await?;
	wait_count(services, &reader_id, &room, 0).await?;
	let (response, since) = reader.sync(Some(&since)).await?;
	expect_timeline(&response, &room, "the reader's own send")?;
	expect_count(&response, &room, Some(0), "the reader's own send")?;

	// A new message raises the count, with no read: no zero is involved.
	writer.text(&room, "m2", "two").await?;
	wait_count(services, &reader_id, &room, 1).await?;
	let (response, since) = reader.sync(Some(&since)).await?;
	expect_count(&response, &room, Some(1), "another user's message")?;

	// Another message, read before the next sync: that sync carries the
	// message and must carry the zero.
	let m3 = writer.text(&room, "m3", "three").await?;
	reader.receipt(&room, &m3).await?;
	wait_count(services, &reader_id, &room, 0).await?;
	let (response, since) = reader.sync(Some(&since)).await?;
	expect_timeline(&response, &room, "reading a message that came in the same sync")?;
	expect_count(&response, &room, Some(0), "reading a message that came in the same sync")?;

	// A read on a quiet sync, as before.
	writer.text(&room, "m4", "four").await?;
	wait_count(services, &reader_id, &room, 1).await?;
	let (_, since) = reader.sync(Some(&since)).await?;
	let m5 = writer.text(&room, "m5", "five").await?;
	let (_, since) = reader.sync(Some(&since)).await?;
	reader.receipt(&room, &m5).await?;
	wait_count(services, &reader_id, &room, 0).await?;
	let (response, since) = reader.sync(Some(&since)).await?;
	expect_count(&response, &room, Some(0), "a read on a quiet sync")?;

	// Nothing new: the room stays out, so the sync would long-poll.
	let (response, _) = reader.sync(Some(&since)).await?;
	expect_room_absent(&response, &room, "a sync with nothing new")
}

/// The room's main `notification_count` in this sync, `None` when left out.
fn expect_count(response: &Value, room_id: &RoomId, want: Option<u64>, after: &str) -> Result {
	let got = response
		.pointer(&format!(
			"/rooms/join/{}/unread_notifications/notification_count",
			pointer_escape(room_id.as_str())
		))
		.and_then(Value::as_u64);

	if got != want {
		return Err(err!("after {after}: notification_count {got:?}, want {want:?}: {response}"));
	}

	Ok(())
}

/// The room carries timeline events in this sync.
fn expect_timeline(response: &Value, room_id: &RoomId, after: &str) -> Result {
	let events = response
		.pointer(&format!("/rooms/join/{}/timeline/events", pointer_escape(room_id.as_str())))
		.and_then(Value::as_array)
		.map_or(0, Vec::len);

	if events == 0 {
		return Err(err!("after {after}: the sync carried no timeline events: {response}"));
	}

	Ok(())
}

fn expect_room_absent(response: &Value, room_id: &RoomId, after: &str) -> Result {
	if response
		.pointer(&format!("/rooms/join/{}", pointer_escape(room_id.as_str())))
		.is_some()
	{
		return Err(err!("after {after}: the room was in the sync: {response}"));
	}

	Ok(())
}

async fn wait_count(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	want: u64,
) -> Result {
	timeout(Duration::from_secs(5), async {
		while services
			.pusher
			.notification_count(user_id, room_id)
			.await != want
		{
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| err!("the room's count never reached {want}"))
}

fn pointer_escape(key: &str) -> String { key.replace('~', "~0").replace('/', "~1") }

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

	async fn text(&self, room_id: &RoomId, txn_id: &str, body: &str) -> Result<OwnedEventId> {
		self.send(room_id, txn_id, &json!({ "msgtype": "m.text", "body": body }))
			.await
	}

	async fn send(
		&self,
		room_id: &RoomId,
		txn_id: &str,
		content: &Value,
	) -> Result<OwnedEventId> {
		let path = format!("rooms/{room_id}/send/m.room.message/main-count-reset-{txn_id}");
		let response = self.put(&path, content).await?;

		Ok(field(&response, "event_id")?.try_into()?)
	}

	/// A public, unthreaded `m.read` receipt.
	async fn receipt(&self, room_id: &RoomId, event_id: &EventId) -> Result {
		self.post(&format!("rooms/{room_id}/receipt/m.read/{event_id}"), &json!({}))
			.await
			.map(|_| ())
	}

	/// A sync with thread counts opted in; returns the body and `next_batch`.
	async fn sync(&self, since: Option<&str>) -> Result<(Value, String)> {
		let filter = json!({ "room": { "timeline": { "unread_thread_notifications": true } } });
		let mut query = vec![("filter", filter.to_string()), ("timeout", "0".to_owned())];
		if let Some(since) = since {
			query.push(("since", since.to_owned()));
		}

		let response = self
			.services
			.client
			.clients
			.default
			.get(self.url("sync"))
			.query(&query)
			.bearer_auth(self.token)
			.send()
			.await?
			.error_for_status()?
			.json::<Value>()
			.await?;

		let next_batch = field(&response, "next_batch")?.to_owned();

		Ok((response, next_batch))
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

	async fn put(&self, path: &str, body: &Value) -> Result<Value> {
		Ok(self
			.services
			.client
			.clients
			.default
			.put(self.url(path))
			.bearer_auth(self.token)
			.json(body)
			.send()
			.await?
			.error_for_status()?
			.json::<Value>()
			.await?)
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
			password: Some("sync-main-count-reset-password"),
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
