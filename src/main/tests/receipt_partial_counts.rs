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

const READER_TOKEN: &str = "receipt-partial-counts-reader-token";
const WRITER_TOKEN: &str = "receipt-partial-counts-writer-token";

/// A receipt reads up to its event, not the whole room.
///
/// Notified events after the receipt stay counted, per thread scope: a reader
/// who stops partway through a room keeps the rest unread, mentions included.
/// A scope's count never rises on a receipt, so an earlier receipt cannot
/// bring back what a later one already read.
#[test]
fn receipt_keeps_later_events_unread() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path = PathBuf::from(root).join(format!("tuwunel-receipt-counts-{}", process_id()));

	let mut args = Args::default_test(&["fresh", "cleanup"]);

	args.option.extend([
		format!("database_path={db_path:?}"),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
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
	let counts = Counts { services, user_id: &reader_id };

	partial_read(&reader, &writer, &counts).await?;
	read_markers_partial_read(&reader, &writer, &counts).await?;
	thread_scopes(&reader, &writer, &counts).await?;

	Ok(())
}

/// Reading the first of three messages leaves the other two, and the mention
/// among them, unread; reading the last clears the room.
async fn partial_read(reader: &Client<'_>, writer: &Client<'_>, counts: &Counts<'_>) -> Result {
	let room = writer.create_room().await?;
	reader.join(&room).await?;

	let first = writer.text(&room, "a1", "first", None).await?;
	writer
		.text(&room, "a2", "second", Some(counts.user_id))
		.await?;

	let last = writer.text(&room, "a3", "third", None).await?;
	counts.wait_main(&room, (3, 1)).await?;

	reader.receipt(&room, &first, None).await?;
	counts
		.expect_main(&room, (2, 1), "a receipt on the first message")
		.await?;

	reader.receipt(&room, &last, None).await?;
	counts
		.expect_main(&room, (0, 0), "a receipt on the last message")
		.await
}

/// `/read_markers` reads up to its receipt the same way.
async fn read_markers_partial_read(
	reader: &Client<'_>,
	writer: &Client<'_>,
	counts: &Counts<'_>,
) -> Result {
	let room = writer.create_room().await?;
	reader.join(&room).await?;

	let first = writer.text(&room, "b1", "first", None).await?;
	writer
		.text(&room, "b2", "second", Some(counts.user_id))
		.await?;

	counts.wait_main(&room, (2, 1)).await?;

	reader.read_markers(&room, &first).await?;
	counts
		.expect_main(&room, (1, 1), "a read marker on the first message")
		.await
}

/// Main and thread receipts each read only their own scope, up to their event,
/// and an unthreaded receipt behind a thread's own receipt leaves that thread
/// as it was.
async fn thread_scopes(reader: &Client<'_>, writer: &Client<'_>, counts: &Counts<'_>) -> Result {
	let room = writer.create_room().await?;
	reader.join(&room).await?;

	let thread_root = writer.text(&room, "c1", "root", None).await?;
	let first_reply = writer
		.reply_in_thread(&room, "c2", &thread_root, None)
		.await?;

	writer
		.reply_in_thread(&room, "c3", &thread_root, Some(counts.user_id))
		.await?;

	writer.text(&room, "c4", "later", None).await?;
	counts.wait_main(&room, (2, 0)).await?;
	counts
		.wait_thread(&room, &thread_root, (2, 1))
		.await?;

	reader
		.receipt(&room, &thread_root, Some("main"))
		.await?;

	counts
		.expect_main(&room, (1, 0), "a main receipt on the thread root")
		.await?;
	counts
		.expect_thread(&room, &thread_root, (2, 1), "a main receipt")
		.await?;

	reader
		.receipt(&room, &first_reply, Some(thread_root.as_str()))
		.await?;

	counts
		.expect_thread(&room, &thread_root, (1, 1), "a thread receipt on its first reply")
		.await?;
	counts
		.expect_main(&room, (1, 0), "a thread receipt")
		.await?;

	// An unthreaded receipt arriving later, on an event behind the thread's
	// receipt: the thread keeps what its own receipt left.
	let newest = writer.text(&room, "c5", "newest", None).await?;
	counts.wait_main(&room, (2, 0)).await?;

	reader.receipt(&room, &thread_root, None).await?;
	counts
		.expect_main(&room, (2, 0), "an unthreaded receipt on the thread root")
		.await?;

	counts
		.expect_thread(&room, &thread_root, (1, 1), "an unthreaded receipt behind the thread's")
		.await?;

	// Unthreaded on the newest message reads the threads before it too.
	reader.receipt(&room, &newest, None).await?;
	counts
		.expect_main(&room, (0, 0), "an unthreaded receipt on the newest message")
		.await?;

	counts
		.expect_thread(&room, &thread_root, (0, 0), "an unthreaded receipt on the newest message")
		.await
}

/// One user's view of the room's counts, as the server stores them.
struct Counts<'a> {
	services: &'a Services,
	user_id: &'a UserId,
}

impl Counts<'_> {
	async fn main(&self, room_id: &RoomId) -> (u64, u64) {
		let pusher = &self.services.pusher;
		let notifications = pusher
			.notification_count(self.user_id, room_id)
			.await;

		let highlights = pusher
			.highlight_count(self.user_id, room_id)
			.await;

		(notifications, highlights)
	}

	async fn threads(&self, room_id: &RoomId) -> BTreeMap<OwnedEventId, (u64, u64)> {
		self.services
			.pusher
			.thread_notification_counts(self.user_id, room_id)
			.await
	}

	/// Push evaluation trails the send response, so a count is polled until
	/// it arrives.
	async fn wait_main(&self, room_id: &RoomId, want: (u64, u64)) -> Result {
		timeout(Duration::from_secs(5), async {
			while self.main(room_id).await != want {
				sleep(Duration::from_millis(20)).await;
			}
		})
		.await
		.map_err(|_| err!("the room's counts did not reach {want:?}"))
	}

	async fn wait_thread(&self, room_id: &RoomId, root: &EventId, want: (u64, u64)) -> Result {
		timeout(Duration::from_secs(5), async {
			while self.threads(room_id).await.get(root).copied() != Some(want) {
				sleep(Duration::from_millis(20)).await;
			}
		})
		.await
		.map_err(|_| err!("the thread's counts did not reach {want:?}"))
	}

	/// A receipt's counts are written before its response, so one sample
	/// after it is the outcome.
	async fn expect_main(&self, room_id: &RoomId, want: (u64, u64), after: &str) -> Result {
		let got = self.main(room_id).await;
		if got != want {
			return Err!("after {after}, the room's counts are {got:?}, not {want:?}");
		}

		Ok(())
	}

	async fn expect_thread(
		&self,
		room_id: &RoomId,
		root: &EventId,
		want: (u64, u64),
		after: &str,
	) -> Result {
		let got = self
			.threads(room_id)
			.await
			.get(root)
			.copied()
			.unwrap_or_default();

		if got != want {
			return Err!("after {after}, the thread's counts are {got:?}, not {want:?}");
		}

		Ok(())
	}
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

	/// A text message, mentioning `mention` when given.
	async fn text(
		&self,
		room_id: &RoomId,
		txn_id: &str,
		body: &str,
		mention: Option<&UserId>,
	) -> Result<OwnedEventId> {
		let mut content = json!({ "msgtype": "m.text", "body": body });
		if let Some(user_id) = mention {
			content["m.mentions"] = json!({ "user_ids": [user_id] });
		}

		self.send(room_id, txn_id, &content).await
	}

	async fn reply_in_thread(
		&self,
		room_id: &RoomId,
		txn_id: &str,
		root: &EventId,
		mention: Option<&UserId>,
	) -> Result<OwnedEventId> {
		let mut content = json!({
			"msgtype": "m.text",
			"body": "in the thread",
			"m.relates_to": { "rel_type": "m.thread", "event_id": root },
		});

		if let Some(user_id) = mention {
			content["m.mentions"] = json!({ "user_ids": [user_id] });
		}

		self.send(room_id, txn_id, &content).await
	}

	async fn send(
		&self,
		room_id: &RoomId,
		txn_id: &str,
		content: &Value,
	) -> Result<OwnedEventId> {
		let path = format!("rooms/{room_id}/send/m.room.message/receipt-counts-{txn_id}");
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

	/// A public `m.read` receipt, for `thread` when given.
	async fn receipt(
		&self,
		room_id: &RoomId,
		event_id: &EventId,
		thread: Option<&str>,
	) -> Result {
		let body = thread.map_or_else(|| json!({}), |thread| json!({ "thread_id": thread }));

		self.post(&format!("rooms/{room_id}/receipt/m.read/{event_id}"), &body)
			.await
			.map(|_| ())
	}

	async fn read_markers(&self, room_id: &RoomId, event_id: &EventId) -> Result {
		self.post(&format!("rooms/{room_id}/read_markers"), &json!({ "m.read": event_id }))
			.await
			.map(|_| ())
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
			password: Some("receipt-counts-password"),
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
