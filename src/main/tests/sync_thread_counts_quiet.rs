#![cfg(test)]

use std::{
	collections::BTreeMap, env::var, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id as process_id, time::Duration,
};

use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err,
	ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId},
};
use tuwunel_service::{Services, users::Register};

const READER_TOKEN: &str = "sync-thread-counts-quiet-reader-token";
const WRITER_TOKEN: &str = "sync-thread-counts-quiet-writer-token";

/// Every sync that carries a room carries all of its unread threads.
///
/// Clients take a thread missing from `unread_thread_notifications` as read:
/// matrix-js-sdk resets it to zero. A sync whose timeline is empty used to
/// list only the threads whose read cursor moved, so another user's typing or
/// receipt, or reading one thread, cleared every other thread on the client.
///
/// Set `SYNC_THREAD_COUNTS_DUMP` to a file path to keep the raw responses,
/// for replaying them through a client.
#[test]
fn quiet_syncs_keep_every_unread_thread() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path =
		PathBuf::from(root).join(format!("tuwunel-sync-thread-counts-{}", process_id()));

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

	let reader_id = register(services, "threadreader", READER_TOKEN).await?;
	let writer_id = register(services, "threadwriter", WRITER_TOKEN).await?;

	let reader = Client { services, base, token: READER_TOKEN };
	let writer = Client { services, base, token: WRITER_TOKEN };
	let mut syncs = Vec::new();

	let room = writer.create_room().await?;
	reader.join(&room).await?;

	let root_a = writer.text(&room, "a", "thread a").await?;
	let root_b = writer.text(&room, "b", "thread b").await?;
	let reply_a = writer
		.reply_in_thread(&room, "a1", &root_a)
		.await?;
	writer
		.reply_in_thread(&room, "b1", &root_b)
		.await?;

	wait_thread(services, &reader_id, &room, &root_a, (1, 0)).await?;
	wait_thread(services, &reader_id, &room, &root_b, (1, 0)).await?;

	let dump = json!({
		"user_id": reader_id,
		"room_id": room,
		"roots": { "a": root_a, "b": root_b },
	});

	let since = record(&mut syncs, &dump, reader.sync(None).await?)?;
	expect_threads(last(&syncs), &room, &[(&root_a, 1), (&root_b, 1)], "the initial sync")?;

	// Nothing new: the room stays out, so the sync would long-poll.
	record(&mut syncs, &dump, reader.sync(Some(&since)).await?)?;
	expect_room_absent(last(&syncs), &room, "a sync with nothing new")?;

	writer.typing(&room, &writer_id).await?;
	let since = record(&mut syncs, &dump, reader.sync(Some(&since)).await?)?;
	expect_threads(last(&syncs), &room, &[(&root_a, 1), (&root_b, 1)], "another user's typing")?;

	// Reading thread a sends it as zero, and thread b as still unread.
	reader
		.receipt(&room, &reply_a, Some(root_a.as_str()))
		.await?;

	wait_thread(services, &reader_id, &room, &root_a, (0, 0)).await?;
	let since = record(&mut syncs, &dump, reader.sync(Some(&since)).await?)?;
	expect_threads(last(&syncs), &room, &[(&root_a, 0), (&root_b, 1)], "reading thread a")?;

	// Another user's receipt. Thread a is read, so it may be left out.
	writer.receipt(&room, &root_b, None).await?;
	let since = record(&mut syncs, &dump, reader.sync(Some(&since)).await?)?;
	expect_threads(last(&syncs), &room, &[(&root_b, 1)], "another user's receipt")?;

	record(&mut syncs, &dump, reader.sync(Some(&since)).await?)?;
	expect_room_absent(last(&syncs), &room, "a sync with nothing new after the receipts")
}

/// Keep a sync response, rewriting the dump file (when asked for) before any
/// check runs, so a failing run still leaves what it saw. Returns
/// `next_batch`.
fn record(
	syncs: &mut Vec<Value>,
	dump: &Value,
	(response, next_batch): (Value, String),
) -> Result<String> {
	syncs.push(response);
	if let Ok(path) = var("SYNC_THREAD_COUNTS_DUMP") {
		let mut dump = dump.clone();
		dump["syncs"] = json!(syncs);
		std::fs::write(path, serde_json::to_vec_pretty(&dump)?)?;
	}

	Ok(next_batch)
}

fn last(syncs: &[Value]) -> &Value { syncs.last().expect("a sync was recorded") }

/// The room carries exactly these thread counts (notification counts).
fn expect_threads(
	response: &Value,
	room_id: &RoomId,
	want: &[(&EventId, u64)],
	after: &str,
) -> Result {
	let threads = response
		.pointer(&format!(
			"/rooms/join/{}/unread_thread_notifications",
			pointer_escape(room_id.as_str())
		))
		.and_then(Value::as_object)
		.ok_or_else(|| err!("after {after}: the room carried no thread counts: {response}"))?;

	let got: BTreeMap<&str, Option<u64>> = threads
		.iter()
		.map(|(root, counts)| {
			(
				root.as_str(),
				counts
					.get("notification_count")
					.and_then(Value::as_u64),
			)
		})
		.collect();

	let want: BTreeMap<&str, Option<u64>> = want
		.iter()
		.map(|(root, count)| (root.as_str(), Some(*count)))
		.collect();

	if got != want {
		return Err(err!("after {after}: thread counts {got:?}, want {want:?}"));
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

fn pointer_escape(key: &str) -> String { key.replace('~', "~0").replace('/', "~1") }

async fn wait_thread(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	root: &EventId,
	want: (u64, u64),
) -> Result {
	timeout(Duration::from_secs(5), async {
		loop {
			let counts = services
				.pusher
				.thread_notification_counts(user_id, room_id)
				.await;

			if counts.get(root).copied().unwrap_or_default() == want {
				return;
			}

			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| err!("thread {root} never reached {want:?}"))
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

	async fn text(&self, room_id: &RoomId, txn_id: &str, body: &str) -> Result<OwnedEventId> {
		self.send(room_id, txn_id, &json!({ "msgtype": "m.text", "body": body }))
			.await
	}

	async fn reply_in_thread(
		&self,
		room_id: &RoomId,
		txn_id: &str,
		root: &EventId,
	) -> Result<OwnedEventId> {
		let content = json!({
			"msgtype": "m.text",
			"body": "in the thread",
			"m.relates_to": { "rel_type": "m.thread", "event_id": root },
		});

		self.send(room_id, txn_id, &content).await
	}

	async fn send(
		&self,
		room_id: &RoomId,
		txn_id: &str,
		content: &Value,
	) -> Result<OwnedEventId> {
		let path = format!("rooms/{room_id}/send/m.room.message/thread-counts-{txn_id}");
		let response = self.put(&path, content).await?;

		Ok(field(&response, "event_id")?.try_into()?)
	}

	async fn typing(&self, room_id: &RoomId, user_id: &UserId) -> Result {
		let body = json!({ "typing": true, "timeout": 30_000 });

		self.put(&format!("rooms/{room_id}/typing/{user_id}"), &body)
			.await
			.map(|_| ())
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
			password: Some("sync-thread-counts-password"),
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
