#![cfg(test)]

use std::{
	collections::BTreeMap, env::var, fs::remove_dir_all, net::TcpListener, path::PathBuf,
	process::id as process_id, time::Duration,
};

use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpListener as GatewayListener,
	spawn,
	sync::mpsc::{UnboundedReceiver, unbounded_channel},
	time::{sleep, timeout},
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	matrix::pdu::RawPduId,
	ruma::{
		EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
		api::client::push::{
			Pusher, PusherIds, PusherInit, PusherKind,
			set_pusher::v3::Request as SetPusherRequest,
		},
		device_id,
		push::HttpPusherData,
	},
};
use tuwunel_service::{Services, users::Register};

const READER_TOKEN: &str = "receipt-partial-counts-reader-token";
const WRITER_TOKEN: &str = "receipt-partial-counts-writer-token";
const PUSHKEY: &str = "receipt-partial-counts-pushkey";
const GATEWAY_PUSHKEY: &str = "receipt-partial-counts-gateway";

/// A receipt reads up to its event, not the whole room.
///
/// Notified events after the receipt stay counted, per thread scope: a reader
/// who stops partway through a room keeps the rest unread, mentions included.
/// A scope's count never rises on a receipt, so an earlier receipt cannot
/// bring back what a later one already read. Deferred pushes follow the
/// same scope and position.
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
	let counts = Counts { services, user_id: &reader_id };

	partial_read(&reader, &writer, &counts).await?;
	read_markers_partial_read(&reader, &writer, &counts).await?;
	thread_scopes(&reader, &writer, &counts).await?;
	own_send_scope(&reader, &writer, &counts).await?;
	deferred_pushes(&reader, &writer, &counts).await?;
	thread_push_delivered(&reader, &writer, &counts).await?;

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

/// A user's own send reads only its scope: writing in the main timeline keeps
/// their unread threads, and replying in one thread keeps the others and the
/// main timeline.
async fn own_send_scope(reader: &Client<'_>, writer: &Client<'_>, counts: &Counts<'_>) -> Result {
	let room = writer.create_room().await?;
	reader.join(&room).await?;

	let first_root = writer
		.text(&room, "s1", "first root", None)
		.await?;
	let second_root = writer
		.text(&room, "s2", "second root", None)
		.await?;
	writer
		.reply_in_thread(&room, "s3", &first_root, None)
		.await?;
	writer
		.reply_in_thread(&room, "s4", &second_root, Some(counts.user_id))
		.await?;

	counts.wait_main(&room, (2, 0)).await?;
	counts
		.wait_thread(&room, &first_root, (1, 0))
		.await?;
	counts
		.wait_thread(&room, &second_root, (1, 1))
		.await?;

	reader
		.text(&room, "s5", "in the main timeline", None)
		.await?;
	counts
		.expect_main(&room, (0, 0), "the reader's own main send")
		.await?;
	counts
		.expect_thread(&room, &first_root, (1, 0), "the reader's own main send")
		.await?;
	counts
		.expect_thread(&room, &second_root, (1, 1), "the reader's own main send")
		.await?;

	writer
		.text(&room, "s6", "main again", None)
		.await?;
	counts.wait_main(&room, (1, 0)).await?;

	reader
		.reply_in_thread(&room, "s7", &first_root, None)
		.await?;
	counts
		.expect_thread(&room, &first_root, (0, 0), "the reader's own reply in that thread")
		.await?;
	counts
		.expect_thread(&room, &second_root, (1, 1), "the reader's reply in another thread")
		.await?;
	counts
		.expect_main(&room, (1, 0), "the reader's reply in a thread")
		.await
}

/// A receipt drops the deferred pushes it reads and keeps the rest: those
/// after it, and those in another thread scope.
async fn deferred_pushes(
	reader: &Client<'_>,
	writer: &Client<'_>,
	counts: &Counts<'_>,
) -> Result {
	let room = writer.create_room().await?;
	reader.join(&room).await?;

	let root = writer.text(&room, "d1", "root", None).await?;
	let reply = writer
		.reply_in_thread(&room, "d2", &root, Some(counts.user_id))
		.await?;

	let main = writer.text(&room, "d3", "main", None).await?;
	let last = writer.text(&room, "d4", "last", None).await?;
	counts.wait_main(&room, (3, 0)).await?;
	counts.wait_thread(&room, &root, (1, 1)).await?;

	let queued = counts
		.defer(&room, &[&root, &reply, &main, &last])
		.await?;
	let [root, reply, main, last] = queued.as_slice() else {
		return Err!("four pushes were deferred");
	};

	// Main up to `main`: the thread's reply is another scope, `last` is after.
	reader
		.receipt(&room, &main.0, Some("main"))
		.await?;
	counts.expect_deferred(&room, &[reply.1, last.1], "a main receipt")?;

	// The thread's own receipt reads its reply; the main push after stays.
	reader
		.receipt(&room, &reply.0, Some(root.0.as_str()))
		.await?;

	counts.expect_deferred(&room, &[last.1], "a thread receipt")?;

	// Unthreaded on the first message: nothing left is at or before it.
	reader.receipt(&room, &root.0, None).await?;
	counts.expect_deferred(&room, &[last.1], "an unthreaded receipt behind the rest")
}

/// A thread's deferred push is delivered after the main timeline is read: the
/// flush asks whether the room has anything unread, threads included.
async fn thread_push_delivered(
	reader: &Client<'_>,
	writer: &Client<'_>,
	counts: &Counts<'_>,
) -> Result {
	let room = writer.create_room().await?;
	reader.join(&room).await?;

	let root = writer.text(&room, "e1", "root", None).await?;
	let reply = writer
		.reply_in_thread(&room, "e2", &root, Some(counts.user_id))
		.await?;

	let main = writer.text(&room, "e3", "main", None).await?;
	counts.wait_main(&room, (2, 0)).await?;
	counts.wait_thread(&room, &root, (1, 1)).await?;

	let mut delivered = counts.gateway().await?;
	let queued = counts
		.defer_to(GATEWAY_PUSHKEY, &room, &[&root, &reply, &main])
		.await?;

	reader.receipt(&room, &main, Some("main")).await?;
	counts
		.expect_main(&room, (0, 0), "a main receipt on the latest main message")
		.await?;

	counts.expect_deferred(&room, &[queued[1].1], "a main receipt with a thread unread")?;

	counts
		.services
		.sending
		.flush_suppressed_for_user(counts.user_id.to_owned(), "receipt-partial-counts")
		.await;

	timeout(Duration::from_secs(5), async {
		while let Some(body) = delivered.recv().await {
			if body.contains(reply.as_str()) {
				return true;
			}
		}

		false
	})
	.await
	.ok()
	.filter(|found| *found)
	.ok_or_else(|| err!("the thread's deferred push was not delivered"))?;

	Ok(())
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

	/// A push gateway for the user, answering every notification; each
	/// request body arrives on the returned channel.
	async fn gateway(&self) -> Result<UnboundedReceiver<String>> {
		let listener = GatewayListener::bind(("127.0.0.1", 0)).await?;
		let url = format!("http://{}/_matrix/push/v1/notify", listener.local_addr()?);
		let (tx, rx) = unbounded_channel();

		spawn(async move {
			while let Ok((mut socket, _)) = listener.accept().await {
				let mut request = Vec::new();
				let mut buf = [0_u8; 4096];
				while let Ok(Ok(read @ 1..)) =
					timeout(Duration::from_millis(200), socket.read(&mut buf)).await
				{
					request.extend_from_slice(&buf[..read]);
				}

				let response = "HTTP/1.1 200 OK\r\nContent-Type: \
				                application/json\r\nContent-Length: 15\r\nConnection: \
				                close\r\n\r\n{\"rejected\":[]}";

				socket.write_all(response.as_bytes()).await.ok();
				tx.send(String::from_utf8_lossy(&request).into_owned())
					.ok();
			}
		});

		let pusher: Pusher = PusherInit {
			ids: PusherIds::new(GATEWAY_PUSHKEY.to_owned(), "receipt.counts.test".to_owned()),
			kind: PusherKind::Http(HttpPusherData::new(url)),
			app_display_name: "Receipt counts".into(),
			device_display_name: "Receipt counts".into(),
			profile_tag: None,
			lang: "en".into(),
		}
		.into();

		self.services
			.pusher
			.set_pusher(
				self.user_id,
				device_id!("RECEIPTCOUNTS"),
				&SetPusherRequest::post(pusher).action,
			)
			.await?;

		Ok(rx)
	}

	/// Defers a push for each event, as the pusher does while the user is
	/// active, and returns each event with its PDU id.
	async fn defer(
		&self,
		room_id: &RoomId,
		events: &[&EventId],
	) -> Result<Vec<(OwnedEventId, RawPduId)>> {
		self.defer_to(PUSHKEY, room_id, events).await
	}

	async fn defer_to(
		&self,
		pushkey: &str,
		room_id: &RoomId,
		events: &[&EventId],
	) -> Result<Vec<(OwnedEventId, RawPduId)>> {
		let mut queued = Vec::new();
		for event_id in events {
			let pdu_id = self
				.services
				.timeline
				.get_pdu_id(event_id)
				.await?;
			if !self
				.services
				.pusher
				.queue_suppressed_push(self.user_id, pushkey, room_id, pdu_id)
			{
				return Err!("the push for {event_id} was not deferred");
			}

			queued.push(((*event_id).to_owned(), pdu_id));
		}

		Ok(queued)
	}

	/// The room's deferred pushes are `want`, in order; read without taking
	/// them.
	fn expect_deferred(&self, room_id: &RoomId, want: &[RawPduId], after: &str) -> Result {
		let got = self
			.services
			.pusher
			.suppressed_room_pdus(self.user_id, room_id);

		if got != want {
			return Err!("after {after}, the deferred pushes are {got:?}, not {want:?}");
		}

		Ok(())
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
