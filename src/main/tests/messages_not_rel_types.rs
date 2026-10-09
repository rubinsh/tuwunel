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

const TOKEN: &str = "messages-not-rel-types-test-access-token";

/// MSC3874 `not_rel_types` on `/messages`: a client pages a room's main
/// timeline without its thread replies, and a full page of main messages
/// comes back however many replies sit between them.
#[test]
fn messages_leave_out_the_excluded_relation_types() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path =
		PathBuf::from(root).join(format!("tuwunel-messages-not-rel-types-{}", process_id()));

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
	register(services, "threadwriter", TOKEN).await?;

	let client = Client { services, base, token: TOKEN };
	let room = client.create_room().await?;

	let main1 = client.text(&room, "main1", "one").await?;
	let thread_root = client.text(&room, "root", "a question").await?;
	let mut replies = Vec::new();
	for n in 0..4 {
		replies.push(
			client
				.thread_reply(&room, &thread_root, n)
				.await?,
		);
	}
	let main2 = client.text(&room, "main2", "two").await?;
	replies.push(
		client
			.thread_reply(&room, &thread_root, 4)
			.await?,
	);
	let main3 = client.text(&room, "main3", "three").await?;

	let only_messages = json!({ "types": ["m.room.message"] });
	let main_only = json!({
		"types": ["m.room.message"],
		"org.matrix.msc3874.not_rel_types": ["m.thread"],
	});

	// The first page holds the newest main messages, past the reply between them.
	let (page, end) = client
		.messages(&room, &main_only, 3, None)
		.await?;
	expect_page(&page, &[&main3, &main2, &thread_root], "the first main-timeline page")?;

	// The next page carries on past the four replies under the root.
	let (page, _) = client
		.messages(&room, &main_only, 3, end.as_deref())
		.await?;
	expect_page(&page, &[&main1], "the next main-timeline page")?;

	// Without the field, nothing is left out: the same page is mostly replies.
	let (page, _) = client
		.messages(&room, &only_messages, 3, None)
		.await?;
	expect_page(&page, &[&main3, &replies[4], &main2], "a page without not_rel_types")
}

/// The page's events, newest first, are exactly these.
fn expect_page(page: &[Value], want: &[&OwnedEventId], what: &str) -> Result {
	let got: Vec<&str> = page
		.iter()
		.filter_map(|event| event.get("event_id").and_then(Value::as_str))
		.collect();
	let want: Vec<&str> = want.iter().map(|id| id.as_str()).collect();

	if got != want {
		return Err(err!("{what}: got {got:?}, want {want:?}"));
	}

	Ok(())
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
		let path = format!("rooms/{room_id}/send/m.room.message/not-rel-types-{txn_id}");
		let response = self.put(&path, content).await?;

		Ok(field(&response, "event_id")?.try_into()?)
	}

	/// A reply in the thread under `root`.
	async fn thread_reply(
		&self,
		room_id: &RoomId,
		root: &EventId,
		n: u32,
	) -> Result<OwnedEventId> {
		let content = json!({
			"msgtype": "m.text",
			"body": format!("reply {n}"),
			"m.relates_to": { "rel_type": "m.thread", "event_id": root, "is_falling_back": true },
		});

		self.send(room_id, &format!("reply{n}"), &content)
			.await
	}

	/// A backward `/messages` page with this filter; returns its events and
	/// `end`.
	async fn messages(
		&self,
		room_id: &RoomId,
		filter: &Value,
		limit: u32,
		from: Option<&str>,
	) -> Result<(Vec<Value>, Option<String>)> {
		let mut query = vec![
			("dir", "b".to_owned()),
			("limit", limit.to_string()),
			("filter", filter.to_string()),
		];
		if let Some(from) = from {
			query.push(("from", from.to_owned()));
		}

		let response = self
			.services
			.client
			.clients
			.default
			.get(self.url(&format!("rooms/{room_id}/messages")))
			.query(&query)
			.bearer_auth(self.token)
			.send()
			.await?
			.error_for_status()?
			.json::<Value>()
			.await?;

		let chunk = response
			.get("chunk")
			.and_then(Value::as_array)
			.cloned()
			.unwrap_or_default();
		let end = response
			.get("end")
			.and_then(Value::as_str)
			.map(ToOwned::to_owned);

		Ok((chunk, end))
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
			password: Some("not-rel-types-password"),
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
