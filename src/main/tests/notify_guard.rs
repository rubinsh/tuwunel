#![cfg(test)]

//! The notification guard end to end: settings written through the client
//! API as account data, activity reported through its endpoint, and pushes
//! driven through the pusher service to stub gateways. A dropped notice
//! returns `Ok(())` and never reaches the gateway; badge pushes are not
//! guarded.

use std::{
	env::var,
	fs::remove_dir_all,
	net::TcpListener as StdListener,
	path::PathBuf,
	process::id as process_id,
	str::from_utf8,
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	spawn,
	sync::mpsc::{UnboundedReceiver, UnboundedSender, error::TryRecvError, unbounded_channel},
	task::JoinHandle,
	time::{sleep, timeout},
};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	matrix::{Pdu, PduCount, PduId, RawPduId},
	ruma::{
		DeviceId, OwnedRoomId, OwnedUserId, UserId,
		api::client::push::{
			Pusher, PusherIds, PusherInit, PusherKind,
			set_pusher::v3::Request as SetPusherRequest,
		},
		device_id,
		push::{HttpPusherData, Ruleset},
	},
};
use tuwunel_database::Json;
use tuwunel_service::{Services, users::Register};

const PC_TOKEN: &str = "notify-guard-test-pc-access-token";
const PHONE_TOKEN: &str = "notify-guard-test-phone-access-token";
const BOT_TOKEN: &str = "notify-guard-test-bot-access-token";
const APP_ID: &str = "app.tuwunel.guard";
const HUMAN: &str = "@alice:remote.example";
const OTHER_HUMAN: &str = "@carol:remote.example";
const SHORT_ROOM_ID: u64 = 0x6A7D;

type Capture = UnboundedReceiver<Vec<u8>>;

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
	fn drop(&mut self) { self.0.abort(); }
}

/// The account under test, its room, and a client for each of its devices.
struct Fixture<'a> {
	services: &'a Services,
	base: &'a str,
	user: OwnedUserId,
	bot: OwnedUserId,
	room: OwnedRoomId,
	ruleset: Ruleset,
}

/// A registered pusher whose gateway records what it receives.
struct Gateway {
	pusher: Pusher,
	rx: Capture,
	_stub: AbortOnDrop,
}

#[test]
fn notify_guard() -> Result {
	let listener = StdListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let db_path = PathBuf::from(root).join(format!("tuwunel-notify-guard-{}", process_id()));

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

	let user = register(services, "roy", &[
		(device_id!("PC"), PC_TOKEN),
		(device_id!("PHONE"), PHONE_TOKEN),
	])
	.await?;
	let bot = register(services, "bot", &[(device_id!("BOT"), BOT_TOKEN)]).await?;

	let created = http(services, "POST", &format!("{base}/_matrix/client/v3/createRoom"))
		.bearer_auth(PC_TOKEN)
		.json(&json!({ "preset": "public_chat" }))
		.send()
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?;
	let room: OwnedRoomId = created["room_id"]
		.as_str()
		.ok_or_else(|| err!("createRoom returned no room_id"))?
		.try_into()?;

	http(services, "POST", &format!("{base}/_matrix/client/v3/join/{room}"))
		.bearer_auth(BOT_TOKEN)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?;

	let ruleset = Ruleset::server_default(&user);
	let fx = Fixture { services, base, user, bot, room, ruleset };

	advertised(&fx).await?;
	no_settings_deliver_as_before(&fx).await?;
	burst_limits_alerts_per_room(&fx).await?;
	failed_sends_keep_their_alert(&fx).await?;
	room_levels(&fx).await?;
	bot_rooms_take_the_bot_room_default(&fx).await?;
	pause_and_quiet_hours(&fx).await?;
	active_device_suppression(&fx).await?;
	activity_endpoint_errors(&fx).await?;
	deferred_flush_is_guarded(&fx).await
}

/// Clients show their guard settings only where the server advertises it.
async fn advertised(fx: &Fixture<'_>) -> Result {
	let versions = http(fx.services, "GET", &format!("{}/_matrix/client/versions", fx.base))
		.send()
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?;

	let flag = &versions["unstable_features"]["com.chat-harness.notify_guard"];
	if flag != &json!(true) {
		return Err!("/versions does not advertise the guard: {flag}");
	}

	Ok(())
}

/// Without any guard settings, every notice is delivered: no burst limit.
async fn no_settings_deliver_as_before(fx: &Fixture<'_>) -> Result {
	let mut gw = fx
		.gateway("pk-plain", device_id!("PHONE"), 0)
		.await?;

	for id in ["$plain-1:x", "$plain-2:x"] {
		fx.push(&gw, &fx.message(id, HUMAN, json!({ "body": "hi" })))
			.await?;
		expect_delivered(&mut gw, id)?;
	}

	Ok(())
}

/// A present defaults key without `burstWindowSec` holds a room to one
/// alert per 120 s, and a dropped notice stays dropped when retried.
async fn burst_limits_alerts_per_room(fx: &Fixture<'_>) -> Result {
	fx.settings("com.chat-harness.notify.defaults", json!({ "v": 1 }))
		.await?;
	let mut gw = fx
		.gateway("pk-burst", device_id!("PHONE"), 0)
		.await?;

	let first = fx.message("$burst-1:x", HUMAN, json!({ "body": "one" }));
	let second = fx.message("$burst-2:x", HUMAN, json!({ "body": "two" }));

	fx.push(&gw, &first).await?;
	expect_delivered(&mut gw, "$burst-1:x")?;

	fx.push(&gw, &second).await?;
	expect_nothing(&mut gw, "a second alert inside the burst window")?;
	fx.push(&gw, &second).await?;
	expect_nothing(&mut gw, "a retry of a dropped notice")
}

/// A failed send keeps nothing: its retry alerts once, and while it has not
/// succeeded another event may take its burst slot.
async fn failed_sends_keep_their_alert(fx: &Fixture<'_>) -> Result {
	let mut gw = fx
		.gateway("pk-retry", device_id!("PHONE"), 1)
		.await?;
	let first = fx.message("$retry-1:x", HUMAN, json!({ "body": "one" }));

	if fx.push(&gw, &first).await.is_ok() {
		return Err!("the gateway refused the first send, yet it reported success");
	}
	expect_delivered(&mut gw, "$retry-1:x")?;

	fx.push(&gw, &first).await?;
	expect_delivered(&mut gw, "$retry-1:x")?;

	let later = fx.message("$retry-2:x", HUMAN, json!({ "body": "two" }));
	fx.push(&gw, &later).await?;
	expect_nothing(&mut gw, "an alert after the retried one, inside the window")?;

	let mut gw = fx
		.gateway("pk-release", device_id!("PHONE"), 1)
		.await?;
	let failed = fx.message("$release-1:x", HUMAN, json!({ "body": "one" }));
	if fx.push(&gw, &failed).await.is_ok() {
		return Err!("the gateway refused the send, yet it reported success");
	}
	expect_delivered(&mut gw, "$release-1:x")?;

	let next = fx.message("$release-2:x", HUMAN, json!({ "body": "two" }));
	fx.push(&gw, &next).await?;
	expect_delivered(&mut gw, "$release-2:x")
}

async fn room_levels(fx: &Fixture<'_>) -> Result {
	fx.settings("com.chat-harness.notify.defaults", json!({ "v": 1, "burstWindowSec": 0 }))
		.await?;
	fx.settings("com.chat-harness.notify.bots", json!({ "v": 1, "users": [fx.bot] }))
		.await?;
	let mut gw = fx
		.gateway("pk-levels", device_id!("PHONE"), 0)
		.await?;
	let mention = json!({ "body": "hi", "m.mentions": { "user_ids": [fx.user] } });

	fx.room_settings(json!({ "level": "mentions" }))
		.await?;
	fx.expect(&mut gw, "$lvl-plain:x", HUMAN, json!({ "body": "hi" }), false)
		.await?;
	fx.expect(&mut gw, "$lvl-mention:x", HUMAN, mention.clone(), true)
		.await?;

	// A highlighted body is not a mention when m.mentions says otherwise.
	let named = json!({ "body": format!("{} hi", fx.user), "m.mentions": {} });
	fx.expect(&mut gw, "$lvl-named:x", HUMAN, named, false)
		.await?;

	fx.room_settings(json!({ "level": "humans" }))
		.await?;
	fx.expect(&mut gw, "$lvl-bot:x", fx.bot.as_str(), mention.clone(), false)
		.await?;
	let marked = json!({ "body": "hi", "com.chat-harness.sender_kind": "bot" });
	fx.expect(&mut gw, "$lvl-marked:x", OTHER_HUMAN, marked, false)
		.await?;
	fx.expect(&mut gw, "$lvl-human:x", HUMAN, json!({ "body": "hi" }), true)
		.await?;

	fx.room_settings(json!({ "level": "muted" }))
		.await?;
	fx.expect(&mut gw, "$lvl-muted:x", HUMAN, mention.clone(), false)
		.await?;

	// The badge still counts while the room is muted. A fresh pusher, since
	// one that already holds the current count is not sent it again.
	let mut badge = fx
		.gateway("pk-badge", device_id!("PHONE"), 0)
		.await?;
	fx.services
		.pusher
		.send_badge_notice(&fx.user, &badge.pusher)
		.await?;
	recv(&mut badge.rx, "a badge push in a muted room")?;

	let snoozed = json!({ "snoozeUntil": now_ms().saturating_add(3_600_000) });
	fx.room_settings(snoozed).await?;
	fx.expect(&mut gw, "$lvl-snoozed:x", HUMAN, mention.clone(), false)
		.await?;

	fx.room_settings(json!({ "mutedThreads": ["$root:x"] }))
		.await?;
	let reply = json!({
		"body": "in the thread",
		"m.relates_to": { "rel_type": "m.thread", "event_id": "$root:x" },
	});
	fx.expect(&mut gw, "$lvl-thread:x", HUMAN, reply, false)
		.await?;
	fx.expect(&mut gw, "$lvl-main:x", HUMAN, json!({ "body": "main" }), true)
		.await?;

	fx.settings("com.chat-harness.notify.rooms", json!({}))
		.await
}

/// A room whose other members are all bots takes the bot-room default.
async fn bot_rooms_take_the_bot_room_default(fx: &Fixture<'_>) -> Result {
	let mut gw = fx
		.gateway("pk-botroom", device_id!("PHONE"), 0)
		.await?;
	let defaults = json!({ "v": 1, "burstWindowSec": 0, "botRoomLevel": "mentions" });
	fx.settings("com.chat-harness.notify.defaults", defaults)
		.await?;

	fx.expect(&mut gw, "$botroom-plain:x", fx.bot.as_str(), json!({ "body": "hi" }), false)
		.await?;

	// Once the bot is off the list, the room is not a bot room.
	fx.settings("com.chat-harness.notify.bots", json!({ "v": 1, "users": [] }))
		.await?;
	fx.expect(&mut gw, "$botroom-human:x", fx.bot.as_str(), json!({ "body": "hi" }), true)
		.await?;

	fx.settings("com.chat-harness.notify.defaults", json!({ "v": 1, "burstWindowSec": 0 }))
		.await
}

async fn pause_and_quiet_hours(fx: &Fixture<'_>) -> Result {
	let mut gw = fx
		.gateway("pk-schedule", device_id!("PHONE"), 0)
		.await?;
	let mention = json!({ "body": "hi", "m.mentions": { "user_ids": [fx.user] } });

	let paused = json!({
		"v": 1, "pauseUntil": now_ms().saturating_add(3_600_000),
		"breakThrough": { "users": [HUMAN] },
	});
	fx.settings("com.chat-harness.notify.schedule", paused)
		.await?;
	fx.expect(&mut gw, "$pause:x", HUMAN, mention.clone(), false)
		.await?;

	let quiet = json!({
		"v": 1, "timezone": "UTC",
		"quietHours": [{ "days": [0, 1, 2, 3, 4, 5, 6], "start": "00:00", "end": "00:00" }],
		"breakThrough": { "users": [HUMAN] },
	});
	fx.settings("com.chat-harness.notify.schedule", quiet)
		.await?;
	fx.expect(&mut gw, "$quiet-other:x", OTHER_HUMAN, mention.clone(), false)
		.await?;
	fx.expect(&mut gw, "$quiet-through:x", HUMAN, json!({ "body": "hi" }), true)
		.await?;

	fx.settings("com.chat-harness.notify.schedule", json!({}))
		.await
}

/// While the PC views the room, the phone's pusher stays quiet for it, the
/// PC's own pusher does not, and a clear lifts it.
async fn active_device_suppression(fx: &Fixture<'_>) -> Result {
	let mut phone = fx
		.gateway("pk-phone", device_id!("PHONE"), 0)
		.await?;
	let mut pc = fx.gateway("pk-pc", device_id!("PC"), 0).await?;

	let heartbeat = json!({ "room_id": fx.room, "seq": 10 });
	let (status, body) = fx.activity(Some(PC_TOKEN), &heartbeat).await?;
	if status != 200 || body != json!({ "ttl_ms": 60_000 }) {
		return Err!("heartbeat answered {status} {body}");
	}

	// Off by default: the heartbeat suppresses nothing.
	fx.expect(&mut phone, "$active-off:x", HUMAN, json!({ "body": "hi" }), true)
		.await?;

	let defaults = json!({ "v": 1, "burstWindowSec": 0, "activeDeviceSuppression": true });
	fx.settings("com.chat-harness.notify.defaults", defaults)
		.await?;

	let event = fx.message("$active-on:x", HUMAN, json!({ "body": "hi" }));
	fx.push(&phone, &event).await?;
	expect_nothing(&mut phone, "the phone while the PC views the room")?;
	fx.push(&pc, &event).await?;
	expect_delivered(&mut pc, "$active-on:x")?;

	let (status, _) = fx
		.activity(Some(PC_TOKEN), &json!({ "room_id": null, "seq": 10 }))
		.await?;
	if status != 200 {
		return Err!("clear answered {status}");
	}
	fx.expect(&mut phone, "$active-cleared:x", HUMAN, json!({ "body": "hi" }), true)
		.await?;

	fx.settings("com.chat-harness.notify.defaults", json!({ "v": 1, "burstWindowSec": 0 }))
		.await
}

async fn activity_endpoint_errors(fx: &Fixture<'_>) -> Result {
	let (status, body) = fx
		.activity(Some(PHONE_TOKEN), &json!({ "room_id": fx.room, "seq": "soon" }))
		.await?;
	if status != 400 || body["errcode"] != "M_BAD_JSON" {
		return Err!("a bad body answered {status} {body}");
	}

	let (status, _) = fx
		.activity(None, &json!({ "room_id": fx.room, "seq": 1 }))
		.await?;
	if status != 401 {
		return Err!("a request without a token answered {status}");
	}

	Ok(())
}

/// Pushes held back and flushed later pass the guard too.
async fn deferred_flush_is_guarded(fx: &Fixture<'_>) -> Result {
	let mut gw = fx
		.gateway("pk-flush", device_id!("PHONE"), 0)
		.await?;
	let pushkey = gw.pusher.ids.pushkey.clone();

	fx.services
		.db
		.get("userroomid_notificationcount")?
		.put((&fx.user, &fx.room), 1_u64);

	fx.room_settings(json!({ "level": "muted" }))
		.await?;
	let held = fx.persist("$flush-muted:x", 1)?;
	fx.services
		.pusher
		.queue_suppressed_push(&fx.user, &pushkey, &fx.room, held);
	fx.services
		.sending
		.flush_suppressed_for_user(fx.user.clone(), "guard test")
		.await;
	expect_nothing(&mut gw, "a flushed push in a muted room")?;

	fx.settings("com.chat-harness.notify.rooms", json!({}))
		.await?;
	let held = fx.persist("$flush-open:x", 2)?;
	fx.services
		.pusher
		.queue_suppressed_push(&fx.user, &pushkey, &fx.room, held);
	fx.services
		.sending
		.flush_suppressed_for_user(fx.user.clone(), "guard test")
		.await;

	expect_delivered(&mut gw, "$flush-open:x")
}

impl Fixture<'_> {
	fn message(&self, event_id: &str, sender: &str, content: Value) -> Pdu {
		let mut content = content;
		content["msgtype"] = json!("m.text");

		serde_json::from_value(json!({
			"type": "m.room.message",
			"content": content,
			"event_id": event_id,
			"room_id": self.room,
			"sender": sender,
			"prev_events": ["$prev:remote.example"],
			"auth_events": ["$auth:remote.example"],
			"origin_server_ts": 1_838_188_000,
			"depth": 12,
			"hashes": { "sha256": "thishashcoversallfieldsincasethisisredacted" },
		}))
		.expect("a valid test event")
	}

	/// Store a message so the deferred queue can load it by its PDU id.
	fn persist(&self, event_id: &str, count: u64) -> Result<RawPduId> {
		let pdu = self.message(event_id, HUMAN, json!({ "body": "held" }));
		let pdu_id: RawPduId = PduId {
			shortroomid: SHORT_ROOM_ID,
			count: PduCount::Normal(count),
		}
		.into();

		self.services
			.db
			.get("pduid_pdu")?
			.raw_put(pdu_id, Json(&pdu));

		Ok(pdu_id)
	}

	async fn push(&self, gw: &Gateway, pdu: &Pdu) -> Result {
		self.services
			.pusher
			.send_push_notice(&self.user, &gw.pusher, &self.ruleset, pdu)
			.await
	}

	/// Push one message and check whether it alerted.
	async fn expect(
		&self,
		gw: &mut Gateway,
		event_id: &str,
		sender: &str,
		content: Value,
		alerts: bool,
	) -> Result {
		self.push(gw, &self.message(event_id, sender, content))
			.await?;

		match alerts {
			| true => expect_delivered(gw, event_id),
			| false => expect_nothing(gw, event_id),
		}
	}

	/// Write one guard settings key as the PC would.
	async fn settings(&self, kind: &str, content: Value) -> Result {
		let url =
			format!("{}/_matrix/client/v3/user/{}/account_data/{kind}", self.base, self.user);

		http(self.services, "PUT", &url)
			.bearer_auth(PC_TOKEN)
			.json(&content)
			.send()
			.await?
			.error_for_status()?;

		Ok(())
	}

	async fn room_settings(&self, room: Value) -> Result {
		let mut rooms = serde_json::Map::new();
		rooms.insert(self.room.to_string(), room);

		self.settings("com.chat-harness.notify.rooms", json!({ "v": 1, "rooms": rooms }))
			.await
	}

	async fn activity(&self, token: Option<&str>, body: &Value) -> Result<(u16, Value)> {
		let url = format!("{}/_matrix/client/unstable/com.chat-harness/activity", self.base);
		let mut request = http(self.services, "POST", &url).json(body);
		if let Some(token) = token {
			request = request.bearer_auth(token);
		}

		let response = request.send().await?;
		let status = response.status().as_u16();
		let body = response
			.json::<Value>()
			.await
			.unwrap_or(Value::Null);

		Ok((status, body))
	}

	/// Register a pusher for `device` at a fresh stub gateway, which refuses
	/// its first `failures` requests.
	async fn gateway(
		&self,
		pushkey: &str,
		device: &DeviceId,
		failures: usize,
	) -> Result<Gateway> {
		let listener = TcpListener::bind("127.0.0.1:0").await?;
		let url = format!("http://{}/_matrix/push/v1/notify", listener.local_addr()?);

		let pusher: Pusher = PusherInit {
			ids: PusherIds::new(pushkey.to_owned(), APP_ID.to_owned()),
			kind: PusherKind::Http(HttpPusherData::new(url)),
			app_display_name: "Guard Test".into(),
			device_display_name: "Guard Device".into(),
			profile_tag: None,
			lang: "en".into(),
		}
		.into();

		self.services
			.pusher
			.set_pusher(&self.user, device, &SetPusherRequest::post(pusher).action)
			.await?;

		let pusher = self
			.services
			.pusher
			.get_pusher(&self.user, pushkey)
			.await?;

		let (tx, rx) = unbounded_channel();
		let stub = AbortOnDrop(spawn(stub_gateway(listener, tx, failures)));

		Ok(Gateway { pusher, rx, _stub: stub })
	}
}

fn expect_delivered(gw: &mut Gateway, event_id: &str) -> Result {
	let body = recv(&mut gw.rx, event_id)?;
	let body: Value = serde_json::from_slice(&body)
		.map_err(|e| err!("push body for {event_id} was not json: {e}"))?;

	let got = body["notification"]["event_id"].as_str();
	if got != Some(event_id) {
		return Err!("expected a push for {event_id}, got {got:?}");
	}

	expect_nothing(gw, event_id)
}

fn expect_nothing(gw: &mut Gateway, case: &str) -> Result {
	match gw.rx.try_recv() {
		| Err(TryRecvError::Empty) => Ok(()),
		| Err(TryRecvError::Disconnected) => Err!("stub gateway closed during {case}"),
		| Ok(body) => Err!("{case}: unexpected push {}", String::from_utf8_lossy(&body)),
	}
}

/// The send is awaited before this runs, so its request has already arrived.
fn recv(rx: &mut Capture, case: &str) -> Result<Vec<u8>> {
	rx.try_recv()
		.map_err(|_| err!("{case}: expected a push, the gateway got none"))
}

async fn stub_gateway(listener: TcpListener, tx: UnboundedSender<Vec<u8>>, failures: usize) {
	let mut failures = failures;

	while let Ok((mut socket, _)) = listener.accept().await {
		let Some(body) = read_request(&mut socket).await else {
			continue;
		};

		if tx.send(body).is_err() {
			return;
		}

		let (status, body) = match failures {
			| 0 => ("200 OK", r#"{"rejected":[]}"#),
			| _ => ("502 Bad Gateway", "{}"),
		};
		failures = failures.saturating_sub(1);

		let response = format!(
			"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: \
			 {}\r\nConnection: close\r\n\r\n{body}",
			body.len(),
		);

		socket.write_all(response.as_bytes()).await.ok();
		socket.flush().await.ok();
	}
}

async fn read_request(socket: &mut TcpStream) -> Option<Vec<u8>> {
	let mut buf = Vec::new();
	let mut chunk = [0_u8; 4096];
	loop {
		if let Some(head_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
			let length: usize = from_utf8(&buf[..head_end])
				.ok()?
				.lines()
				.find_map(|line| {
					line.split_once(':')
						.filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
				})
				.and_then(|(_, value)| value.trim().parse().ok())?;

			let start = head_end.checked_add(4)?;
			let end = start.checked_add(length)?;
			if buf.len() >= end {
				return Some(buf[start..end].to_vec());
			}
		}

		let read = socket.read(&mut chunk).await.ok()?;
		if read == 0 {
			return None;
		}

		buf.extend_from_slice(&chunk[..read]);
	}
}

fn http(services: &Services, method: &str, url: &str) -> reqwest::RequestBuilder {
	let client = &services.client.clients.default;
	match method {
		| "PUT" => client.put(url),
		| "POST" => client.post(url),
		| _ => client.get(url),
	}
}

fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
		.unwrap_or_default()
}

/// Wait for the listener to answer, which the boot does not itself await.
async fn wait_until_ready(services: &Services, base: &str) -> Result {
	let url = format!("{base}/_matrix/client/versions");

	timeout(Duration::from_secs(10), async {
		while http(services, "GET", &url).send().await.is_err() {
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| err!("server listener did not become ready"))
}

/// Register a local user with a device for each `(device, token)`.
async fn register(
	services: &Services,
	localpart: &str,
	devices: &[(&DeviceId, &str)],
) -> Result<OwnedUserId> {
	let user_id = UserId::parse_with_server_name(localpart, services.globals.server_name())?;

	services
		.users
		.full_register(Register {
			user_id: Some(&user_id),
			password: Some("notify-guard-password"),
			..Default::default()
		})
		.await?;

	for (device, token) in devices {
		services
			.users
			.create_device(&user_id, Some(device), (Some(token), None), None, None, None)
			.await?;
	}

	Ok(user_id)
}
