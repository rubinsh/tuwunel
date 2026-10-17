use std::collections::HashSet;

use chrono::{TimeZone, Utc};
use ruma::{device_id, event_id, room_id, user_id};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use super::{
	Content,
	activity::{ACTIVITY_TTL_MS, Activity},
	is_bot_sender, is_mentioned,
	memory::{BURST_CAP, Burst, DROP_CAP, DROP_TTL_MS, Memory},
	schedule::Schedule,
	settings::{
		BOTS_KEY, Bots, DEFAULTS_KEY, Defaults, Level, MAX_VALUE_BYTES, Parsed, ROOMS_KEY, Rooms,
		SCHEDULE_KEY, Settings, parse_content, parse_stored,
	},
};

/// Shared with the client team: every case must parse the same on both sides.
/// Copied from `docs/notify-guard/fixtures/parse-cases.json` at
/// Hepi420/matrix-bot-web-client@6ac74c4c52b82425ef8f546d9cc7baa362b26fbe.
const PARSE_CASES: &str = include_str!("fixtures/parse-cases.json");

fn parse<T: DeserializeOwned>(content: &Value) -> Parsed<T> {
	match content {
		| Value::Null => Parsed::Absent,
		| content => parse_content(content.clone()),
	}
}

fn level(level: Level) -> &'static str {
	match level {
		| Level::All => "all",
		| Level::Humans => "humans",
		| Level::Mentions => "mentions",
		| Level::Muted => "muted",
	}
}

fn sorted(ids: &HashSet<String>) -> Value {
	let mut ids: Vec<_> = ids.iter().cloned().collect();
	ids.sort();
	json!(ids)
}

fn clock(minutes: u32) -> String { format!("{:02}:{:02}", minutes / 60, minutes % 60) }

/// What the guard parsed, in the fixtures' shape.
fn normalized(key: &str, content: &Value) -> Value {
	match key {
		| DEFAULTS_KEY => match parse::<Defaults>(content).into_option() {
			| None => json!({
				"v": 1, "updatedAt": 0, "accountLevel": "all", "botRoomLevel": null,
				"burstWindowSec": 0, "activeDeviceSuppression": false,
			}),
			| Some(d) => json!({
				"v": 1, "updatedAt": d.updated_at, "accountLevel": level(d.account_level),
				"botRoomLevel": d.bot_room_level.map(level),
				"burstWindowSec": d.burst_window_sec,
				"activeDeviceSuppression": d.active_device_suppression,
			}),
		},
		| ROOMS_KEY => {
			let rooms: Rooms = parse(content).into_option().unwrap_or_default();
			let entries: Map<String, Value> = rooms
				.rooms
				.iter()
				.map(|(id, room)| {
					(
						id.clone(),
						json!({
							"level": room.level.map(level),
							"snoozeUntil": room.snooze_until,
							"muteAllThreads": room.mute_all_threads,
							"mutedThreads": sorted(&room.muted_threads),
						}),
					)
				})
				.collect();

			json!({ "v": 1, "updatedAt": rooms.updated_at, "rooms": entries })
		},
		| SCHEDULE_KEY => {
			let schedule: Schedule = parse(content).into_option().unwrap_or_default();
			let windows: Vec<Value> = schedule
				.quiet_hours
				.iter()
				.map(|window| {
					let days: Vec<usize> = (0..7).filter(|&day| window.days[day]).collect();
					json!({ "days": days, "start": clock(window.start), "end": clock(window.end) })
				})
				.collect();

			json!({
				"v": 1, "updatedAt": schedule.updated_at,
				"timezone": schedule.timezone.as_ref().map(|zone| zone.name.as_str()),
				"quietHours": windows, "pauseUntil": schedule.pause_until,
				"breakThrough": {
					"rooms": sorted(&schedule.break_through.rooms),
					"users": sorted(&schedule.break_through.users),
				},
			})
		},
		| BOTS_KEY => {
			let bots: Bots = parse(content).into_option().unwrap_or_default();
			json!({ "v": 1, "updatedAt": bots.updated_at, "users": sorted(&bots.users) })
		},
		| key => panic!("fixture names an unknown key {key}"),
	}
}

/// The fixture's expectation, with the fields a client may leave out filled in
/// and lists sorted, so it compares with [`normalized`].
fn expected(key: &str, expected: &Value) -> Value {
	let mut expected = expected.clone();
	let ids = |value: Option<&Value>| {
		let mut ids: Vec<String> = value
			.and_then(Value::as_array)
			.map(|ids| {
				ids.iter()
					.filter_map(Value::as_str)
					.map(ToOwned::to_owned)
					.collect()
			})
			.unwrap_or_default();
		ids.sort();
		json!(ids)
	};

	match key {
		| ROOMS_KEY =>
			for room in expected["rooms"]
				.as_object_mut()
				.expect("rooms is an object")
				.values_mut()
			{
				*room = json!({
					"level": room.get("level").cloned().unwrap_or(Value::Null),
					"snoozeUntil": room.get("snoozeUntil").cloned().unwrap_or(Value::Null),
					"muteAllThreads": room.get("muteAllThreads").cloned().unwrap_or(json!(false)),
					"mutedThreads": ids(room.get("mutedThreads")),
				});
			},
		| SCHEDULE_KEY => {
			let through = &mut expected["breakThrough"];
			*through = json!({
				"rooms": ids(through.get("rooms")),
				"users": ids(through.get("users")),
			});
		},
		| BOTS_KEY => expected["users"] = ids(expected.get("users")),
		| _ => {},
	}

	expected
}

#[test]
fn shared_parse_cases() {
	let fixtures: Value = serde_json::from_str(PARSE_CASES).expect("fixtures are json");
	let cases = fixtures["cases"]
		.as_array()
		.expect("cases is a list");

	assert!(cases.len() >= 35, "fixture file lost cases");
	for case in cases {
		let name = case["name"].as_str().expect("case has a name");
		let key = case["key"].as_str().expect("case has a key");

		assert_eq!(
			normalized(key, &case["content"]),
			expected(key, &case["expected"]),
			"case {name:?}",
		);
	}
}

#[test]
fn stored_events_wrap_the_value_in_content() {
	let stored = |content: Value| {
		serde_json::to_vec(&json!({ "type": DEFAULTS_KEY, "content": content })).unwrap()
	};

	let read = parse_stored::<Defaults>(Some(&stored(json!({ "v": 1, "burstWindowSec": 30 }))));
	assert_eq!(read.into_option().map(|d| d.burst_window_sec), Some(30));

	// A deleted key is a tombstone with empty content: absent, not degraded.
	assert!(matches!(parse_stored::<Defaults>(Some(&stored(json!({})))), Parsed::Absent));
	assert!(matches!(parse_stored::<Defaults>(None), Parsed::Absent));
	assert_eq!(parse_stored::<Defaults>(Some(b"{")).degraded(), Some("not_json"));

	let huge = json!({ "v": 1, "pad": "x".repeat(MAX_VALUE_BYTES) });
	assert_eq!(parse_stored::<Defaults>(Some(&stored(huge))).degraded(), Some("too_large"));
}

#[test]
fn absent_defaults_add_no_burst_limit() {
	let absent = Settings::default();
	assert_eq!(absent.burst_window_ms(), None);
	assert!(!absent.active_device_suppression());
	assert_eq!(absent.account_level(), Level::All);

	let present = Settings {
		defaults: parse::<Defaults>(&json!({ "v": 1 })).into_option(),
		..Settings::default()
	};
	assert_eq!(present.burst_window_ms(), Some(120_000));

	let off = Settings {
		defaults: parse::<Defaults>(&json!({ "v": 1, "burstWindowSec": 0 })).into_option(),
		..Settings::default()
	};
	assert_eq!(off.burst_window_ms(), None);
}

fn schedule(content: &Value) -> Schedule {
	parse::<Schedule>(content)
		.into_option()
		.expect("schedule parses")
}

fn at(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> u64 {
	let ms = Utc
		.with_ymd_and_hms(year, month, day, hour, minute, 0)
		.unwrap()
		.timestamp_millis();

	u64::try_from(ms).unwrap()
}

/// Sunday to Thursday nights, 22:00 to 07:00 in Jerusalem. 2026-10-18 is a
/// Sunday, in summer time (UTC+3).
fn weeknights() -> Schedule {
	schedule(&json!({
		"v": 1, "timezone": "Asia/Jerusalem",
		"quietHours": [{ "days": [0, 1, 2, 3, 4], "start": "22:00", "end": "07:00" }],
	}))
}

#[test]
fn quiet_hours_cross_midnight_from_their_start_day() {
	let quiet = weeknights();

	assert!(!quiet.quiet(at(2026, 10, 18, 18, 59)), "Sunday 21:59");
	assert!(quiet.quiet(at(2026, 10, 18, 19, 0)), "Sunday 22:00");
	assert!(quiet.quiet(at(2026, 10, 19, 3, 59)), "Monday 06:59, Sunday's window");
	assert!(!quiet.quiet(at(2026, 10, 19, 4, 0)), "Monday 07:00");
	assert!(quiet.quiet(at(2026, 10, 23, 3, 0)), "Friday 06:00, Thursday's window");
	assert!(!quiet.quiet(at(2026, 10, 23, 19, 30)), "Friday 22:30");
	assert!(!quiet.quiet(at(2026, 10, 24, 3, 0)), "Saturday 06:00");
}

#[test]
fn quiet_hours_follow_the_local_clock_across_dst() {
	let quiet = weeknights();

	// 19:30 UTC is 22:30 in summer time but 21:30 once the clocks go back on
	// 2026-10-25 (UTC+2).
	assert!(quiet.quiet(at(2026, 10, 18, 19, 30)));
	assert!(!quiet.quiet(at(2026, 10, 25, 19, 30)));
	assert!(quiet.quiet(at(2026, 10, 25, 20, 0)));
}

#[test]
fn a_window_ending_at_its_start_lasts_a_day() {
	let quiet = schedule(&json!({
		"v": 1, "timezone": "UTC",
		"quietHours": [{ "days": [6], "start": "09:00", "end": "09:00" }],
	}));

	// 2026-10-17 is a Saturday.
	assert!(!quiet.quiet(at(2026, 10, 17, 8, 59)));
	assert!(quiet.quiet(at(2026, 10, 17, 9, 0)));
	assert!(quiet.quiet(at(2026, 10, 18, 8, 59)));
	assert!(!quiet.quiet(at(2026, 10, 18, 9, 0)));
}

#[test]
fn zone_names_match_in_any_case_and_keep_their_spelling() {
	for name in ["asia/jerusalem", "ASIA/JERUSALEM", "Asia/Jerusalem"] {
		let quiet = schedule(&json!({
			"v": 1, "timezone": name,
			"quietHours": [{ "days": [0], "start": "22:00", "end": "07:00" }],
		}));

		assert_eq!(
			quiet
				.timezone
				.as_ref()
				.map(|zone| zone.name.as_str()),
			Some(name)
		);
		assert!(quiet.quiet(at(2026, 10, 18, 19, 0)), "{name}: Sunday 22:00 in Jerusalem");
		assert!(!quiet.quiet(at(2026, 10, 18, 18, 59)), "{name}: Sunday 21:59");
	}

	// A link name resolves to its zone: 03:00 UTC is 23:00 in New York.
	let linked = schedule(&json!({
		"v": 1, "timezone": "us/eastern",
		"quietHours": [{ "days": [6], "start": "22:00", "end": "07:00" }],
	}));
	assert!(linked.quiet(at(2026, 10, 18, 3, 0)));

	for offset in ["+01:00", "-0500", "GMT+1:00"] {
		let schedule = schedule(&json!({
			"v": 1, "timezone": offset,
			"quietHours": [{ "days": [0], "start": "00:00", "end": "00:00" }],
		}));
		assert!(schedule.timezone_dropped, "{offset} is not a zone name");
	}
}

#[test]
fn an_unknown_timezone_turns_only_quiet_hours_off() {
	let schedule = schedule(&json!({
		"v": 1, "timezone": "Mars/Base", "pauseUntil": 2000,
		"quietHours": [{ "days": [0, 1, 2, 3, 4, 5, 6], "start": "00:00", "end": "00:00" }],
		"breakThrough": { "users": ["@roy:x"] },
	}));

	assert!(schedule.timezone_dropped);
	assert!(!schedule.quiet(at(2026, 10, 18, 12, 0)));
	assert!(schedule.paused(1999));
	assert!(!schedule.paused(2000));
	assert!(schedule.breaks_through("!any:x", "@roy:x"));
}

#[test]
fn a_drop_stays_final_for_a_day() {
	let memory = Memory::default();
	let user = user_id!("@u:x");
	let event = event_id!("$e");

	assert!(!memory.was_dropped(user, "pk", event, 0));
	memory.record_drop(user, "pk", event, 1000);
	assert!(memory.was_dropped(user, "pk", event, 1000 + DROP_TTL_MS - 1));
	assert!(!memory.was_dropped(user, "pk", event, 1000 + DROP_TTL_MS));
	assert!(!memory.was_dropped(user, "other", event, 1000), "per pusher");
	assert!(!memory.was_dropped(user_id!("@v:x"), "pk", event, 1000), "per user");

	memory.forget_pusher(user, "pk");
	assert!(!memory.was_dropped(user, "pk", event, 1000));
}

#[test]
fn the_drop_record_is_bounded() {
	let memory = Memory::default();
	let user = user_id!("@u:x");

	for n in 0..DROP_CAP.saturating_add(10) {
		let event = format!("$e{n}");
		memory.record_drop(user, "pk", event.as_str().try_into().unwrap(), 5);
	}

	let (map, order, _) = memory.sizes();
	assert_eq!((map, order), (DROP_CAP, DROP_CAP));
	assert!(
		!memory.was_dropped(user, "pk", "$e0".try_into().unwrap(), 5),
		"oldest went first"
	);

	// Expired records go as soon as anything new is recorded.
	memory.record_drop(user, "pk", event_id!("$late"), 5 + DROP_TTL_MS);
	assert_eq!(memory.sizes().0, 1);
}

#[test]
fn burst_lets_one_alert_through_per_window() {
	let memory = Memory::default();
	let (user, room) = (user_id!("@u:x"), room_id!("!r:x"));
	let take = |event: &str, now| {
		memory.take_burst(user, "pk", room, event.try_into().unwrap(), 1000, now)
	};

	assert_eq!(take("$a", 0), Burst::Taken);
	assert_eq!(take("$b", 1), Burst::Held, "a second event in the window");
	assert_eq!(take("$a", 2), Burst::Taken, "the holder passes its own slot");
	assert_eq!(take("$c", 1000), Burst::Taken, "the window is over");
	assert_eq!(
		memory.take_burst(user, "pk2", room, event_id!("$d"), 1000, 1001),
		Burst::Taken,
		"each pusher has its own slot",
	);
	assert_eq!(
		memory.take_burst(user, "pk", room_id!("!s:x"), event_id!("$d"), 1000, 1001),
		Burst::Taken,
		"each room has its own slot",
	);
}

#[test]
fn a_failed_send_releases_its_burst_slot() {
	let memory = Memory::default();
	let (user, room) = (user_id!("@u:x"), room_id!("!r:x"));

	assert_eq!(memory.take_burst(user, "pk", room, event_id!("$a"), 1000, 0), Burst::Taken);

	// Releasing for another event leaves the holder's slot alone.
	memory.release_burst(user, "pk", room, event_id!("$b"));
	assert_eq!(memory.take_burst(user, "pk", room, event_id!("$b"), 1000, 1), Burst::Held);

	memory.release_burst(user, "pk", room, event_id!("$a"));
	assert_eq!(memory.take_burst(user, "pk", room, event_id!("$b"), 1000, 2), Burst::Taken);
}

#[test]
fn burst_slots_are_bounded() {
	let memory = Memory::default();
	let user = user_id!("@u:x");

	for n in 0..BURST_CAP {
		let room = format!("!r{n}:x");
		let room = room.as_str().try_into().unwrap();
		memory.take_burst(user, "pk", room, event_id!("$e"), 10, 0);
	}
	assert_eq!(memory.sizes().2, BURST_CAP);

	memory.take_burst(user, "pk", room_id!("!new:x"), event_id!("$e"), 10, 10);
	assert_eq!(memory.sizes().2, 1, "expired slots were reaped");
}

#[test]
fn activity_quiets_only_the_accounts_other_devices() {
	let activity = Activity::default();
	let (user, room) = (user_id!("@u:x"), room_id!("!r:x"));

	activity.report(user, device_id!("PC"), Some(room), 10, 0);

	assert!(activity.viewed_elsewhere(user, room, device_id!("PHONE"), 1));
	assert!(!activity.viewed_elsewhere(user, room, device_id!("PC"), 1), "not itself");
	assert!(!activity.viewed_elsewhere(user, room_id!("!s:x"), device_id!("PHONE"), 1));
	assert!(!activity.viewed_elsewhere(user_id!("@v:x"), room, device_id!("PHONE"), 1));
	assert!(
		!activity.viewed_elsewhere(user, room, device_id!("PHONE"), ACTIVITY_TTL_MS),
		"the claim lapses on its own",
	);
}

#[test]
fn activity_keeps_the_newest_report() {
	let activity = Activity::default();
	let (user, pc, phone) = (user_id!("@u:x"), device_id!("PC"), device_id!("PHONE"));
	let (a, b) = (room_id!("!a:x"), room_id!("!b:x"));

	// Tab A, then tab B, then A again; B's report arrives last.
	activity.report(user, pc, Some(a), 1, 0);
	activity.report(user, pc, Some(a), 3, 0);
	activity.report(user, pc, Some(b), 2, 0);
	assert!(activity.viewed_elsewhere(user, a, phone, 1));
	assert!(!activity.viewed_elsewhere(user, b, phone, 1));

	// The same seq renews the claim.
	activity.report(user, pc, Some(a), 3, 50_000);
	assert!(activity.viewed_elsewhere(user, a, phone, 100_000));
}

#[test]
fn a_late_clear_never_ends_a_newer_claim() {
	let activity = Activity::default();
	let (user, pc, phone) = (user_id!("@u:x"), device_id!("PC"), device_id!("PHONE"));
	let (a, b) = (room_id!("!a:x"), room_id!("!b:x"));

	activity.report(user, pc, Some(a), 1, 0);
	activity.report(user, pc, Some(b), 2, 0);
	activity.report(user, pc, None, 1, 0);
	assert!(activity.viewed_elsewhere(user, b, phone, 1));

	// A clear ends its own claim, and a heartbeat it covered cannot restore it.
	activity.report(user, pc, None, 2, 0);
	assert!(!activity.viewed_elsewhere(user, b, phone, 1));
	activity.report(user, pc, Some(b), 2, 0);
	assert!(!activity.viewed_elsewhere(user, b, phone, 1));

	activity.report(user, pc, Some(b), 3, 0);
	assert!(activity.viewed_elsewhere(user, b, phone, 1));
	assert_eq!(activity.len(), 1);
}

fn content(content: Value) -> Content { serde_json::from_value(content).unwrap() }

#[test]
fn mentions_follow_m_mentions_then_the_body() {
	let user = user_id!("@roy:shai.matrix");

	assert!(is_mentioned(
		user,
		&content(json!({
			"body": "hi", "m.mentions": { "user_ids": ["@roy:shai.matrix"] },
		}))
	));

	// With m.mentions present, the body does not count.
	assert!(!is_mentioned(
		user,
		&content(json!({
			"body": "@roy:shai.matrix hi", "m.mentions": {},
		}))
	));
	assert!(!is_mentioned(
		user,
		&content(json!({
			"body": "@roy:shai.matrix hi", "m.mentions": null,
		}))
	));
	assert!(!is_mentioned(
		user,
		&content(json!({
			"body": "@room hi", "m.mentions": { "room": true },
		}))
	));

	// Without it, the full ID in the body counts, outside quoted lines.
	assert!(is_mentioned(user, &content(json!({ "body": "ping @roy:shai.matrix" }))));
	assert!(!is_mentioned(
		user,
		&content(json!({ "body": "> @roy:shai.matrix said\n\nok" }))
	));
	assert!(!is_mentioned(user, &content(json!({ "body": "roy, look" }))));
	assert!(!is_mentioned(user, &content(json!({}))));
}

#[test]
fn bots_are_listed_or_marked() {
	let settings = Settings {
		bots: parse::<Bots>(&json!({ "v": 1, "users": ["@ch-pi:x"] }))
			.into_option()
			.unwrap(),
		..Settings::default()
	};

	assert!(is_bot_sender(&settings, "@ch-pi:x", &content(json!({}))));
	assert!(is_bot_sender(
		&settings,
		"@other:x",
		&content(json!({
			"com.chat-harness.sender_kind": "bot",
		}))
	));
	assert!(!is_bot_sender(&settings, "@roy:x", &content(json!({ "body": "hi" }))));
}
