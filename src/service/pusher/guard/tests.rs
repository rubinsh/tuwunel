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
		BOTS_KEY, Bots, DEFAULTS_KEY, Defaults, Level, MAX_CONTENT_BYTES, MAX_STORED_BYTES,
		Parsed, ROOMS_KEY, Rooms, SCHEDULE_KEY, Settings, js_json_len, parse_content,
		parse_stored,
	},
};

/// Shared with the client team: every case must parse the same on both sides.
/// Copied from `docs/notify-guard/fixtures/parse-cases.json` at
/// Hepi420/matrix-bot-web-client@125431a7856d5efba9f241369c2beac9e8db81da.
const PARSE_CASES: &str = include_str!("fixtures/parse-cases.json");

fn parse<T: DeserializeOwned>(content: &Value) -> Parsed<T> {
	match content {
		| Value::Null => Parsed::Absent,
		| content => parse_content(content.clone()),
	}
}

/// Parse as the guard does: from the stored account-data event.
fn parse_as_stored<T: DeserializeOwned>(key: &str, content: &Value) -> Parsed<T> {
	let stored = (!content.is_null()).then(|| {
		serde_json::to_vec(&json!({ "type": key, "content": content })).expect("serializes")
	});

	parse_stored(stored.as_deref())
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
		| DEFAULTS_KEY => match parse_as_stored::<Defaults>(key, content).into_option() {
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
			let rooms: Rooms = parse_as_stored(key, content)
				.into_option()
				.unwrap_or_default();
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
			let schedule: Schedule = parse_as_stored(key, content)
				.into_option()
				.unwrap_or_default();
			let windows: Vec<Value> = schedule
				.quiet_hours
				.iter()
				.map(|window| {
					let days: Vec<f64> = (0_u8..7)
						.filter(|&day| window.days[usize::from(day)])
						.map(f64::from)
						.collect();
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
			let bots: Bots = parse_as_stored(key, content)
				.into_option()
				.unwrap_or_default();
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

			// The guard keeps a window's days as a set: a repeated day changes
			// nothing it decides.
			for window in expected["quietHours"]
				.as_array_mut()
				.into_iter()
				.flatten()
			{
				let mut days: Vec<f64> = window["days"]
					.as_array()
					.into_iter()
					.flatten()
					.filter_map(Value::as_f64)
					.collect();
				days.sort_by(f64::total_cmp);
				days.dedup();
				window["days"] = json!(days);
			}
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

	assert!(cases.len() >= 52, "fixture file lost cases");
	assert_eq!(fixtures["byteBudget"]["limit"], json!(MAX_CONTENT_BYTES));
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

	let huge = vec![b' '; MAX_STORED_BYTES + 1];
	assert_eq!(parse_stored::<Defaults>(Some(&huge)).degraded(), Some("too_large"));
}

/// `JSON.stringify(value)` and its UTF-8 length, as Node writes them.
const JS_LENGTHS: &[(&str, usize)] = &[
	("0", 1),
	("-0.0", 1),
	("1.0", 1),
	("1.5", 3),
	("120.0", 3),
	("-7", 2),
	("1e21", 5),
	("1E-7", 4),
	("0.000001", 8),
	("123456789012345678901", 21),
	("9007199254740993", 16),
	("1.2345e-5", 11),
	("5e-324", 6),
	("1.7976931348623157e308", 23),
	("0.1", 3),
	("1e2", 3),
	("1e20", 21),
	(r#""é""#, 4),
	(r#""😀""#, 6),
	(r#""\u0001""#, 8),
	(r#""\n""#, 4),
	(r#""\"""#, 4),
	(r#""\\""#, 4),
	(r#""\u2028""#, 5),
	(r#""\u007f""#, 3),
	(r#""a\tb""#, 6),
	("[]", 2),
	("{}", 2),
	(r#"[1,"x",null,true,false]"#, 23),
	(r#"{"a":1.0,"b":[1.5,"é"]}"#, 22),
];

#[test]
fn content_is_measured_as_json_stringify_writes_it() {
	for &(json, want) in JS_LENGTHS {
		let value: Value = serde_json::from_str(json).expect("vector is json");
		assert_eq!(js_json_len(&value), want, "JSON.stringify({json})");
	}
}

/// A `defaults` value padded with an ignored field to `size` bytes as a
/// client counts them, with `v` written as `1.0`, which serde_json would
/// count two bytes longer.
fn padded_defaults(size: usize) -> Value {
	let mut value = json!({ "v": 1.0, "pad": "" });
	let pad = size
		.checked_sub(js_json_len(&value))
		.expect("room to pad");
	let mut pad_text = "é".repeat(pad / 2);
	pad_text.push_str(&"x".repeat(pad % 2));
	value["pad"] = json!(pad_text);
	assert_eq!(js_json_len(&value), size);
	value
}

#[test]
fn the_budget_is_counted_as_clients_count_it() {
	let stored = |content: Value| {
		serde_json::to_vec(&json!({ "type": DEFAULTS_KEY, "content": content })).unwrap()
	};

	let at_budget = stored(padded_defaults(MAX_CONTENT_BYTES));
	assert!(matches!(parse_stored::<Defaults>(Some(&at_budget)), Parsed::Read(_)));

	let over = stored(padded_defaults(MAX_CONTENT_BYTES + 1));
	assert_eq!(parse_stored::<Defaults>(Some(&over)).degraded(), Some("too_large"));

	// The largest stored form of content within budget stays under the outer
	// limit: every number written `-0.0` where JavaScript writes `0`.
	let zeros: Vec<Value> = vec![json!(-0.0); (MAX_CONTENT_BYTES - 20) / 2];
	let content = json!({ "v": 1, "pad": zeros });
	assert!(js_json_len(&content) <= MAX_CONTENT_BYTES);
	let stored = stored(content);
	assert!(stored.len() < MAX_STORED_BYTES, "stored {} bytes", stored.len());
	assert!(matches!(parse_stored::<Defaults>(Some(&stored)), Parsed::Read(_)));
}

#[test]
fn numbers_and_nulls_follow_the_client_parser() {
	let defaults = |content: Value| parse::<Defaults>(&content).into_option();
	let rooms = |content: Value| parse::<Rooms>(&content).into_option();
	let schedule = |content: Value| parse::<Schedule>(&content).into_option();

	// JavaScript numbers: a whole float is an integer.
	let read = defaults(json!({ "v": 1.0, "burstWindowSec": 120.0, "updatedAt": 5.0 }));
	assert_eq!(read.map(|d| d.burst_window_sec), Some(120));
	assert!(defaults(json!({ "v": 1, "burstWindowSec": 1.5 })).is_none());
	assert!(defaults(json!({ "v": 1, "updatedAt": 9_007_199_254_740_992_u64 })).is_none());
	assert!(defaults(json!({ "v": 1, "updatedAt": -1 })).is_none());

	// `null` only where the spec shows it.
	assert!(defaults(json!({ "v": 1, "botRoomLevel": null })).is_some());
	assert!(rooms(json!({ "v": 1, "rooms": { "!a:x": { "level": null } } })).is_none());
	assert!(rooms(json!({ "v": 1, "rooms": { "!a:x": { "snoozeUntil": null } } })).is_none());
	assert!(schedule(json!({ "v": 1, "pauseUntil": null })).is_some());

	// The clients check only an ID's sigil.
	let bare = rooms(json!({ "v": 1, "rooms": { "!": { "mutedThreads": ["$"] } } }));
	assert!(bare.is_some_and(|rooms| rooms.rooms.contains_key("!")));

	// One to seven days, repeats included; whole-number floats count.
	let window = |days: Value| {
		schedule(json!({
			"v": 1, "timezone": "UTC",
			"quietHours": [{ "days": days, "start": "22:00", "end": "07:00" }],
		}))
	};
	assert!(window(json!([1, 1, 1, 1, 1, 1, 1])).is_some());
	assert!(window(json!([1, 1, 1, 1, 1, 1, 1, 1])).is_none());
	assert!(window(json!([1.0])).is_some());

	// A valid timezone is kept even with no quiet hours.
	let zone = schedule(json!({ "v": 1, "timezone": "UTC" })).unwrap();
	assert_eq!(zone.timezone.map(|zone| zone.name), Some("UTC".to_owned()));
	assert!(!zone.timezone_dropped);

	// Clock times: exactly HH:MM, 00:00 to 23:59.
	let clock = |time: &str| {
		schedule(json!({
			"v": 1, "timezone": "UTC",
			"quietHours": [{ "days": [1], "start": time, "end": "07:00" }],
		}))
		.is_some()
	};
	assert!(clock("00:00") && clock("23:59") && clock("09:05"));
	assert!(!clock("24:00") && !clock("9:05") && !clock(" 9:05") && !clock("09:60"));
	assert!(!clock("09:5 ") && !clock("０9:05") && !clock("09-05"));
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
fn a_changed_burst_window_applies_at_once() {
	let memory = Memory::default();
	let (user, room) = (user_id!("@u:x"), room_id!("!r:x"));
	let take = |event: &str, window_sec: u64, now_sec: u64| {
		memory.take_burst(
			user,
			"pk",
			room,
			event.try_into().unwrap(),
			window_sec * 1000,
			now_sec * 1000,
		)
	};

	// Alert at 1 s under a 120 s window, then the window shrinks to 30 s.
	assert_eq!(take("$a", 120, 1), Burst::Taken);
	assert_eq!(take("$b", 30, 61), Burst::Taken, "60 s on, outside the new window");

	// Alert at 200 s under 120 s, then the window grows to 300 s.
	assert_eq!(take("$c", 120, 200), Burst::Taken);
	assert_eq!(take("$d", 300, 350), Burst::Held, "150 s on, inside the new window");
}

#[test]
fn burst_slots_are_bounded() {
	let fill = |memory: &Memory| {
		for n in 0..BURST_CAP {
			let room = format!("!r{n}:x");
			let room = room.as_str().try_into().unwrap();
			memory.take_burst(user_id!("@u:x"), "pk", room, event_id!("$e"), 10, 0);
		}
		assert_eq!(memory.sizes().2, BURST_CAP);
	};

	// Slots older than any window a client may set are reaped.
	let memory = Memory::default();
	fill(&memory);
	memory.take_burst(
		user_id!("@u:x"),
		"pk",
		room_id!("!new:x"),
		event_id!("$e"),
		10,
		86_400_000,
	);
	assert_eq!(memory.sizes().2, 1);

	// Slots that could still be inside a window are all released at the cap.
	let memory = Memory::default();
	fill(&memory);
	memory.take_burst(user_id!("@u:x"), "pk", room_id!("!new:x"), event_id!("$e"), 10, 10);
	assert_eq!(memory.sizes().2, 1);
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
