#![cfg(test)]

//! Pure-helper tests for the matrix-channel ephemeral ring queue.
//!
//! The `Service` itself depends on `services.globals.next_count()`,
//! which requires a full Services builder. We instead test the
//! eviction + filter helpers in isolation against a hand-built
//! `VecDeque<EphemeralEntry>`. This covers the correctness blockers
//! pi flagged on the v1 design (PR #210 review):
//!   - per-sender isolation across two senders;
//!   - fast-burst preservation;
//!   - ring eviction when a room outpaces `MAX_ENTRIES_PER_ROOM`;
//!   - `since`-counter semantics.

use std::collections::VecDeque;

use ruma::{OwnedRoomId, OwnedUserId, serde::Raw};
use serde_json::json;

use super::{
	EphemeralEntry, MAX_ENTRIES_PER_ROOM, entries_window_in, push_with_eviction, render_entry,
};

const UNCAPPED: u64 = u64::MAX;

fn user(name: &str) -> OwnedUserId { format!("@{name}:test.srv").parse().unwrap() }

fn room(name: &str) -> OwnedRoomId { format!("!{name}:test.srv").parse().unwrap() }

fn entry(counter: u64, kind: &str, sender: &str, body: serde_json::Value) -> EphemeralEntry {
	EphemeralEntry {
		counter,
		event_type: kind.to_owned(),
		sender: user(sender),
		origin_server_ts: 1_700_000_000_000 + counter,
		content: serde_json::value::to_raw_value(&body).unwrap(),
	}
}

// Production counters from `services.globals.next_count()` start
// at 1 and increase monotonically, so the tests use 1-indexed values
// to match. The filter is `counter > since`, so `since=0` matches
// every real entry — that's the "client has never seen any events"
// case.

#[test]
fn push_preserves_order_within_capacity() {
	let mut ring: VecDeque<EphemeralEntry> = VecDeque::new();
	for i in 1..=5_u64 {
		push_with_eviction(&mut ring, entry(i, "com.example.t", "alice", json!({ "i": i })));
	}
	assert_eq!(ring.len(), 5);
	let counters: Vec<u64> = ring.iter().map(|e| e.counter).collect();
	assert_eq!(counters, (1..=5).collect::<Vec<_>>());
}

#[test]
fn push_evicts_oldest_past_capacity() {
	let mut ring: VecDeque<EphemeralEntry> = VecDeque::new();
	for i in 1..=(MAX_ENTRIES_PER_ROOM as u64 + 50) {
		push_with_eviction(&mut ring, entry(i, "com.example.t", "alice", json!({ "i": i })));
	}
	assert_eq!(ring.len(), MAX_ENTRIES_PER_ROOM);
	let first = ring.front().unwrap();
	let last = ring.back().unwrap();
	assert_eq!(first.counter, 51);
	assert_eq!(last.counter, MAX_ENTRIES_PER_ROOM as u64 + 50);
}

#[test]
fn since_counter_filters_strictly_greater() {
	let mut ring: VecDeque<EphemeralEntry> = VecDeque::new();
	for i in 1..=10_u64 {
		push_with_eviction(&mut ring, entry(i, "com.example.t", "alice", json!({ "i": i })));
	}
	let after_4 = entries_window_in(&ring, 4, UNCAPPED);
	let counters: Vec<u64> = after_4.iter().map(|e| e.counter).collect();
	assert_eq!(counters, vec![5, 6, 7, 8, 9, 10]);

	// since=0: a fresh consumer that's never synced yet sees the full
	// history. since=last_counter: caught up, nothing new.
	assert_eq!(entries_window_in(&ring, 0, UNCAPPED).len(), 10);
	assert_eq!(entries_window_in(&ring, 10, UNCAPPED).len(), 0);
}

#[test]
fn next_batch_caps_emission_to_avoid_double_delivery() {
	// Regression for pi's v2 review: /sync must not emit entries
	// newer than the response's advertised next_batch. Otherwise the
	// next /sync (which uses next_batch as `since`) would re-emit the
	// same entry. Window is `since < counter <= next_batch`.
	let mut ring: VecDeque<EphemeralEntry> = VecDeque::new();
	for i in 1..=10_u64 {
		push_with_eviction(&mut ring, entry(i, "com.example.t", "alice", json!({ "i": i })));
	}

	// /sync sampled next_batch=7 then a producer raced with three
	// more PUTs (counters 8, 9, 10 land before the room is rendered).
	let observed = entries_window_in(&ring, 4, 7);
	let counters: Vec<u64> = observed.iter().map(|e| e.counter).collect();
	assert_eq!(counters, vec![5, 6, 7]);

	// The next /sync passes since=7 (the previous next_batch). It
	// must still see counters 8, 9, 10 — none of them have been
	// emitted yet.
	let next_round = entries_window_in(&ring, 7, 10);
	let counters: Vec<u64> = next_round.iter().map(|e| e.counter).collect();
	assert_eq!(counters, vec![8, 9, 10]);
}

#[test]
fn fast_burst_is_observed_when_consumer_polls_late() {
	// Producer writes Thinking -> Bash -> Thinking -> Read -> Thinking -> null
	// (the exact PR #210 burst pattern), all between two consumer polls.
	// Last poll's `since` was at counter=0; next poll must see all 6.
	let mut ring: VecDeque<EphemeralEntry> = VecDeque::new();
	let burst = [
		(1, "Thinking"),
		(2, "Bash"),
		(3, "Thinking"),
		(4, "Read"),
		(5, "Thinking"),
		(6, "null"),
	];
	for (i, label) in burst {
		push_with_eviction(
			&mut ring,
			entry(
				i,
				"com.shai.matrix-channel.bot_activity",
				"matrix-bot",
				json!({ "working": label }),
			),
		);
	}
	let observed = entries_window_in(&ring, 0, UNCAPPED);
	assert_eq!(observed.len(), 6);
	let labels: Vec<String> = observed
		.iter()
		.map(|e| {
			serde_json::from_str::<serde_json::Value>(e.content.get())
				.unwrap()
				.get("working")
				.unwrap()
				.as_str()
				.unwrap()
				.to_string()
		})
		.collect();
	assert_eq!(labels, vec!["Thinking", "Bash", "Thinking", "Read", "Thinking", "null"]);
}

#[test]
fn two_senders_in_one_room_do_not_overwrite() {
	// matrix-bot and matrix-channel-dev both writing in the same dev room.
	// Each must be visible as a distinct entry; nothing is overwritten.
	let mut ring: VecDeque<EphemeralEntry> = VecDeque::new();
	push_with_eviction(
		&mut ring,
		entry(
			1,
			"com.shai.matrix-channel.bot_activity",
			"matrix-bot",
			json!({ "working": "Bash" }),
		),
	);
	push_with_eviction(
		&mut ring,
		entry(
			2,
			"com.shai.matrix-channel.bot_activity",
			"matrix-channel-dev",
			json!({ "working": "Edit" }),
		),
	);
	push_with_eviction(
		&mut ring,
		entry(
			3,
			"com.shai.matrix-channel.bot_activity",
			"matrix-bot",
			json!({ "working": "Read" }),
		),
	);

	let observed = entries_window_in(&ring, 0, UNCAPPED);
	assert_eq!(observed.len(), 3);
	let pairs: Vec<(String, String)> = observed
		.iter()
		.map(|e| {
			let body: serde_json::Value = serde_json::from_str(e.content.get()).unwrap();
			(e.sender.localpart().to_string(), body["working"].as_str().unwrap().to_string())
		})
		.collect();
	assert_eq!(pairs, vec![
		("matrix-bot".to_string(), "Bash".to_string()),
		("matrix-channel-dev".to_string(), "Edit".to_string()),
		("matrix-bot".to_string(), "Read".to_string()),
	],);
}

#[test]
fn render_entry_emits_top_level_sender_room_id_and_origin_ts() {
	// Pi confirmed the wire shape the web-client's
	// `readActivityFromEvent()` expects: top-level `sender`, `room_id`
	// and `origin_server_ts` so `event.getSender()` / `event.getRoomId()`
	// / `event.getTs()` resolve. matrix-js-sdk's
	// `mapSyncEventsFormat(joinObj.ephemeral)` doesn't stamp the parent
	// roomId onto custom ephemeral events, so the server has to put it
	// in the event JSON itself.
	let entry = entry(
		42,
		"com.shai.matrix-channel.bot_activity",
		"matrix-bot",
		json!({ "working": "Bash", "awaiting_permission": null }),
	);
	let room_id = room("dev");
	let raw: Raw<serde_json::Value> = render_entry(&entry, &room_id).unwrap();
	let parsed: serde_json::Value = serde_json::from_str(raw.json().get()).unwrap();

	assert_eq!(parsed["type"], "com.shai.matrix-channel.bot_activity");
	assert_eq!(parsed["sender"], "@matrix-bot:test.srv");
	assert_eq!(parsed["room_id"], "!dev:test.srv");
	assert_eq!(parsed["origin_server_ts"], 1_700_000_000_042_u64);
	assert_eq!(parsed["content"]["working"], "Bash");
	assert!(parsed["content"]["awaiting_permission"].is_null());
}

#[test]
fn render_entry_room_id_tracks_caller() {
	// Two renders of the same entry into different rooms must surface
	// the caller's roomId, not anything cached on the entry itself.
	let entry = entry(1, "com.example.t", "alice", json!({"x": 1}));
	let room_a = room("a");
	let room_b = room("b");
	let parsed_a: serde_json::Value = serde_json::from_str(
		render_entry::<serde_json::Value>(&entry, &room_a)
			.unwrap()
			.json()
			.get(),
	)
	.unwrap();
	let parsed_b: serde_json::Value = serde_json::from_str(
		render_entry::<serde_json::Value>(&entry, &room_b)
			.unwrap()
			.json()
			.get(),
	)
	.unwrap();
	assert_eq!(parsed_a["room_id"], "!a:test.srv");
	assert_eq!(parsed_b["room_id"], "!b:test.srv");
}

#[test]
fn render_entry_round_trips_arbitrary_content() {
	// Content is held as RawJsonValue to avoid re-parse cost; verify
	// nothing is mangled in the round-trip.
	let body = json!({
		"deeply": { "nested": [1, 2, { "x": "y" }] },
		"unicode": "🚀",
		"empty_obj": {},
	});
	let entry = entry(7, "com.example.t", "alice", body.clone());
	let raw: Raw<serde_json::Value> = render_entry(&entry, &room("any")).unwrap();
	let parsed: serde_json::Value = serde_json::from_str(raw.json().get()).unwrap();
	assert_eq!(parsed["content"], body);
}
