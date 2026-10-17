//! The guard's settings, read from the user's global account data.
//!
//! Four keys, each parsed on its own. A key that is missing, over its size
//! budget, of an unknown version, or with any invalid field yields that key's
//! defaults and leaves the other keys in force; the one exception is an
//! unusable timezone, which only turns quiet hours off. The shapes and limits
//! are frozen by the notification guard spec (§5.7) and pinned by fixtures
//! shared with the clients that write them, whose parser this one matches:
//! numbers are JavaScript numbers (`120.0` is `120`), and `null` is valid only
//! where the spec shows it.

use std::collections::{HashMap, HashSet};

use serde::{
	Deserialize, Deserializer,
	de::{DeserializeOwned, Error as _},
};
use serde_json::{Map, Value};

use super::schedule::Schedule;

pub(super) const DEFAULTS_KEY: &str = "com.chat-harness.notify.defaults";
pub(super) const ROOMS_KEY: &str = "com.chat-harness.notify.rooms";
pub(super) const SCHEDULE_KEY: &str = "com.chat-harness.notify.schedule";
pub(super) const BOTS_KEY: &str = "com.chat-harness.notify.bots";

/// The only schema version this guard reads.
const VERSION: f64 = 1.0;

/// A key's `content` may be this large, counted as the UTF-8 length of
/// `JSON.stringify(content)`; clients refuse to save more. Bounds the work
/// done per push.
pub(super) const MAX_CONTENT_BYTES: usize = 65_536;

/// Stored events larger than this are not even parsed. The server may store a
/// number longer than JavaScript writes it (`-0.0` for `0`), at most 2.5 times
/// the client's count, so no content within budget can reach this.
pub(super) const MAX_STORED_BYTES: usize = 256 * 1024;

/// When a present `defaults` key names no burst window.
const DEFAULT_BURST_WINDOW_SEC: u64 = 120;
const MAX_BURST_WINDOW_SEC: u64 = 86_400;

const MAX_ROOMS: usize = 1000;
const MAX_MUTED_THREADS: usize = 200;
pub(super) const MAX_WINDOWS: usize = 14;
pub(super) const MAX_BREAK_THROUGH: usize = 200;
const MAX_BOTS: usize = 500;

/// JavaScript's largest safe integer; clients write numbers as JS numbers.
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
const MAX_SAFE_FLOAT: f64 = 9_007_199_254_740_991.0;

/// A room's alert level (D2).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(super) enum Level {
	All,
	Humans,
	Mentions,
	Muted,
}

/// `com.chat-harness.notify.defaults`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Defaults {
	/// Validated, though no rule reads it.
	#[cfg_attr(
		not(test),
		expect(dead_code, reason = "read only by the fixture tests")
	)]
	#[serde(default, deserialize_with = "time")]
	pub(super) updated_at: u64,

	#[serde(default = "Defaults::account_level")]
	pub(super) account_level: Level,

	#[serde(default)]
	pub(super) bot_room_level: Option<Level>,

	/// Zero turns burst control off.
	#[serde(
		default = "Defaults::burst_window_sec",
		deserialize_with = "burst_window"
	)]
	pub(super) burst_window_sec: u64,

	#[serde(default)]
	pub(super) active_device_suppression: bool,
}

/// `com.chat-harness.notify.rooms`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Rooms {
	/// Validated, though no rule reads it.
	#[cfg_attr(
		not(test),
		expect(dead_code, reason = "read only by the fixture tests")
	)]
	#[serde(default, deserialize_with = "time")]
	pub(super) updated_at: u64,

	#[serde(default, deserialize_with = "rooms")]
	pub(super) rooms: HashMap<String, Room>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Room {
	/// Optional, but never `null`.
	#[serde(default, deserialize_with = "present_level")]
	pub(super) level: Option<Level>,

	/// Unix ms; a past value is ignored. Optional, but never `null`.
	#[serde(default, deserialize_with = "present_time")]
	pub(super) snooze_until: Option<u64>,

	#[serde(default)]
	pub(super) mute_all_threads: bool,

	#[serde(
		default,
		deserialize_with = "ids::<_, MAX_MUTED_THREADS, '$'>"
	)]
	pub(super) muted_threads: HashSet<String>,
}

/// `com.chat-harness.notify.bots`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Bots {
	/// Validated, though no rule reads it.
	#[cfg_attr(
		not(test),
		expect(dead_code, reason = "read only by the fixture tests")
	)]
	#[serde(default, deserialize_with = "time")]
	pub(super) updated_at: u64,

	#[serde(default, deserialize_with = "ids::<_, MAX_BOTS, '@'>")]
	pub(super) users: HashSet<String>,
}

/// Every key, each already reduced to its defaults where it could not be read.
#[derive(Debug, Default)]
pub(super) struct Settings {
	/// `None` while the key is absent or unreadable, which keeps delivery as
	/// it was before the guard: in particular no new burst limit.
	pub(super) defaults: Option<Defaults>,
	pub(super) rooms: Rooms,
	pub(super) schedule: Schedule,
	pub(super) bots: Bots,
}

/// The outcome of reading one key.
#[derive(Debug)]
pub(super) enum Parsed<T> {
	/// The account never wrote it, or deleted it.
	Absent,
	/// It was read as written.
	Read(T),
	/// It exists but cannot be used; the reason is a short code for the log.
	Degraded(&'static str),
}

impl Defaults {
	const fn account_level() -> Level { Level::All }

	const fn burst_window_sec() -> u64 { DEFAULT_BURST_WINDOW_SEC }
}

impl Settings {
	/// Burst window in ms, or `None` when burst control is off: set to zero,
	/// or never configured at all.
	pub(super) fn burst_window_ms(&self) -> Option<u64> {
		self.defaults
			.as_ref()
			.map(|defaults| defaults.burst_window_sec)
			.filter(|&sec| sec > 0)
			.map(|sec| sec.saturating_mul(1000))
	}

	pub(super) fn active_device_suppression(&self) -> bool {
		self.defaults
			.as_ref()
			.is_some_and(|defaults| defaults.active_device_suppression)
	}

	pub(super) fn account_level(&self) -> Level {
		self.defaults
			.as_ref()
			.map_or(Level::All, |defaults| defaults.account_level)
	}

	pub(super) fn bot_room_level(&self) -> Option<Level> {
		self.defaults
			.as_ref()
			.and_then(|defaults| defaults.bot_room_level)
	}

	pub(super) fn room(&self, room_id: &str) -> Option<&Room> { self.rooms.rooms.get(room_id) }

	pub(super) fn is_bot(&self, user_id: &str) -> bool { self.bots.users.contains(user_id) }
}

impl<T> Parsed<T> {
	pub(super) fn into_option(self) -> Option<T> {
		match self {
			| Self::Read(value) => Some(value),
			| Self::Absent | Self::Degraded(_) => None,
		}
	}

	pub(super) fn degraded(&self) -> Option<&'static str> {
		match self {
			| Self::Degraded(reason) => Some(reason),
			| Self::Absent | Self::Read(_) => None,
		}
	}
}

/// Parse one stored account-data event, which wraps the value in `content`.
///
/// `stored` is `None` when the account has no such key. A deleted key is
/// stored as a tombstone with empty content, and reads as absent.
pub(super) fn parse_stored<T: DeserializeOwned>(stored: Option<&[u8]>) -> Parsed<T> {
	let Some(stored) = stored else {
		return Parsed::Absent;
	};

	if stored.len() > MAX_STORED_BYTES {
		return Parsed::Degraded("too_large");
	}

	let Ok(mut event) = serde_json::from_slice::<Value>(stored) else {
		return Parsed::Degraded("not_json");
	};

	match event.get_mut("content").map(Value::take) {
		| Some(Value::Object(content)) if content.is_empty() => Parsed::Absent,
		| Some(content) => parse_content(content),
		| None => Parsed::Degraded("no_content"),
	}
}

/// Parse one account-data value as a client wrote it.
pub(super) fn parse_content<T: DeserializeOwned>(content: Value) -> Parsed<T> {
	let Value::Object(content) = content else {
		return Parsed::Degraded("not_object");
	};

	if version(&content) != Some(VERSION) {
		return Parsed::Degraded("unknown_version");
	}

	let content = Value::Object(content);
	if js_json_len(&content) > MAX_CONTENT_BYTES {
		return Parsed::Degraded("too_large");
	}

	match T::deserialize(content) {
		| Ok(value) => Parsed::Read(value),
		| Err(_) => Parsed::Degraded("bad_value"),
	}
}

fn version(content: &Map<String, Value>) -> Option<f64> { content.get("v")?.as_f64() }

/// A non-negative safe integer, as a JavaScript number carries it: `120` and
/// `120.0` alike.
pub(super) fn safe_integer<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
	let number = serde_json::Number::deserialize(deserializer)?;

	number
		.as_u64()
		.or_else(|| integral(number.as_f64()?))
		.filter(|&integer| integer <= MAX_SAFE_INTEGER)
		.ok_or_else(|| D::Error::custom("not a non-negative safe integer"))
}

#[expect(
	clippy::as_conversions,
	clippy::cast_possible_truncation,
	clippy::cast_sign_loss,
	reason = "checked to be a whole number in 0..=2^53-1, which u64 holds exactly"
)]
fn integral(float: f64) -> Option<u64> {
	((0.0..=MAX_SAFE_FLOAT).contains(&float) && float.fract() == 0.0).then_some(float as u64)
}

/// Unix ms.
pub(super) fn time<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
	safe_integer(deserializer)
}

/// Unix ms or `null`.
pub(super) fn optional_time<'de, D: Deserializer<'de>>(
	deserializer: D,
) -> Result<Option<u64>, D::Error> {
	Option::<Value>::deserialize(deserializer)?
		.map(|value| time(value).map_err(D::Error::custom))
		.transpose()
}

/// Unix ms where the field may be left out but not set to `null`.
fn present_time<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
	time(deserializer).map(Some)
}

fn present_level<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Level>, D::Error> {
	Level::deserialize(deserializer).map(Some)
}

fn burst_window<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
	let sec = safe_integer(deserializer)?;
	if sec > MAX_BURST_WINDOW_SEC {
		return Err(D::Error::custom("burst window too long"));
	}

	Ok(sec)
}

fn rooms<'de, D: Deserializer<'de>>(deserializer: D) -> Result<HashMap<String, Room>, D::Error> {
	let rooms = HashMap::<String, Room>::deserialize(deserializer)?;
	if rooms.len() > MAX_ROOMS {
		return Err(D::Error::custom("too many rooms"));
	}

	if !rooms.keys().all(|room_id| is_id(room_id, '!')) {
		return Err(D::Error::custom("not a room id"));
	}

	Ok(rooms)
}

/// A list of at most `CAP` Matrix IDs, each starting with `SIGIL`.
pub(super) fn ids<'de, D, const CAP: usize, const SIGIL: char>(
	deserializer: D,
) -> Result<HashSet<String>, D::Error>
where
	D: Deserializer<'de>,
{
	let ids = Vec::<String>::deserialize(deserializer)?;
	if ids.len() > CAP {
		return Err(D::Error::custom("too many ids"));
	}

	if !ids.iter().all(|id| is_id(id, SIGIL)) {
		return Err(D::Error::custom("not a matrix id"));
	}

	Ok(ids.into_iter().collect())
}

/// The clients check only the sigil, so the guard does too.
fn is_id(id: &str, sigil: char) -> bool { id.starts_with(sigil) }

/// The UTF-8 length of `JSON.stringify(value)`: how clients measure a key
/// against its budget. Counted here rather than by reserializing, since
/// serde_json writes some numbers differently (`1.0` where JavaScript writes
/// `1`).
pub(super) fn js_json_len(value: &Value) -> usize {
	let joined = |lengths: &mut dyn Iterator<Item = usize>, count: usize| {
		lengths
			.fold(2_usize, usize::saturating_add)
			.saturating_add(count.saturating_sub(1))
	};

	match value {
		| Value::Null | Value::Bool(true) => 4,
		| Value::Bool(false) => 5,
		| Value::Number(number) => js_number_len(number.as_f64().unwrap_or_default()),
		| Value::String(string) => js_string_len(string),
		| Value::Array(items) => joined(&mut items.iter().map(js_json_len), items.len()),
		| Value::Object(map) => joined(
			&mut map.iter().map(|(key, value)| {
				js_string_len(key)
					.saturating_add(1)
					.saturating_add(js_json_len(value))
			}),
			map.len(),
		),
	}
}

/// A quoted string as `JSON.stringify` escapes it.
fn js_string_len(string: &str) -> usize {
	string
		.chars()
		.map(|char| match char {
			| '"' | '\\' | '\u{8}' | '\t' | '\n' | '\u{c}' | '\r' => 2,
			| char if u32::from(char) < 0x20 => 6,
			| char => char.len_utf8(),
		})
		.fold(2, usize::saturating_add)
}

/// A number as JavaScript writes it: `1` for `1.0`, `1e+21`, `0.000001`.
fn js_number_len(float: f64) -> usize { ryu_js::Buffer::new().format(float).len() }
