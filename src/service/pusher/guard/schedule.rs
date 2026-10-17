//! Quiet hours, pause and break-through (D7, D9):
//! `com.chat-harness.notify.schedule`.
//!
//! Windows are local wall-clock times in the schedule's IANA timezone, judged
//! at delivery time, so they hold across midnight and DST changes. Zone names
//! match in any case and are kept as written. A missing or unknown timezone
//! turns the quiet hours off and keeps the rest.

use std::collections::HashSet;

use chrono::{DateTime, Datelike, NaiveTime, Timelike, Utc, Weekday};
use chrono_tz::{TZ_VARIANTS, Tz};
use serde::{Deserialize, Deserializer, de::Error as _};

use super::settings::{MAX_BREAK_THROUGH, MAX_WINDOWS, ids, optional_time, time};

#[derive(Debug, Default, Deserialize)]
#[serde(from = "Stored")]
pub(super) struct Schedule {
	#[cfg_attr(
		not(test),
		expect(dead_code, reason = "read only by the fixture tests")
	)]
	pub(super) updated_at: u64,

	/// `None` when there are no quiet hours, or no usable timezone for them.
	pub(super) timezone: Option<Zone>,

	/// Empty unless `timezone` is set.
	pub(super) quiet_hours: Vec<Window>,

	/// Quiet hours were written but dropped for want of a usable timezone.
	pub(super) timezone_dropped: bool,

	/// Unix ms; a past value is ignored.
	pub(super) pause_until: Option<u64>,

	pub(super) break_through: BreakThrough,
}

/// The value as written; an unusable timezone only turns quiet hours off.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Stored {
	#[serde(default, deserialize_with = "time")]
	updated_at: u64,

	#[serde(default)]
	timezone: Option<String>,

	#[serde(default, deserialize_with = "windows")]
	quiet_hours: Vec<Window>,

	#[serde(default, deserialize_with = "optional_time")]
	pause_until: Option<u64>,

	#[serde(default)]
	break_through: BreakThrough,
}

/// A named IANA zone, matched in any case and kept as written.
#[derive(Debug)]
pub(super) struct Zone {
	#[cfg_attr(
		not(test),
		expect(dead_code, reason = "read only by the fixture tests")
	)]
	pub(super) name: String,
	pub(super) tz: Tz,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct BreakThrough {
	#[serde(
		default,
		deserialize_with = "ids::<_, MAX_BREAK_THROUGH, '!'>"
	)]
	pub(super) rooms: HashSet<String>,

	#[serde(
		default,
		deserialize_with = "ids::<_, MAX_BREAK_THROUGH, '@'>"
	)]
	pub(super) users: HashSet<String>,
}

/// One weekly window. It starts on each of `days` (0 = Sunday) at `start`
/// and ends at `end`; an `end` at or before `start` falls on the next day.
#[derive(Debug, Deserialize)]
pub(super) struct Window {
	#[serde(deserialize_with = "days")]
	pub(super) days: [bool; 7],

	/// Minutes after local midnight.
	#[serde(deserialize_with = "clock")]
	pub(super) start: u32,

	#[serde(deserialize_with = "clock")]
	pub(super) end: u32,
}

impl From<Stored> for Schedule {
	fn from(stored: Stored) -> Self {
		let timezone = stored
			.timezone
			.and_then(|name| Some(Zone { tz: zone(&name)?, name }))
			.filter(|_| !stored.quiet_hours.is_empty());

		let timezone_dropped = timezone.is_none() && !stored.quiet_hours.is_empty();

		Self {
			updated_at: stored.updated_at,
			quiet_hours: if timezone.is_some() {
				stored.quiet_hours
			} else {
				Vec::new()
			},
			timezone,
			timezone_dropped,
			pause_until: stored.pause_until,
			break_through: stored.break_through,
		}
	}
}

impl Schedule {
	pub(super) fn paused(&self, now_ms: u64) -> bool {
		self.pause_until
			.is_some_and(|until| until > now_ms)
	}

	/// Whether a quiet-hours window covers `now_ms`, in the local time of the
	/// schedule's timezone.
	pub(super) fn quiet(&self, now_ms: u64) -> bool {
		let Some(Zone { tz, .. }) = self.timezone else {
			return false;
		};

		let Some(now) = i64::try_from(now_ms)
			.ok()
			.and_then(DateTime::<Utc>::from_timestamp_millis)
		else {
			return false;
		};

		let local = now.with_timezone(&tz);
		let minute = local
			.hour()
			.saturating_mul(60)
			.saturating_add(local.minute());

		self.quiet_hours
			.iter()
			.any(|window| window.covers(local.weekday(), minute))
	}

	pub(super) fn breaks_through(&self, room_id: &str, sender: &str) -> bool {
		self.break_through.rooms.contains(room_id) || self.break_through.users.contains(sender)
	}
}

impl Window {
	fn starts_on(&self, day: Weekday) -> bool {
		usize::try_from(day.num_days_from_sunday())
			.ok()
			.and_then(|index| self.days.get(index))
			.copied()
			.unwrap_or(false)
	}

	/// Whether the local `day` and `minute` of the day fall in this window.
	fn covers(&self, day: Weekday, minute: u32) -> bool {
		let today = self.starts_on(day);
		let yesterday = self.starts_on(day.pred());

		if self.end > self.start {
			today && minute >= self.start && minute < self.end
		} else {
			(today && minute >= self.start) || (yesterday && minute < self.end)
		}
	}
}

/// An IANA zone or link name, in any case, as both teams' runtimes accept
/// it. Offsets such as `+01:00` are not names.
fn zone(name: &str) -> Option<Tz> {
	name.parse().ok().or_else(|| {
		TZ_VARIANTS
			.iter()
			.copied()
			.find(|tz| tz.name().eq_ignore_ascii_case(name))
	})
}

fn windows<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Window>, D::Error> {
	let windows = Vec::<Window>::deserialize(deserializer)?;
	if windows.len() > MAX_WINDOWS {
		return Err(D::Error::custom("too many quiet-hour windows"));
	}

	Ok(windows)
}

fn days<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[bool; 7], D::Error> {
	let listed = Vec::<u8>::deserialize(deserializer)?;
	if listed.is_empty() {
		return Err(D::Error::custom("a window needs a day"));
	}

	let mut days = [false; 7];
	for day in listed {
		let slot = days
			.get_mut(usize::from(day))
			.ok_or_else(|| D::Error::custom("day out of range"))?;

		*slot = true;
	}

	Ok(days)
}

/// `HH:MM`, 24-hour, as minutes after midnight.
fn clock<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
	let text = String::deserialize(deserializer)?;
	if text.len() != 5 {
		return Err(D::Error::custom("time is not HH:MM"));
	}

	let time = NaiveTime::parse_from_str(&text, "%H:%M")
		.map_err(|_| D::Error::custom("time is not HH:MM"))?;

	Ok(time
		.hour()
		.saturating_mul(60)
		.saturating_add(time.minute()))
}
