use std::{
	cmp::Ordering,
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

use futures::StreamExt;
use serde::Deserialize;
use tuwunel_core::{
	Result, err, implement, info,
	smallvec::SmallVec,
	utils::{
		BoolExt, ReadyExt,
		stream::{IterStream, TryIgnore},
	},
	warn,
};
use tuwunel_database::{Database, Get, Handle, Map, SEP};

use crate::{Services, rooms::pdu_metadata::typed_relations::Key as RelationKey};

/// Owned copy of a reverse-map identity, used to dereference a loser.
///
/// Sized for the common modern event id; longer identities spill.
type Identity = SmallVec<[u8; 48]>;

/// The `relatesto_typed` rows to rewrite, each with its stale child value.
///
/// The key is copied verbatim and rewritten at its own row; one wider than
/// the writer's fixed length cannot be a relation row and is skipped.
pub(super) type Relations = Vec<(RelationKey, u64)>;

/// Bitmap over the short id space, one bit per id up to the global counter.
///
/// Out-of-range bits are silently absent: setting one is a no-op and
/// testing one is false.
type Bits = Vec<u64>;

/// One short id paired with the identity its row names.
type Candidate = (u64, Identity);

/// Reverse rows no forward value claims, paired with the identities they
/// name.
type Candidates = Vec<Candidate>;

/// One family's resolution: losers, the winner each maps to, the rows a
/// promotion can heal, and the count the dereference could not settle.
///
/// Only a row proved absent is promotable; an unsettled one may still hold
/// a live forward row, so it refuses instead.
type Resolution = (Vec<u64>, BTreeMap<u64, u64>, Candidates, u64);

/// What dereferencing one candidate's identity proved.
///
/// Absent is the only outcome a promotion may act on. A failed read and a
/// forward row whose value is not a short id both leave the row unsettled,
/// which is not the same as proving nothing is there.
enum Resolved {
	Winner(u64),
	Absent,
	Unsettled,
}

/// Exclusive upper bound on verifiable short ids.
///
/// Each scan bitmap costs one bit per id up to the global counter, and the
/// deep sweep holds up to five at once. At or above this bound the scan
/// reports unverifiable instead.
const MAX_SHORT: u64 = 1 << 30;

/// One family's residue: its losers, their winners, the rows a heal pass
/// completes, and the counts that impugn the scan.
///
/// The losers include every row the dereference could not pair, so
/// `winners` is total exactly when `promotable` is empty and `unresolved`
/// is zero. A contended slot leaves both its claimants unhealed, since
/// nothing in the residue names which one the allocator meant.
#[derive(Default)]
pub(super) struct Family {
	pub(super) rows: u64,
	pub(super) losers: Vec<u64>,
	pub(super) winners: BTreeMap<u64, u64>,
	pub(super) dangling: Candidates,
	pub(super) promotable: Candidates,
	pub(super) contended: u64,
	pub(super) unresolved: u64,
	pub(super) malformed: u64,
}

/// Everything one scan measured, and the worklists the repair consumes.
///
/// The deep counts stay zero when neither family has a loser, since the
/// deeper indexes are not read in that case.
#[derive(Default)]
pub(super) struct Scan {
	pub(super) events: Family,
	pub(super) statekeys: Family,
	pub(super) dirty: u64,
	pub(super) entries: u64,
	pub(super) infected: BTreeSet<u64>,
	pub(super) orphans: u64,
	pub(super) missing_parents: u64,
	pub(super) infected_parents: u64,
	pub(super) malformed_diffs: u64,
	pub(super) moves: Vec<u64>,
	pub(super) relations: Relations,
	pub(super) strays: u64,
	pub(super) unverifiable: bool,
}

/// Statediff walk context: the bitmaps each row's entries are tested
/// against.
///
/// Folded with a [`Counts`] accumulator over every
/// `shortstatehash_statediff` row by [`Diffs::row`].
struct Diffs<'a> {
	counter: u64,
	event_stale: &'a [u64],
	statekey_stale: &'a [u64],
	event_reverse: &'a [u64],
	statekey_reverse: &'a [u64],
}

/// Counts the statediff walk accumulates.
///
/// `malformed` covers rows the framing rejects, entries included; the
/// ghost tallies surface in the sweep's log and the remaining fields
/// mirror their [`Scan`] counterparts.
#[derive(Default)]
struct Counts {
	infected: BTreeSet<u64>,
	ghosts: u64,
	removed_ghosts: u64,
	orphans: u64,
	missing_parents: u64,
	malformed: u64,
}

/// The `sroomid` field of a stored notification value.
///
/// A mirror of the pusher's stored shape just wide enough for the stray
/// census; every other field is ignored.
#[derive(Deserialize)]
struct Notification {
	sroomid: u64,
}

/// Measures short id injectivity across both families.
///
/// Each reverse map streams before its forward map, so a concurrent
/// allocation surfaces only on the forward side and cannot be flagged
/// stale. The deeper indexes are read only when a loser exists.
#[tracing::instrument(level = "debug", skip_all)]
pub(super) async fn scan(services: &Services) -> Result<Scan> {
	info!("Scanning ShortID columns for duplicate values...");

	let counter = services.globals.current_count();

	if counter >= MAX_SHORT {
		warn!(
			%counter,
			"Short id space too large to verify injectivity; stale auth chain caches, if \
			 any, survive and can distort state resolution until `server clear-caches`."
		);
		return Ok(Scan { unverifiable: true, ..Default::default() });
	}

	let words = usize::try_from((counter / 64).saturating_add(1))
		.map_err(|_| err!("short id bitmap exceeds the address width"))?;

	let (events, event_reverse) =
		family(services, "eventid_shorteventid", "shorteventid_eventid", counter, words).await;

	let (statekeys, statekey_reverse) =
		family(services, "statekey_shortstatekey", "shortstatekey_statekey", counter, words)
			.await;

	if events.losers.is_empty() && statekeys.losers.is_empty() {
		return Ok(Scan { events, statekeys, ..Default::default() });
	}

	let swept =
		sweep(services, &events, &statekeys, event_reverse, statekey_reverse, counter).await;

	let scan = Scan { events, statekeys, ..swept };

	// The stray census is the widest pass and gates nothing; it reports on
	// the boot that acts and skips the rescans a refusal or a heal causes.
	match scan.anomalous() || scan.healable() {
		| true => Ok(scan),
		| false => {
			let strays = strays(&services.db, counter, words).await;

			Ok(Scan { strays, ..scan })
		},
	}
}

/// Whether any count impugns the scan or exceeds what the repair handles.
///
/// Any anomaly refuses the destructive repair lane; the cache-clearing
/// lane is unconditionally safe and proceeds regardless. The classes a
/// heal pass completes are gone by the time this decides, so they are
/// absent here.
#[implement(Scan)]
pub(super) fn anomalous(&self) -> bool {
	self.events.anomalous()
		|| self.statekeys.anomalous()
		|| self.orphans > 0
		|| self.missing_parents > 0
		|| self.infected_parents > 0
		|| self.malformed_diffs > 0
}

/// Whether a heal pass has rows to complete in either family.
///
/// The orphan and parent counts are taken against bitmaps a heal then
/// changes, so a healable scan decides nothing else until it rescans.
#[implement(Scan)]
pub(super) fn healable(&self) -> bool { self.events.healable() || self.statekeys.healable() }

/// Whether this family carries a shape the repair does not handle.
///
/// A contended slot has two claimants and nothing to break the tie, an
/// unresolved row was never proved absent, and a malformed key leaves the
/// bitmaps too incomplete for any other verdict to stand.
#[implement(Family)]
fn anomalous(&self) -> bool { self.contended > 0 || self.unresolved > 0 || self.malformed > 0 }

/// Whether this family has rows a heal pass completes.
///
/// Anything impugning the family withholds both classes: the same
/// bitmaps that name a dangling winner are the ones a malformed key
/// leaves incomplete.
#[implement(Family)]
pub(super) fn healable(&self) -> bool {
	!self.anomalous() && (!self.dangling.is_empty() || !self.promotable.is_empty())
}

/// Scans one family in two passes, and a third only where the bitmaps
/// disagree.
///
/// The reverse bitmap completes before the forward stream begins, so a
/// concurrent allocation surfaces only forward-side and cannot be counted
/// dangling or stale. The third pass names each loser and its identity; on
/// a clean family the bitmap difference proves there are none, so it never
/// runs. Returns the family and its reverse-key bitmap, which the deep sweep
/// reuses to detect orphaned statediff entries.
#[tracing::instrument(
	level = "debug",
	skip_all,
	fields(
		%forward,
		%reverse,
	),
)]
async fn family(
	services: &Services,
	forward: &'static str,
	reverse: &'static str,
	counter: u64,
	words: usize,
) -> (Family, Bits) {
	let db = &services.db;

	let (reverse_bits, rows, reverse_malformed) = reverse_bitmap(&db[reverse], words).await;

	let (forward_bits, mut dangling, forward_malformed) =
		dangling_winners(&db[forward], &reverse_bits, counter, words).await;

	// A set reverse bit no forward value claims is what the pass collects.
	let candidates = match any_unclaimed(&reverse_bits, &forward_bits, counter) {
		| false => Candidates::new(),
		| true => loser_candidates(&db[reverse], &forward_bits, counter).await,
	};

	drop(forward_bits);

	let (losers, winners, mut promotable, unresolved) = resolve(&db[forward], &candidates).await;

	let contended = contenders(&mut dangling, by_short)
		.saturating_add(contenders(&mut promotable, by_identity));

	let family = Family {
		rows,
		losers,
		winners,
		dangling,
		promotable,
		contended,
		unresolved,
		malformed: reverse_malformed.saturating_add(forward_malformed),
	};

	info!(
		%forward,
		%reverse,
		rows = family.rows,
		losers = family.losers.len(),
		dangling = family.dangling.len(),
		promotable = family.promotable.len(),
		contended = family.contended,
		unresolved = family.unresolved,
		malformed = family.malformed,
		"Finished scanning column pair."
	);

	(family, reverse_bits)
}

/// Streams a reverse map into its keyset bitmap, its row count, and its
/// count of keys that are not an 8-byte short id.
///
/// The rows are counted here rather than in the loser pass, which a clean
/// family skips.
async fn reverse_bitmap(map: &Arc<Map>, words: usize) -> (Bits, u64, u64) {
	map.raw_keys()
		.ignore_err()
		.ready_fold((vec![0_u64; words], 0_u64, 0_u64), |(mut bits, rows, malformed), key| {
			let rows = rows.saturating_add(1);

			match short_of(key) {
				| None => (bits, rows, malformed.saturating_add(1)),
				| Some(short) => {
					set_bit(&mut bits, short);

					(bits, rows, malformed)
				},
			}
		})
		.await
}

/// Streams a forward map against the reverse bitmap for dangling winners.
///
/// A dangling winner is a forward value no reverse row answers for.
/// Values past the counter are concurrent allocations, not danglings. Each
/// one carries the identity its forward row is keyed by, which is the
/// reverse row a heal reinstates.
async fn dangling_winners(
	map: &Arc<Map>,
	reverse_bits: &[u64],
	counter: u64,
	words: usize,
) -> (Bits, Candidates, u64) {
	map.raw_stream()
		.ignore_err()
		.ready_fold(
			(vec![0_u64; words], Candidates::new(), 0_u64),
			|(mut bits, mut dangling, malformed), (key, value)| match short_of(value) {
				| None => (bits, dangling, malformed.saturating_add(1)),
				| Some(short) => {
					if short <= counter && !get_bit(reverse_bits, short) {
						dangling.push((short, Identity::from_slice(key)));
					}

					set_bit(&mut bits, short);

					(bits, dangling, malformed)
				},
			},
		)
		.await
}

/// Whether any reverse key's short id went unclaimed by a forward value.
///
/// The bitmaps round up to a whole word, so ids past the counter are
/// addressable in the last one and are masked off. The mask keeps the
/// counter's own bit, matching the bound the loser pass applies.
fn any_unclaimed(reverse_bits: &[u64], forward_bits: &[u64], counter: u64) -> bool {
	let last = usize::try_from(counter / 64).unwrap_or(usize::MAX);
	let tail = u64::MAX >> 63_u64.saturating_sub(counter % 64);

	debug_assert_eq!(reverse_bits.len(), last.saturating_add(1), "bitmap spans the counter");
	debug_assert_eq!(forward_bits.len(), reverse_bits.len(), "bitmaps span one id space");

	reverse_bits
		.iter()
		.copied()
		.zip(forward_bits.iter().copied())
		.enumerate()
		.any(|(word, (reverse, forward))| {
			let mask = match word < last {
				| true => u64::MAX,
				| false => tail,
			};

			(reverse & !forward & mask) != 0
		})
}

/// Collects reverse keys no forward value claims.
///
/// The identity each row names rides along for the dereference pass.
async fn loser_candidates(map: &Arc<Map>, forward_bits: &[u64], counter: u64) -> Candidates {
	map.raw_stream()
		.ignore_err()
		.ready_fold(Candidates::new(), |mut candidates, (key, value)| {
			let unclaimed =
				short_of(key).filter(|short| *short <= counter && !get_bit(forward_bits, *short));

			if let Some(short) = unclaimed {
				candidates.push((short, Identity::from_slice(value)));
			}

			candidates
		})
		.await
}

/// Dereferences each candidate's identity to split losers from winners.
///
/// The identity a loser's reverse row names must hold a live forward row,
/// whose value is the winner. A candidate resolving to itself was a
/// concurrent allocation, not a loser.
async fn resolve(map: &Arc<Map>, candidates: &[(u64, Identity)]) -> Resolution {
	let (mut losers, winners, promotable, unsettled, paired) = candidates
		.iter()
		.map(candidate_identity)
		.stream()
		.get(map)
		.map(resolution)
		.zip(candidates.iter().stream())
		.ready_fold(
			(Vec::new(), BTreeMap::new(), Candidates::new(), 0_u64, 0_usize),
			|(mut losers, mut winners, mut promotable, unsettled, paired),
			 (resolved, candidate)| {
				let paired = paired.saturating_add(1);
				let loser = candidate_short(candidate);

				match resolved {
					| Resolved::Winner(winner) if winner == loser =>
						(losers, winners, promotable, unsettled, paired),
					| Resolved::Winner(winner) => {
						losers.push(loser);
						winners.insert(loser, winner);

						(losers, winners, promotable, unsettled, paired)
					},
					| Resolved::Absent => {
						losers.push(loser);
						promotable.push(candidate.clone());

						(losers, winners, promotable, unsettled, paired)
					},
					| Resolved::Unsettled => {
						losers.push(loser);

						(losers, winners, promotable, unsettled.saturating_add(1), paired)
					},
				}
			},
		)
		.await;

	// A batched lookup can compress a failed chunk into one error item,
	// desynchronizing the zip; the unpaired tail is undereferenced, so it refuses.
	let tail = candidates.get(paired..).unwrap_or_default();
	losers.extend(tail.iter().map(candidate_short));

	let unresolved = unsettled.saturating_add(u64::try_from(tail.len()).unwrap_or(u64::MAX));

	(losers, winners, promotable, unresolved)
}

/// Counts candidates contending for a slot another candidate already
/// claims.
///
/// Sorting is what makes contenders adjacent; the comparator names the
/// half of the pair that decides the slot, the short id for a
/// reinstatement and the identity for a promotion.
fn contenders<F>(candidates: &mut [Candidate], cmp: F) -> u64
where
	F: Fn(&Candidate, &Candidate) -> Ordering,
{
	candidates.sort_unstable_by(&cmp);

	let contenders = candidates
		.windows(2)
		.filter(|pair| cmp(&pair[0], &pair[1]).is_eq())
		.count();

	u64::try_from(contenders).unwrap_or(u64::MAX)
}

// Named for the higher-ranked closure generality the dereference stream
// needs; an inline closure pins the item lifetimes.
fn candidate_identity((_, identity): &Candidate) -> &Identity { identity }

fn candidate_short((short, _): &Candidate) -> u64 { *short }

fn by_short(a: &Candidate, b: &Candidate) -> Ordering { a.0.cmp(&b.0) }

fn by_identity(a: &Candidate, b: &Candidate) -> Ordering { a.1.cmp(&b.1) }

// A failed read is not an absent row; only the not-found error proves the
// forward row is missing, and a promotion is a write.
fn resolution(result: Result<Handle<'_>>) -> Resolved {
	match result {
		| Ok(handle) => short_of(&handle).map_or(Resolved::Unsettled, Resolved::Winner),
		| Err(error) if error.is_not_found() => Resolved::Absent,
		| Err(_) => Resolved::Unsettled,
	}
}

/// Reads the deeper indexes once a loser exists in either family.
///
/// Statediff entries are tested against both families and chain-cache rows
/// against either, while the shortroomid families are counted for the
/// report without gating any repair.
#[tracing::instrument(level = "debug", skip_all)]
async fn sweep(
	services: &Services,
	events: &Family,
	statekeys: &Family,
	event_reverse: Bits,
	statekey_reverse: Bits,
	counter: u64,
) -> Scan {
	let db = &services.db;
	let words = event_reverse.len();
	let event_stale = bits_of(&events.losers, words);
	let statekey_stale = bits_of(&statekeys.losers, words);

	let walk = Diffs {
		counter,
		event_stale: &event_stale,
		statekey_stale: &statekey_stale,
		event_reverse: &event_reverse,
		statekey_reverse: &statekey_reverse,
	};

	let counts = diffs(db, words, walk).await;

	drop(event_reverse);
	drop(statekey_reverse);

	// A descendant of an infected state would need re-derivation down the
	// diff chain, which is not built; the anomaly refuses the destructive
	// lane instead.
	let infected_parents = match counts.infected.is_empty() {
		| true => 0,
		| false =>
			db["shortstatehash_statediff"]
				.raw_stream()
				.ignore_err()
				.ready_fold(0_u64, |descendants, (_, value)| {
					let parent = value.get(0..8).and_then(short_of);

					descendants.saturating_add(u64::from(
						parent.is_some_and(|parent| counts.infected.contains(&parent)),
					))
				})
				.await,
	};

	// A key or value that is stale or not a whole number of short ids
	// poisons the row either way.
	let (dirty, entries) = db["authchainkey_authchain"]
		.raw_stream()
		.ignore_err()
		.ready_fold((0_u64, 0_u64), |(dirty, entries), (key, chain)| {
			let hit = disposable(key, &event_stale, &statekey_stale)
				|| disposable(chain, &event_stale, &statekey_stale);

			(dirty.saturating_add(u64::from(hit)), entries.saturating_add(1))
		})
		.await;

	// ready_fold rather than ready_filter_map: the higher-ranked adapter
	// fails the boot coroutine's Send obligation over cursor-borrowed items.
	let moves: Vec<u64> = db["shorteventid_shortstatehash"]
		.raw_keys()
		.ignore_err()
		.ready_fold(Vec::new(), |mut moves, key| {
			if let Some(loser) = short_of(key).filter(|short| get_bit(&event_stale, *short)) {
				moves.push(loser);
			}

			moves
		})
		.await;

	let relations: Relations = db["relatesto_typed"]
		.raw_stream()
		.ignore_err()
		.ready_fold(Relations::new(), |mut relations, (key, value)| {
			let dirty = short_of(value)
				.filter(|loser| get_bit(&event_stale, *loser))
				.zip(RelationKey::try_from(key).ok());

			if let Some((loser, key)) = dirty {
				relations.push((key, loser));
			}

			relations
		})
		.await;

	// dirty and entries read zero on any boot whose chain clear ran first;
	// a refusing database's later boots report the live dirt instead.
	warn!(
		dirty,
		entries,
		infected = counts.infected.len(),
		ghosts = counts.ghosts,
		removed_ghosts = counts.removed_ghosts,
		orphans = counts.orphans,
		missing_parents = counts.missing_parents,
		infected_parents,
		malformed_diffs = counts.malformed,
		moves = moves.len(),
		relations = relations.len(),
		"Swept the deeper short id indexes."
	);

	Scan {
		dirty,
		entries,
		infected: counts.infected,
		orphans: counts.orphans,
		missing_parents: counts.missing_parents,
		infected_parents,
		malformed_diffs: counts.malformed,
		moves,
		relations,
		..Default::default()
	}
}

/// Folds every statediff row through the walk, its parent keyset first.
///
/// The whole keyset must precede the row walk, a row's parent appearing
/// anywhere in the file.
async fn diffs(db: &Database, words: usize, walk: Diffs<'_>) -> Counts {
	let parents = db["shortstatehash_statediff"]
		.raw_keys()
		.ignore_err()
		.ready_fold(vec![0_u64; words], |mut bits, key| {
			if let Some(short) = short_of(key) {
				set_bit(&mut bits, short);
			}

			bits
		})
		.await;

	db["shortstatehash_statediff"]
		.raw_stream()
		.ignore_err()
		.ready_fold(Counts::default(), |counts, (key, value)| {
			walk.row(counts, key, value, &parents)
		})
		.await
}

impl Diffs<'_> {
	/// Folds one statediff row through the walk.
	///
	/// The value carries an 8-byte parent, then 16-byte entries of a
	/// statekey and an event half, an added run first and a removed run
	/// only behind an 8-byte zero sentinel. The sentinel shifts entry
	/// alignment by 8, so the walk is sequential rather than chunked.
	fn row(&self, mut counts: Counts, key: &[u8], value: &[u8], parents: &[u64]) -> Counts {
		let (Some(row), Some(parent)) = (short_of(key), value.get(0..8).and_then(short_of))
		else {
			counts.malformed = counts.malformed.saturating_add(1);
			return counts;
		};

		if parent != 0 && parent <= self.counter && !get_bit(parents, parent) {
			counts.missing_parents = counts.missing_parents.saturating_add(1);
		}

		let mut removed_run = false;
		let mut removed = 0_u64;
		let mut at = 8_usize;

		while at < value.len() {
			if !removed_run && value[at..].starts_with(&0_u64.to_be_bytes()) {
				removed_run = true;
				at = at.saturating_add(8);
				continue;
			}

			let entries = (
				value
					.get(at..at.saturating_add(8))
					.and_then(short_of),
				value
					.get(at.saturating_add(8)..at.saturating_add(16))
					.and_then(short_of),
			);

			let (Some(statekey), Some(event)) = entries else {
				counts.malformed = counts.malformed.saturating_add(1);
				return counts;
			};

			removed = removed.saturating_add(u64::from(removed_run));

			if get_bit(self.statekey_stale, statekey) || get_bit(self.event_stale, event) {
				counts.infected.insert(row);
				counts.ghosts = counts.ghosts.saturating_add(1);
				counts.removed_ghosts = counts
					.removed_ghosts
					.saturating_add(u64::from(removed_run));
			}

			let orphaned = (statekey <= self.counter
				&& !get_bit(self.statekey_reverse, statekey))
				|| (event <= self.counter && !get_bit(self.event_reverse, event));

			counts.orphans = counts.orphans.saturating_add(u64::from(orphaned));
			at = at.saturating_add(16);
		}

		// The writer gates the sentinel on a nonempty removed run.
		if removed_run && removed == 0 {
			counts.malformed = counts.malformed.saturating_add(1);
		}

		counts
	}
}

/// Counts shortroomid references with no forward row.
///
/// Purged rooms and losing allocations both produce them; no repair step
/// touches a shortroomid family, so the count reports and gates nothing.
#[tracing::instrument(level = "debug", skip_all)]
async fn strays(db: &Database, counter: u64, words: usize) -> u64 {
	let rooms = db["roomid_shortroomid"]
		.raw_stream()
		.ignore_err()
		.ready_fold(vec![0_u64; words], |mut bits, (_, value)| {
			if let Some(short) = short_of(value) {
				set_bit(&mut bits, short);
			}

			bits
		})
		.await;

	let stray = |short: Option<u64>| {
		u64::from(short.is_some_and(|short| short <= counter && !get_bit(&rooms, short)))
	};

	let strays = db["pduid_pdu"]
		.raw_keys()
		.ignore_err()
		.ready_fold(0_u64, |strays, key| {
			strays.saturating_add(stray(key.get(0..8).and_then(short_of)))
		})
		.await;

	// The search key carries the shortroomid twice: as the prefix and again
	// inside the pdu id behind the separator-terminated word.
	let strays = db["tokenids"]
		.raw_keys()
		.ignore_err()
		.ready_fold(strays, |strays, key| {
			let prefix = key.get(0..8).and_then(short_of);
			let embedded = key.get(8..).and_then(pdu_shortroomid);

			strays
				.saturating_add(stray(prefix))
				.saturating_add(stray(embedded))
		})
		.await;

	// Sending-queue keys hold a pdu id behind the destination only when
	// the value is empty; nonempty rows queue EDUs.
	let current = db["servercurrentevent_data"]
		.raw_stream()
		.ignore_err();

	let strays = db["servernameevent_data"]
		.raw_stream()
		.ignore_err()
		.chain(current)
		.ready_fold(strays, |strays, (key, value)| {
			let pdu = value.is_empty().and_then(|| pdu_shortroomid(key));

			strays.saturating_add(stray(pdu))
		})
		.await;

	db["useridcount_notification"]
		.raw_stream()
		.ignore_err()
		.ready_fold(strays, |strays, (_, value)| {
			let sroomid = serde_json::from_slice(value)
				.ok()
				.map(|notification: Notification| notification.sroomid);

			strays.saturating_add(stray(sroomid))
		})
		.await
}

/// Extracts the shortroomid of a pdu id sitting behind a separator.
///
/// The pdu id must have the 16-byte normal or 24-byte backfilled width;
/// anything else yields nothing.
fn pdu_shortroomid(bytes: &[u8]) -> Option<u64> {
	let sep = bytes.iter().position(|&byte| byte == SEP)?;
	let id = bytes.get(sep.saturating_add(1)..)?;

	(id.len() == 16 || id.len() == 24)
		.and_then(|| id.get(0..8))
		.and_then(short_of)
}

pub(super) fn short_of(bytes: &[u8]) -> Option<u64> {
	bytes.try_into().ok().map(u64::from_be_bytes)
}

fn bits_of(shorts: &[u64], words: usize) -> Bits {
	shorts
		.iter()
		.fold(vec![0_u64; words], |mut bits, short| {
			set_bit(&mut bits, *short);

			bits
		})
}

fn disposable(bytes: &[u8], event_stale: &[u64], statekey_stale: &[u64]) -> bool {
	!bytes.len().is_multiple_of(size_of::<u64>())
		|| references(bytes, event_stale, statekey_stale)
}

fn references(bytes: &[u8], event_stale: &[u64], statekey_stale: &[u64]) -> bool {
	bytes
		.as_chunks::<{ size_of::<u64>() }>()
		.0
		.iter()
		.copied()
		.map(u64::from_be_bytes)
		.any(|short| get_bit(event_stale, short) || get_bit(statekey_stale, short))
}

fn set_bit(bits: &mut [u64], index: u64) {
	if let Some(word) = usize::try_from(index / 64)
		.ok()
		.and_then(|word| bits.get_mut(word))
	{
		*word |= 1_u64 << (index % 64);
	}
}

fn get_bit(bits: &[u64], index: u64) -> bool {
	usize::try_from(index / 64)
		.ok()
		.and_then(|word| bits.get(word))
		.is_some_and(|word| word & (1_u64 << (index % 64)) != 0)
}

#[cfg(test)]
mod tests {
	use super::{Candidate, Family, Identity, by_identity, by_short, contenders};

	fn candidate(short: u64, identity: &[u8]) -> Candidate {
		(short, Identity::from_slice(identity))
	}

	#[test]
	fn contenders_counts_two_forward_rows_claiming_one_short() {
		let mut dangling = vec![candidate(7, b"$a"), candidate(7, b"$b"), candidate(9, b"$c")];

		assert_eq!(contenders(&mut dangling, by_short), 1);
	}

	#[test]
	fn contenders_counts_two_reverse_rows_naming_one_identity() {
		let mut promotable = vec![candidate(7, b"$a"), candidate(9, b"$a"), candidate(11, b"$b")];

		assert_eq!(contenders(&mut promotable, by_identity), 1);
	}

	#[test]
	fn contenders_is_zero_when_every_slot_is_claimed_once() {
		let mut dangling = vec![candidate(9, b"$a"), candidate(7, b"$b")];

		assert_eq!(contenders(&mut dangling, by_short), 0);
	}

	#[test]
	fn a_lone_dangling_winner_heals_without_refusing() {
		let family = Family {
			dangling: vec![candidate(7, b"$a")],
			..Default::default()
		};

		assert!(family.healable());
		assert!(!family.anomalous());
	}

	#[test]
	fn a_contended_short_refuses_instead_of_healing() {
		let family = Family {
			dangling: vec![candidate(7, b"$a"), candidate(7, b"$b")],
			contended: 1,
			..Default::default()
		};

		assert!(!family.healable());
		assert!(family.anomalous());
	}

	#[test]
	fn a_malformed_key_withholds_the_heal() {
		let family = Family {
			dangling: vec![candidate(7, b"$a")],
			malformed: 1,
			..Default::default()
		};

		assert!(!family.healable());
	}

	#[test]
	fn an_unresolved_row_withholds_the_promotion() {
		let family = Family {
			promotable: vec![candidate(7, b"$a")],
			unresolved: 1,
			..Default::default()
		};

		assert!(!family.healable());
	}
}
