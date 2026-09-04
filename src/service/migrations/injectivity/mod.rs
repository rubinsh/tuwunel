//! Short id injectivity: the one-time scan and repair.
//!
//! Releases before v1.8.3 could mint two short ids for one identity,
//! leaving stale reverse rows in both families, ghost entries in a few
//! compressed states, and auth chains cached from both allocations. The
//! migration measures that residue, repairs what matches the shapes it
//! handles, and marks itself complete like any other. Every release
//! through v1.8.3 also memoized auth chains truncated at a missing
//! ancestor, which no scan tells from a whole one, so the cache is
//! discarded once on a marker of its own.

mod repair;
mod scan;

use tuwunel_core::{
	Result,
	result::{LogErr, NotFound},
	warn,
};

use self::{
	repair::{heal, repair},
	scan::scan,
};
use crate::{Service, Services};

/// Global marker recording the repair ran to completion.
///
/// Refused or unverifiable residue leaves it unwritten, so the next boot
/// scans again.
static MARKER: &[u8] = b"fix_short_injectivity";

/// Global marker recording the one-time auth chain cache clear.
///
/// Gating the clear on [`MARKER`] would re-run it on every boot a refused
/// repair leaves unstamped.
static CLEAR_MARKER: &[u8] = b"clear_auth_chain_cache";

/// Scan passes one boot allows before giving up on convergence.
///
/// A heal completes torn writes and rescans to re-measure what they
/// explain, and each pass strictly reduces the classes it heals, so the
/// second pass is the one that repairs. The last pass never heals, which
/// bounds a shape that does not settle and keeps the dirt-driven clearing
/// lane in [`repair`] reachable on every boot.
const PASSES: usize = 3;

/// Runs the one-time chain cache clear, then the injectivity scan, heal,
/// and repair behind [`MARKER`].
///
/// The clear takes [`CLEAR_MARKER`] and runs ahead of the early return, so
/// a database that already completed the repair still discards its chains.
/// The stamp follows the repair's own verdict: only a settled repair writes
/// it. A heal rescans rather than repairing, because the orphan and parent
/// counts a refusal turns on are taken against bitmaps the heal changes.
#[tracing::instrument(level = "debug", skip_all)]
pub(super) async fn fix(services: &Services) -> Result {
	let global = &services.db["global"];

	if global.get(CLEAR_MARKER).await.is_not_found() {
		clear_chain_cache(services).await;
		services.db["authchainkey_authchain"]
			.sort()
			.log_err()
			.ok();
	}

	if !global.get(MARKER).await.is_not_found() {
		return Ok(());
	}

	for pass in 1..=PASSES {
		let residue = scan(services).await?;

		// The last pass repairs rather than heals, so the dirt-driven
		// clearing lane still fires on a boot whose heals never settle.
		if pass < PASSES && heal(services, &residue) {
			continue;
		}

		if repair(services, &residue).await? {
			global.insert(MARKER, []);
		}

		break;
	}

	Ok(())
}

/// Discards auth chains cached before walk completeness was enforced.
///
/// A chain truncated at a missing ancestor is well-formed, so no scan
/// separates it from a whole one and the population goes at once. The
/// cache is derived and rebuilds on demand.
#[tracing::instrument(level = "debug", skip_all)]
async fn clear_chain_cache(services: &Services) {
	let global = &services.db["global"];

	warn!("Discarding cached auth chains; entries from earlier releases may be truncated.");

	clear_chains(services).await;
	global.insert(CLEAR_MARKER, []);
}

/// Deletes every auth chain cache row under one cork.
///
/// `Map::clear` deletes key by key and `Map::remove` flushes the WAL per
/// key when uncorked. It is snapshot-based, so it holds only because
/// migrations precede the workers that populate the cache.
pub(super) async fn clear_chains(services: &Services) {
	let _cork = services.db.cork_and_sync();

	services.auth_chain.clear_cache().await;
}

/// Stamps both markers on a fresh database.
///
/// A fresh database never ran the unserialized allocator and holds no
/// cached chains, so it has neither residue to scan for nor a cache to
/// discard.
pub(super) fn mark_clean(services: &Services) {
	let global = &services.db["global"];

	global.insert(MARKER, []);
	global.insert(CLEAR_MARKER, []);
}
