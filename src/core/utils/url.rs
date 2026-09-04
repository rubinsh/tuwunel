//! Hostname and URL matching utilities.
//!
//! These helpers provide reusable matching rules that the URL parser does not
//! expose directly.

/// Reports whether a hostname is equal to or beneath a domain name.
///
/// Matching is ASCII case-insensitive and accepts a domain with an optional
/// leading dot. A suffix only matches at a DNS label boundary. A single dot
/// matches only a hostname with a trailing dot.
#[must_use]
pub fn hostname_matches_domain(hostname: &str, domain: &str) -> bool {
	if domain == "." {
		return hostname.ends_with('.');
	}

	let domain = domain.strip_prefix('.').unwrap_or(domain);

	if domain.is_empty() {
		return false;
	}

	if hostname.eq_ignore_ascii_case(domain) {
		return true;
	}

	let Some(separator) = hostname
		.len()
		.checked_sub(domain.len())
		.and_then(|index| index.checked_sub(1))
	else {
		return false;
	};

	let Some(suffix_start) = separator.checked_add(1) else {
		return false;
	};

	hostname.as_bytes().get(separator) == Some(&b'.')
		&& hostname
			.get(suffix_start..)
			.is_some_and(|suffix| suffix.eq_ignore_ascii_case(domain))
}
