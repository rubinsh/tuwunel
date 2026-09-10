use ipaddress::IPAddress;
use reqwest::Url;

use super::valid_cidr_range_url;

#[test]
fn url_cidr_check_handles_ipv4_ipv6_and_domains() {
	let denylist = [
		IPAddress::parse("10.0.0.0/8").expect("test denylist range parses"),
		IPAddress::parse("::1/128").expect("test denylist range parses"),
	];

	let ipv4 = Url::parse("http://10.1.2.3/").expect("test URL parses");
	let ipv6 = Url::parse("http://[::1]/").expect("test URL parses");
	let allowed = Url::parse("https://8.8.8.8/").expect("test URL parses");
	let domain = Url::parse("https://example.com/").expect("test URL parses");

	assert!(!valid_cidr_range_url(&denylist, &ipv4));
	assert!(!valid_cidr_range_url(&denylist, &ipv6));
	assert!(valid_cidr_range_url(&denylist, &allowed));
	assert!(valid_cidr_range_url(&denylist, &domain));
}

/// The protected route is keyed on the whole notification URL.
///
/// These pin the *matcher*, which is the part that decides whether a request
/// gets the denylist exemption at all. Every miss here is a request that falls
/// back to the ordinary pusher client and the ordinary denylist — so a matcher
/// that is too generous is the failure that matters, and most of these are
/// near-misses rather than unrelated URLs.
mod protected_route {
	use std::net::SocketAddr;

	use reqwest::Url;

	use super::super::{ProtectedGateway, protected_route};

	const CONFIGURED: &str = "https://gateway.example.com:3101/_matrix/push/v1/notify";
	const PINNED: &str = "127.0.0.1:3101";

	fn gateway() -> ProtectedGateway {
		ProtectedGateway {
			url: Url::parse(CONFIGURED).expect("test gateway URL parses"),
			addr: PINNED.parse::<SocketAddr>().expect("test pin parses"),
		}
	}

	fn route(url: &str) -> Option<SocketAddr> {
		let url = Url::parse(url).expect("test URL parses");

		protected_route(Some(&gateway()), &url)
	}

	#[test]
	fn the_configured_url_is_pinned() {
		assert_eq!(route(CONFIGURED), Some(PINNED.parse().expect("test pin parses")));
	}

	#[test]
	fn an_unconfigured_server_pins_nothing() {
		let url = Url::parse(CONFIGURED).expect("test URL parses");

		assert_eq!(protected_route(None, &url), None);
	}

	#[test]
	fn the_host_is_matched_case_insensitively() {
		// Not leniency: `Url` lowercases the host at parse, so this asserts the
		// normalization the matcher relies on actually happens.
		assert!(
			route("https://Gateway.Example.COM:3101/_matrix/push/v1/notify").is_some()
		);
	}

	#[test]
	fn a_different_path_on_the_same_gateway_is_not_pinned() {
		// The case a hostname-level override cannot express, and the reason the
		// matcher works on the URL: same host, same port, different endpoint.
		assert_eq!(route("https://gateway.example.com:3101/_matrix/push/v1/other"), None);
		assert_eq!(route("https://gateway.example.com:3101/"), None);
	}

	#[test]
	fn a_different_port_on_the_same_host_is_not_pinned() {
		assert_eq!(route("https://gateway.example.com:3102/_matrix/push/v1/notify"), None);
		assert_eq!(route("https://gateway.example.com/_matrix/push/v1/notify"), None);
	}

	#[test]
	fn a_different_host_is_not_pinned() {
		assert_eq!(route("https://other.example.com:3101/_matrix/push/v1/notify"), None);
		// A suffix of the configured name: the shape a naive `ends_with` match
		// would wrongly accept.
		assert_eq!(route("https://evil-gateway.example.com:3101/_matrix/push/v1/notify"), None);
	}

	#[test]
	fn a_plaintext_url_to_the_same_place_is_not_pinned() {
		// Startup refuses an http gateway URL, so this can only arrive as a
		// pusher's own destination. It must not inherit the exemption: the
		// pin's whole guarantee is the certificate check.
		assert_eq!(route("http://gateway.example.com:3101/_matrix/push/v1/notify"), None);
	}

	#[test]
	fn credentials_or_a_query_do_not_smuggle_a_match() {
		assert_eq!(
			route("https://user:pw@gateway.example.com:3101/_matrix/push/v1/notify"),
			None
		);
		assert_eq!(
			route("https://gateway.example.com:3101/_matrix/push/v1/notify?x=1"),
			None
		);
	}
}
