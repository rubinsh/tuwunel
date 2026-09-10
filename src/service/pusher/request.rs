use std::{fmt::Debug, mem::swap, net::SocketAddr};

use bytes::BytesMut;
use http::Response as HttpResponse;
use reqwest::{Error as ReqwestError, Response};
use ruma::api::{
	IncomingResponse, OutgoingRequest, OutgoingRequestExt, auth_scheme::AuthScheme,
	path_builder::PathBuilder,
};
use tuwunel_core::{
	Err, Result, debug_warn, err, error::error_chain, implement, trace, utils::string_from_bytes,
	warn,
};
use url::Url;

use crate::client::read_response_capped;

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub(super) async fn send_request<T>(&self, dest: &str, request: T) -> Result<T::IncomingResponse>
where
	T: OutgoingRequest + Debug + Send,
	for<'a> T::Authentication: AuthScheme<Input<'a> = ()>,
	for<'a> T::PathBuilder: PathBuilder<Input<'a> = ()>,
{
	let dest = if dest.contains(['?', '#']) {
		let parsed = Url::parse(dest).ok();

		warn!(
			gateway_host = parsed
				.as_ref()
				.and_then(|url| url.host_str())
				.unwrap_or("<invalid>"),
			has_query = dest.contains('?'),
			has_fragment = dest.contains('#'),
			"Push gateway URL carries a query string or fragment, which is not supported; the \
			 notification path is appended after it",
		);

		dest
	} else {
		let push_path = self
			.services
			.config
			.notification_push_path
			.trim_end_matches('/');

		let dest = dest.trim_end_matches('/');

		dest.strip_suffix(push_path).unwrap_or(dest)
	};

	trace!("Push gateway destination: {dest}");

	let http_request = request
		.try_into_http_request::<BytesMut>(dest, (), ())
		.map_err(|e| {
			err!(BadServerResponse(warn!(
				"Failed to find destination {dest} for push gateway: {e}"
			)))
		})?
		.map(BytesMut::freeze);

	let reqwest_request = reqwest::Request::try_from(http_request)?;

	if self
		.services
		.client
		.proxy
		.resolver_alias(reqwest_request.url())
	{
		return Err!(BadServerResponse(
			"Not allowed to request a locally resolved proxy endpoint"
		));
	}

	trace!("Checking request URL for IP");
	if !self
		.services
		.client
		.valid_cidr_range_url(reqwest_request.url())
	{
		return Err!(BadServerResponse("Not allowed to send requests to this IP"));
	}

	// The protected route is selected by the *final* request URL, after the
	// path has been built. Selecting earlier — on the pusher's configured
	// destination, or on its host — would hand the exemption to URLs that only
	// resemble the configured one.
	let pinned = self
		.services
		.client
		.protected_pusher_route(reqwest_request.url());

	let client = match pinned {
		| None => &self.services.client.pusher,
		| Some(_) => self
			.services
			.client
			.pusher_protected
			.as_ref()
			.expect("a pinned route implies the protected client was built"),
	};

	match client.execute(reqwest_request).await {
		| Err(error) => handler_err(dest, error),
		| Ok(response) => self.handle_ok::<T>(dest, response, pinned).await,
	}
}

#[implement(super::Service)]
async fn handle_ok<T>(
	&self,
	dest: &str,
	mut response: Response,
	pinned: Option<SocketAddr>,
) -> Result<T::IncomingResponse>
where
	T: OutgoingRequest,
{
	trace!("Checking response destination's IP");
	match pinned {
		// The protected route's peer is checked positively: present, and
		// exactly the configured address. The ordinary check below is a
		// denylist, and a denylist cannot express this — the pinned address is
		// precisely one the denylist forbids, so reusing it here would reject
		// every notification the route exists to deliver.
		| Some(expected) =>
			if let Some(problem) = pinned_peer_problem(expected, response.remote_addr()) {
				return Err!(BadServerResponse(warn!("{problem}")));
			},
		| None =>
			if let Some(remote_addr) = response.remote_addr()
				&& !self
					.services
					.client
					.valid_cidr_range_ip(remote_addr.ip())
				&& !self.services.client.proxied(response.url())
			{
				return Err!(BadServerResponse("Not allowed to send requests to this IP"));
			},
	}

	let status = response.status();
	let mut http_response_builder = HttpResponse::builder()
		.status(status)
		.version(response.version());

	swap(
		response.headers_mut(),
		http_response_builder
			.headers_mut()
			.expect("http::response::Builder is usable"),
	);

	let limit = self.services.config.max_response_size;
	let body = read_response_capped(response, limit).await?;

	if !status.is_success() {
		debug_warn!(body = ?string_from_bytes(&body), "Push gateway response");
		return Err!(BadServerResponse(warn!(
			"Push gateway {dest} returned unsuccessful HTTP response: {status}"
		)));
	}

	let response = T::IncomingResponse::try_from_http_response(
		http_response_builder
			.body(body)
			.expect("reqwest body is valid http body"),
	);

	response.map_err(|e| {
		err!(BadServerResponse(warn!("Push gateway {dest} returned invalid response: {e}")))
	})
}

/// Why a protected response's peer is not the pinned one.
///
/// Neither branch is reachable through configuration — the pin decides where
/// the connection goes, and startup refuses a pin whose port could diverge from
/// the URL's — so this is separated out to be tested directly rather than left
/// as an assertion nothing exercises.
///
/// Absent is a failure, not a pass. `remote_addr` is `None` for a proxied or
/// otherwise indirect connection, and the protected client is built with no
/// proxy: not knowing the peer means the request did not take the route this
/// pin promises.
fn pinned_peer_problem(expected: SocketAddr, remote: Option<SocketAddr>) -> Option<String> {
	match remote {
		| None => Some(
			"The protected push gateway response has no peer address; the request did not \
			 take the pinned route"
				.to_owned(),
		),
		| Some(remote) if remote != expected => Some(format!(
			"The protected push gateway responded from {remote}, not the pinned {expected}"
		)),
		| Some(_) => None,
	}
}

fn handler_err<R>(dest: &str, error: ReqwestError) -> Result<R> {
	warn!(%dest, chain = %error_chain(&error), "Could not send request to pusher");
	Err(error.into())
}

#[cfg(test)]
mod tests {
	use std::net::SocketAddr;

	use super::pinned_peer_problem;

	fn addr(s: &str) -> SocketAddr { s.parse().expect("test address parses") }

	#[test]
	fn the_pinned_peer_passes() {
		assert!(
			pinned_peer_problem(addr("127.0.0.1:3101"), Some(addr("127.0.0.1:3101")))
				.is_none()
		);
	}

	#[test]
	fn a_different_peer_is_refused() {
		let problem = pinned_peer_problem(addr("127.0.0.1:3101"), Some(addr("127.0.0.2:3101")))
			.expect("a peer that is not the pin is refused");

		assert!(problem.contains("not the pinned"), "{problem}");
	}

	#[test]
	fn the_same_address_on_another_port_is_refused() {
		let problem = pinned_peer_problem(addr("127.0.0.1:3101"), Some(addr("127.0.0.1:3102")))
			.expect("a peer on another port is refused");

		assert!(problem.contains("not the pinned"), "{problem}");
	}

	#[test]
	fn an_absent_peer_is_refused_rather_than_waved_through() {
		// The direction that matters. Treating "unknown" as acceptable is how
		// a check like this quietly stops checking.
		let problem = pinned_peer_problem(addr("127.0.0.1:3101"), None)
			.expect("an unknown peer is refused");

		assert!(problem.contains("no peer address"), "{problem}");
	}
}
