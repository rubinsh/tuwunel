use axum::{Json, extract::State, response::IntoResponse};
use bytes::BytesMut;
use ruma::api::{
	OutgoingResponse,
	client::discovery::{
		discover_homeserver::{self, HomeserverInfo},
		discover_support::{self},
	},
};
use serde_json::Value;
use tuwunel_core::{Err, Result, err};

use crate::Ruma;

/// The vendor key carrying the advertised push gateway.
///
/// Namespaced to this project rather than `m.` or `org.matrix.`: no MSC has
/// been allocated, and claiming an unallocated Matrix.org name is the one thing
/// here that would obstruct upstreaming. It becomes `org.matrix.mscNNNN.*` if
/// and when a number exists.
const PUSH_GATEWAY_KEY: &str = "com.chat-harness.push_gateway.url";

/// # `GET /.well-known/matrix/client`
///
/// Returns the .well-known URL if it is configured, otherwise returns 404.
/// Also includes RTC transport configuration for Element Call (MSC4143), and
/// the push gateway when an operator advertises one.
///
/// This is a plain route rather than a `ruma_route` because the response
/// carries a key ruma's fixed `Response` has no field for. The standard fields
/// are *not* restated here: ruma builds the response exactly as before and its
/// serialized body is what gets the extra key added, so a ruma upgrade that
/// changes `m.homeserver`, `m.identity_server` or `rtc_foci` flows straight
/// through instead of drifting against a hand-written copy.
pub(crate) async fn well_known_client(
	State(services): State<crate::State>,
) -> Result<impl IntoResponse> {
	let homeserver = HomeserverInfo {
		base_url: match services.config.well_known.client.as_ref() {
			| Some(url) => url.to_string(),
			| None => return Err!(Request(NotFound("Not found."))),
		},
	};

	let rtc_foci = services.config.well_known.get_transports()?;

	let response = discover_homeserver::Response {
		rtc_foci,
		..discover_homeserver::Response::new(homeserver)
	};

	let body = response
		.try_into_http_response::<BytesMut>()
		.map_err(|e| err!(error!("Failed to serialize the client discovery response: {e}")))?
		.into_body()
		.freeze();

	let mut value: Value = serde_json::from_slice(&body)
		.map_err(|e| err!(error!("The client discovery response was not JSON: {e}")))?;

	if let Some(gateway) = services.config.well_known.push_gateway.as_ref() {
		let object = value.as_object_mut().ok_or_else(|| {
			err!(error!("The client discovery response was not a JSON object"))
		})?;

		object.insert(PUSH_GATEWAY_KEY.into(), Value::String(gateway.to_string()));
	}

	Ok(Json(value))
}

/// # `GET /.well-known/matrix/support`
///
/// Server support contact and support page of a homeserver's domain.
pub(crate) async fn well_known_support(
	State(services): State<crate::State>,
	_body: Ruma<discover_support::Request>,
) -> Result<discover_support::Response> {
	let config = &services.config.well_known;

	let support_page = config
		.support_page
		.as_ref()
		.map(ToString::to_string);

	let contacts = config.get_contacts();

	let policies = config.get_policies();

	if support_page.is_none() && contacts.is_empty() && policies.is_empty() {
		return Err!(Request(NotFound("Not found.")));
	}

	Ok(discover_support::Response { contacts, support_page, policies })
}
