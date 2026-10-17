//! `POST /_matrix/client/unstable/com.chat-harness/activity`
//!
//! A client reports the room its user is actively viewing on this device (a
//! heartbeat), or that it views none (a clear). While the account has active
//! device suppression on, the push guard holds alerts for that room on the
//! account's other devices. The account and device come from the access
//! token, never from the body, so a report can only ever quiet its own
//! account's other devices.

use axum::extract::State;
use tuwunel_core::Result;

use crate::Ruma;

/// The endpoint's types, implemented by hand: ruma's request macros assume
/// the crate defines ruma's `client`/`server` features.
pub(crate) mod activity {
	use bytes::BufMut;
	use ruma::{
		OwnedRoomId,
		api::{
			IncomingRequest, OutgoingResponse,
			auth_scheme::AccessToken,
			error::{FromHttpRequestError, IntoHttpError},
			metadata,
		},
	};
	use serde::{Deserialize, Serialize};

	metadata! {
		method: POST,
		rate_limited: false,
		authentication: AccessToken,
		path: "/_matrix/client/unstable/com.chat-harness/activity",
	}

	#[derive(Debug, Deserialize)]
	pub(crate) struct Request {
		/// The room viewed, or `None` (sent as `null`) for a clear.
		pub(crate) room_id: Option<OwnedRoomId>,

		/// Orders this device's reports; a clear repeats the last one issued.
		pub(crate) seq: u64,
	}

	#[derive(Debug, Serialize)]
	pub(crate) struct Response {
		/// How long the heartbeat counts unless renewed.
		pub(crate) ttl_ms: u64,
	}

	impl IncomingRequest for Request {
		type EndpointError = ruma::api::error::Error;
		type OutgoingResponse = Response;

		fn try_from_http_request<B, S>(
			request: http::Request<B>,
			_path_args: &[S],
		) -> Result<Self, FromHttpRequestError>
		where
			B: AsRef<[u8]>,
			S: AsRef<str>,
		{
			Self::check_request_method(request.method())?;

			Ok(serde_json::from_slice(request.body().as_ref())?)
		}
	}

	impl OutgoingResponse for Response {
		fn try_into_http_response<T: Default + BufMut>(
			self,
		) -> Result<http::Response<T>, IntoHttpError> {
			let mut body = T::default();
			body.put_slice(&serde_json::to_vec(&self)?);

			Ok(http::Response::builder()
				.header(http::header::CONTENT_TYPE, "application/json")
				.body(body)?)
		}
	}
}

/// # `POST /_matrix/client/unstable/com.chat-harness/activity`
///
/// Record which room this device is viewing, for the push guard.
pub(crate) async fn notify_activity_route(
	State(services): State<crate::State>,
	body: Ruma<activity::Request>,
) -> Result<activity::Response> {
	let ttl_ms = services.pusher.report_activity(
		body.sender_user(),
		body.sender_device()?,
		body.room_id.as_deref(),
		body.seq,
	);

	Ok(activity::Response { ttl_ms })
}
