//! Shared harness for the protected push gateway route tests.
//!
//! Two boots need it: one where the gateway holds a certificate for the name
//! the route names, and one where it holds a certificate for another name. The
//! second is the port-squatter control, and it is only meaningful if it runs
//! against exactly the same machinery as the first.


use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use rustls::{ServerConfig, pki_types::PrivateKeyDer};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpListener,
	sync::mpsc::{UnboundedReceiver, UnboundedSender},
	task::JoinHandle,
};
use tokio_rustls::TlsAcceptor;
use tuwunel_core::{Result, err,
	ruma::{
		api::client::push::{
			Pusher, PusherIds, PusherInit, PusherKind,
			set_pusher::v3::{PusherAction, Request as SetPusherRequest},
		},
		push::{HttpPusherData, PushFormat},
	},
};

/// The gateway's name. `.invalid` is reserved and resolves nowhere, so a
/// request that reaches the listener can only have got there through the
/// configured pin — never through DNS that happened to work.
pub(crate) const GATEWAY_HOST: &str = "gateway.test.invalid";
pub(crate) const NOTIFY_PATH: &str = "/_matrix/push/v1/notify";
const APP_ID: &str = "app.tuwunel.test";

pub(crate) type Captured = (String, Vec<u8>);
// Used by the positive binary only; the squatter control asserts absence and
// never reads a request.
#[allow(dead_code)]
pub(crate) type CaptureRx = UnboundedReceiver<Captured>;
pub(crate) type CaptureTx = UnboundedSender<Captured>;

pub(crate) struct AbortOnDrop(pub(crate) JoinHandle<()>);

impl Drop for AbortOnDrop {
	fn drop(&mut self) { self.0.abort(); }
}

/// A CA, and a server certificate it issued for one name.
pub(crate) struct Pki {
	pub(crate) ca_pem: String,
	pub(crate) server: ServerConfig,
}

pub(crate) fn pki(server_name: &str) -> Result<Pki> {
	// Installed once per test binary; a second call is a no-op error we ignore.
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let ca_key = KeyPair::generate().map_err(|e| err!("test CA key: {e}"))?;
	let mut ca_params =
		CertificateParams::new(Vec::<String>::new()).map_err(|e| err!("test CA params: {e}"))?;
	ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
	ca_params
		.distinguished_name
		.push(DnType::CommonName, "Tuwunel push gateway test CA");

	let ca_cert = ca_params
		.self_signed(&ca_key)
		.map_err(|e| err!("test CA cert: {e}"))?;
	let ca_pem = ca_cert.pem();
	let issuer = Issuer::new(ca_params, ca_key);

	let leaf_key = KeyPair::generate().map_err(|e| err!("test leaf key: {e}"))?;
	let leaf_params = CertificateParams::new(vec![server_name.to_owned()])
		.map_err(|e| err!("test leaf params: {e}"))?;
	let leaf_cert = leaf_params
		.signed_by(&leaf_key, &issuer)
		.map_err(|e| err!("test leaf cert: {e}"))?;

	let server = ServerConfig::builder()
		.with_no_client_auth()
		.with_single_cert(
			vec![leaf_cert.der().clone(), ca_cert.der().clone()],
			PrivateKeyDer::Pkcs8(leaf_key.serialize_der().into()),
		)
		.map_err(|e| err!("test server TLS config: {e}"))?;

	Ok(Pki { ca_pem, server })
}


pub(crate) fn pusher_action(pushkey: &str, url: &str) -> PusherAction {
	let mut data = HttpPusherData::new(url.to_owned());
	data.format = Some(PushFormat::EventIdOnly);

	let pusher: Pusher = PusherInit {
		ids: PusherIds::new(pushkey.to_owned(), APP_ID.to_owned()),
		kind: PusherKind::Http(data),
		app_display_name: "Tuwunel Test".into(),
		device_display_name: "Test Device".into(),
		profile_tag: None,
		lang: "en".into(),
	}
	.into();

	SetPusherRequest::post(pusher).action
}

/// A real TLS gateway: accepts, completes the handshake, and answers the push.
///
/// A handshake failure is dropped silently rather than reported, because that
/// is exactly what the wrong-certificate control expects to happen.
pub(crate) async fn tls_gateway(listener: TcpListener, acceptor: TlsAcceptor, tx: CaptureTx) {
	let response = format!(
		"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
		 15\r\nConnection: close\r\n\r\n{{\"rejected\":[]}}"
	);

	while let Ok((socket, _)) = listener.accept().await {
		let Ok(mut tls) = acceptor.accept(socket).await else {
			continue;
		};

		let Some((path, body)) = read_request(&mut tls).await else {
			continue;
		};

		if tx.send((path, body)).is_err() {
			return;
		}

		tls.write_all(response.as_bytes()).await.ok();
		tls.flush().await.ok();
		tls.shutdown().await.ok();
	}
}

async fn read_request<S>(socket: &mut S) -> Option<(String, Vec<u8>)>
where
	S: AsyncReadExt + Unpin,
{
	let mut buf = Vec::new();
	let mut chunk = [0_u8; 4096];
	loop {
		if let Some(head_end) = find(&buf, b"\r\n\r\n") {
			let content_length = content_length(&buf[..head_end])?;
			let body_start = head_end.checked_add(4)?;
			let body_end = body_start.checked_add(content_length)?;
			if buf.len() >= body_end {
				let path = request_path(&buf[..head_end])?;

				return Some((path, buf[body_start..body_end].to_vec()));
			}
		}

		let read = socket.read(&mut chunk).await.ok()?;
		if read == 0 {
			return None;
		}

		buf.extend_from_slice(&chunk[..read]);
	}
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
	haystack
		.windows(needle.len())
		.position(|window| window == needle)
}

fn request_path(head: &[u8]) -> Option<String> {
	let head = str::from_utf8(head).ok()?;
	let line = head.lines().next()?;

	line.split_whitespace().nth(1).map(ToOwned::to_owned)
}

fn content_length(head: &[u8]) -> Option<usize> {
	let head = str::from_utf8(head).ok()?;

	head.lines()
		.find_map(|line| {
			let (name, value) = line.split_once(':')?;
			name.trim()
				.eq_ignore_ascii_case("content-length")
				.then(|| value.trim().parse().ok())?
		})
		.or(Some(0))
}
