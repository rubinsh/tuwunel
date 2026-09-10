#![cfg(test)]

//! The protected push gateway route, against a real TLS server.
//!
//! The property under test is not "a resolver returns an address". It is that
//! a homeserver whose `ip_range_denylist` denies loopback — the shipped default,
//! and the reason a co-located gateway is unreachable in the first place —
//! delivers a notification to exactly one configured URL at a pinned loopback
//! address, over a connection whose certificate it actually validated, and to
//! nothing else.
//!
//! So the gateway here is a real `rustls` listener holding a certificate issued
//! by a CA generated in this harness, and the homeserver is given that CA
//! through configuration. Nothing is stubbed between the push service and the
//! socket.
//!
//! The negative cases share the boot. Each is a pusher pointed at a URL that
//! differs from the configured one in exactly one way — path, port, scheme —
//! and each must fail to arrive. They matter more than the positive case: a
//! matcher that is too generous is the failure that hands a denylist exemption
//! to a URL nobody reviewed, and the positive case passes either way.

use std::{
	env::var,
	fs::{remove_dir_all, remove_file, write},
	net::{SocketAddr, TcpListener as StdTcpListener},
	path::PathBuf,
	process::id as process_id,
	sync::Arc,
	time::Duration,
};

use serde_json::Value;
use tokio::{
	net::TcpListener,
	spawn,
	sync::mpsc::{error::TryRecvError, unbounded_channel},
	time::{sleep, timeout},
};
use tokio_rustls::TlsAcceptor;
use tuwunel::{Args, Runtime, Server, async_run, async_stop};
use tuwunel_core::{Err, Result, err, ruma::UserId, ruma::device_id};
use tuwunel_service::Services;

mod protected_gateway;

use protected_gateway::{
	AbortOnDrop, CaptureRx, GATEWAY_HOST, NOTIFY_PATH, pki, pusher_action, tls_gateway,
};

/// The exact positive case, plus every near-miss that must not inherit the pin.
#[test]
fn pusher_protected_route() -> Result {
	let pki = pki(GATEWAY_HOST)?;

	// The listener binds first: the port has to be in the configuration the
	// server boots with, and asking the kernel for a free one is the only way
	// to avoid a fixed port that collides under a parallel test run.
	//
	// Bound through `std` rather than tokio, because a tokio listener is
	// registered with the runtime that created it. Binding on a throwaway
	// runtime and dropping it leaves a socket that accepts nothing, and the
	// symptom is a connection refused that reads exactly like the route
	// failing.
	let listener = StdTcpListener::bind("127.0.0.1:0")?;
	listener.set_nonblocking(true)?;
	let addr: SocketAddr = listener.local_addr()?;
	let port = addr.port();

	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let root = PathBuf::from(root);
	let db_path = root.join(format!("tuwunel-test-protected-route-{}", process_id()));
	let ca_path = root.join(format!("tuwunel-test-protected-ca-{}.pem", process_id()));
	write(&ca_path, pki.ca_pem.as_bytes())?;

	let gateway_url = format!("https://{GATEWAY_HOST}:{port}{NOTIFY_PATH}");

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option
		.push(format!("database_path={db_path:?}"));

	// Deliberately NOT clearing ip_range_denylist. The shipped default denies
	// 127.0.0.0/8, and a test that cleared it would prove the route works in a
	// deployment that never needed it.
	args.option
		.push(format!("pusher_protected_gateway_url={gateway_url:?}"));
	args.option
		.push(format!("pusher_protected_gateway_addr=\"127.0.0.1:{port}\""));
	args.option
		.push(format!("pusher_protected_gateway_ca={ca_path:?}"));
	args.option
		.push("startup_netburst=true".to_owned());
	args.option
		.push("suppress_push_when_active=true".to_owned());
	args.option
		.push("sender_retry_backoff_limit=1".to_owned());

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result: Result = runtime.block_on(async {
		let services = Services::build(server.server.clone()).await?;
		let services = services.start().await?;
		_ = server
			.services
			.lock()
			.await
			.insert(services.clone());

		let (tx, mut rx) = unbounded_channel();
		let listener = TcpListener::from_std(listener)?;
		let acceptor = TlsAcceptor::from(Arc::new(pki.server));
		let _stub = AbortOnDrop(spawn(tls_gateway(listener, acceptor, tx)));

		let outcome = async {
			verify_pinned_delivery(&services, &gateway_url, &mut rx).await?;
			verify_near_misses(&services, port, &mut rx).await
		}
		.await;

		server.server.shutdown()?;
		drop(services);

		async_run(&server).await?;
		async_stop(&server).await?;

		outcome
	});

	drop(runtime);
	remove_dir_all(&db_path).ok();
	remove_file(&ca_path).ok();

	result
}

/// The configured URL is delivered, over a validated TLS connection, to the
/// pinned address.
async fn verify_pinned_delivery(
	services: &Services,
	gateway_url: &str,
	rx: &mut CaptureRx,
) -> Result {
	let user = UserId::parse_with_server_name("pinned", services.globals.server_name())?;

	services
		.pusher
		.set_pusher(&user, device_id!("PINNED"), &pusher_action("pk-pinned", gateway_url))
		.await?;

	services.sending.refresh_push_badge(&user).await?;

	let (path, body) = timeout(Duration::from_secs(20), rx.recv())
		.await
		.map_err(|_| {
			err!(
				"the pinned notification never arrived; the route did not deliver over TLS \
				 to the pinned address"
			)
		})?
		.ok_or_else(|| err!("the stub gateway closed"))?;

	if path != NOTIFY_PATH {
		return Err!("the pinned notification hit {path}, not {NOTIFY_PATH}");
	}

	// The body proves this is the real push path rather than any request that
	// happened to reach the listener.
	let body: Value = serde_json::from_slice(&body)
		.map_err(|e| err!("the pinned notification body was not json: {e}"))?;

	if body.get("notification").is_none() {
		return Err!("the pinned notification body carried no notification: {body}");
	}

	Ok(())
}

/// Each of these differs from the configured URL in exactly one way and must
/// stay on the ordinary client, where the denylist refuses loopback.
async fn verify_near_misses(services: &Services, port: u16, rx: &mut CaptureRx) -> Result {
	let cases = [
		// A hostname-level override cannot express this one: same host, same
		// port, a different endpoint. It is the case the whole URL-matching
		// design exists for.
		("wrong-path", format!("https://{GATEWAY_HOST}:{port}/_matrix/push/v1/other")),
		// reqwest's own resolution overrides are hostname-wide, so a route
		// declared for one port would otherwise pin every port on that name.
		(
			"wrong-port",
			format!("https://{GATEWAY_HOST}:{}{NOTIFY_PATH}", port.wrapping_add(1)),
		),
		// The pin's guarantee is the certificate check. Plaintext to the same
		// place must not inherit it.
		("plaintext", format!("http://{GATEWAY_HOST}:{port}{NOTIFY_PATH}")),
	];

	for (name, url) in cases {
		let user = UserId::parse_with_server_name(name, services.globals.server_name())?;
		let device = device_id!("NEARMISS");

		services
			.pusher
			.set_pusher(&user, device, &pusher_action(&format!("pk-{name}"), &url))
			.await?;

		services.sending.refresh_push_badge(&user).await?;
	}

	// One wait covers all three: any of them reaching the listener is a
	// failure, and they were all queued before it started.
	sleep(Duration::from_secs(5)).await;

	match rx.try_recv() {
		| Err(TryRecvError::Empty) => Ok(()),
		| Err(TryRecvError::Disconnected) => Err!("the stub gateway closed"),
		| Ok((path, _)) => Err!(
			"a near-miss URL reached the gateway at {path}; it inherited the protected \
			 route instead of being refused by the denylist"
		),
	}
}
