#![cfg(test)]

//! The port-squatter control: a pinned address is not an authenticated one.
//!
//! The protected route's whole guarantee is that certificate validation still
//! runs. A pinned loopback port is not owned by anyone — if the gateway process
//! dies, any local process can bind it and start receiving room IDs, event IDs
//! and unread counts. What stops that is the homeserver checking a certificate
//! for the gateway's *name*, which a squatter without the key cannot present.
//!
//! (The invariant that holds it up is deployment-side: the gateway's private
//! key must not be readable by other processes. A same-UID process with the key
//! can present the valid certificate, and no test here can say otherwise. This
//! covers the squatter without the key, which is what the code can enforce.)
//!
//! So this boot is the positive test with exactly one thing changed: the
//! listener holds a certificate issued for another name. Everything else — the
//! route, the pin, the trusted CA, the denylist — is identical. The
//! notification must not arrive.

use std::{
	env::var,
	fs::{remove_dir_all, remove_file, write},
	net::{SocketAddr, TcpListener as StdTcpListener},
	path::PathBuf,
	process::id as process_id,
	sync::Arc,
	time::Duration,
};

use tokio::{
	net::TcpListener,
	spawn,
	sync::mpsc::{error::TryRecvError, unbounded_channel},
	time::sleep,
};
use tokio_rustls::TlsAcceptor;
use tuwunel::{Args, Runtime, Server, async_run, async_stop};
use tuwunel_core::{Err, Result, ruma::UserId, ruma::device_id};
use tuwunel_service::Services;

mod protected_gateway;

use protected_gateway::{
	AbortOnDrop, GATEWAY_HOST, NOTIFY_PATH, pki, pusher_action, tls_gateway,
};

/// The name on the certificate the squatter presents. Its CA is the one the
/// homeserver trusts, which makes this a *stronger* control than an untrusted
/// issuer: the chain validates and the connection must still fail, on the name
/// alone.
const SQUATTER_HOST: &str = "squatter.test.invalid";

#[test]
fn pusher_protected_route_wrong_certificate_name() -> Result {
	let pki = pki(SQUATTER_HOST)?;

	let listener = StdTcpListener::bind("127.0.0.1:0")?;
	listener.set_nonblocking(true)?;
	let addr: SocketAddr = listener.local_addr()?;
	let port = addr.port();

	let root = var("TMPDIR").unwrap_or_else(|_| "/nvme/target/tmp".into());
	let root = PathBuf::from(root);
	let db_path = root.join(format!("tuwunel-test-protected-squat-{}", process_id()));
	let ca_path = root.join(format!("tuwunel-test-protected-squat-ca-{}.pem", process_id()));
	write(&ca_path, pki.ca_pem.as_bytes())?;

	// The route names the gateway. The listener answers as someone else.
	let gateway_url = format!("https://{GATEWAY_HOST}:{port}{NOTIFY_PATH}");

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option
		.push(format!("database_path={db_path:?}"));
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
			let user =
				UserId::parse_with_server_name("squatted", services.globals.server_name())?;

			services
				.pusher
				.set_pusher(
					&user,
					device_id!("SQUATTED"),
					&pusher_action("pk-squatted", &gateway_url),
				)
				.await?;

			services.sending.refresh_push_badge(&user).await?;

			sleep(Duration::from_secs(5)).await;

			// `tls_gateway` only reports a request after a completed handshake,
			// so an empty channel is the handshake having failed — which is the
			// property under test. The listener did accept the TCP connection;
			// what it could not do is prove it was the gateway.
			match rx.try_recv() {
				| Err(TryRecvError::Empty) => Ok(()),
				| Err(TryRecvError::Disconnected) => Err!("the stub gateway closed"),
				| Ok((path, _)) => Err!(
					"a notification reached a gateway presenting a certificate for \
					 {SQUATTER_HOST} at {path}; the pinned route accepted a peer it could \
					 not authenticate"
				),
			}
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
