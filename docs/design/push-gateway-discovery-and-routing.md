# Push gateway discovery and restricted routing — as built

Status: **implemented**. This supersedes the proposed design reviewed on
rubinsh/tuwunel#2; where the two disagree, this file is what the code does.

Two capabilities, deliberately separable. Either can ship without the other,
neither changes behaviour for a deployment that does not configure it, and both
ship *off*.

1. **Discovery publication** — advertise an operator-configured push gateway to
   clients through `/.well-known/matrix/client`.
2. **Restricted routing** — let the pusher reach exactly one configured gateway
   URL at a pinned address, without widening `ip_range_denylist` for anything
   else.

---

## What changed from the proposal, and why

The proposal put the exemption in DNS resolution — an origin → address map
consulted by the pusher client's resolver. Review established that this cannot
express what it promises, for three reasons that are properties of the
surrounding code rather than matters of taste:

**A resolver cannot see the port or the path.** `reqwest::dns::Resolve` receives
a `Name` and nothing else, and reqwest documents that the port in the URL always
overrides the port in a resolved `SocketAddr`. A route declared for
`gateway.example:3101` would therefore pin that hostname on *every* URL port and
*every* path. The proposed "wrong port misses" test could not have passed as
written.

**There is a post-connect check the proposal did not account for.**
`src/service/pusher/request.rs::handle_ok` rejects a direct response whose
`remote_addr()` falls in the CIDR denylist. A resolver-level pin to loopback
would therefore have been rejected *after* the request was already sent — the
notification lost, the data already delivered.

**TLS was necessary but not sufficient under the existing client.** `base()`
applies the configured proxy and honours `allow_invalid_tls_certificates`.
Either would defeat a direct, hostname-validated pin, and neither was addressed.

So the exemption moved from the resolver to the **request**, keyed on the final
normalized notification URL, and onto a dedicated client that cannot be proxied,
cannot skip validation and cannot redirect.

---

## Capability 1 — discovery publication

```toml
[global.well_known]
push_gateway = "https://gateway.example.com:3101/_matrix/push/v1/notify"
```

`Option<Url>` on `WellKnownConfig`, absent by default. When absent the response
is byte-identical to today's.

Published as **`com.chat-harness.push_gateway.url`** — a vendor namespace this
project controls. The proposal used `org.matrix.msc_unstable.push_gateway`,
which claims the Matrix.org namespace without an allocated MSC; that is the one
thing here that would obstruct upstreaming. It becomes `org.matrix.mscNNNN.*` if
and when a number exists.

Startup validates the URL is absolute HTTPS with a **domain** host, carries no
credentials, query or fragment, and that its path equals this server's
`notification_push_path`. Publishing a path clients cannot notify is worse than
publishing nothing: discovery succeeds and every notification is lost.

### The response type

`well_known_client` moved off `ruma_route` to a plain axum route. ruma's
`Response` has a fixed field set and does not implement `Serialize`, so
`#[serde(flatten)]` over it is not available either.

It does **not** hand-copy the standard fields. ruma builds its response exactly
as before, that response is serialized through its own `OutgoingResponse`, and
the vendor key is added to the resulting object. A ruma upgrade that changes
`m.homeserver`, `m.identity_server` or `rtc_foci` flows straight through instead
of drifting against a copy. CORS and the other response layers are global, so
the plain route keeps them.

---

## Capability 2 — restricted routing

```toml
[global]
pusher_protected_gateway_url  = "https://gateway.example.com:3101/_matrix/push/v1/notify"
pusher_protected_gateway_addr = "127.0.0.1:3101"
pusher_protected_gateway_ca   = "/etc/tuwunel/gateway-ca.pem"   # optional
```

One optional route, not a list. The proposal configured a plural list while
describing "exactly one"; plural broadens parser, duplicate and conflict
semantics for a requirement nobody has.

### Selection

`send_request` builds the final reqwest request, then compares its URL to the
configured one. Equality is over `Url`'s normalized serialization — scheme and
host lowercased at parse, default port dropped. A match uses the protected
client and carries the pinned address forward; **everything else, including a
different path or port on the same host, uses today's ordinary client
unchanged.**

This is the whole reason selection is not in the resolver: a hostname match
cannot distinguish `/_matrix/push/v1/notify` from `/_matrix/push/v1/anything`,
and the exemption must not extend to a URL nobody reviewed.

### The protected client

Built from `base()` and then narrowed:

| | why |
|---|---|
| `.no_proxy()` | a proxied request never reaches the pin, and its peer is the proxy |
| `.danger_accept_invalid_certs(false)` | `base()` honours the global escape hatch; the pin's protection *is* the certificate check |
| `.redirect(Policy::none())` | a hop away from the configured URL would leave this client pointed somewhere nobody configured, still exempt |
| `.resolve(host, addr)` | **replacement, not permission** — DNS is never consulted for this name on this client, so rebinding and a poisoned cache do not apply |
| `Validating` underneath | the pinned name never reaches it; anything else this client were ever asked for stays denylist-filtered |
| `add_root_certificate` | optional private CA for the gateway, visible to this client alone |

### Post-connect

For the protected route the peer must be **present and exactly the pin**. The
ordinary denylist check cannot express this — the pinned address is precisely
one the denylist forbids — so the two are separate branches. Absent is a
failure, not a pass: `remote_addr` is `None` for an indirect connection, and
this client has no proxy, so not knowing the peer means the request did not take
the promised route.

### Startup validation

Refuses every configuration that would otherwise lie: half a route (URL without
address or vice versa), a trust anchor without a route, a non-HTTPS URL, an
IP-literal host, credentials, a query or a fragment, a pinned port differing
from the URL's effective port, and a route configured while
`allow_invalid_tls_certificates` is on.

The port rule deserves its own line: reqwest connects to the URL's port and
ignores the override's, so a mismatch would silently do nothing and then fail
every notification at the peer check. Startup refuses it rather than letting the
configuration read as something it is not.

`check_http_pusher_url` is **unchanged**, stated here so its absence is reviewed
rather than assumed. The gateway URL has a hostname, so `valid_cidr_range_url`
already returns `true` for it; an exemption there would relax a check that never
gated the gateway.

### Why TLS validation is load-bearing — and its limit

The pinned address is a loopback port, and loopback ports are not owned. If the
gateway process dies, another local process can bind it and start receiving room
IDs, event IDs and unread counts. Certificate validation against the gateway's
*name* is what closes that: a squatter cannot present a valid certificate for a
name it does not control, so the connection fails closed.

The limit, stated because the proposal overclaimed it: this stops a squatter
**without the key**. A same-UID process that can read the gateway's private key
can present the valid certificate. Private-key isolation is a deployment
invariant, not something the homeserver can enforce, and no test here can prove
it.

An IP-literal origin stays unsupported. TLS *can* authenticate one through an IP
SAN — the proposal's "no hostname, therefore no validation" was not accurate —
but an address literal moves the trust onto whoever holds the address, which is
the property the loopback pin exists to avoid.

---

## Touched files

| File | Change |
|---|---|
| `src/core/config/mod.rs` | `well_known.push_gateway`; `pusher_protected_gateway_{url,addr,ca}` |
| `src/core/config/check.rs` | `check_push_gateway` + shared URL shape checks |
| `src/api/client/well_known.rs` | plain route; vendor key added to ruma's own serialized response |
| `src/api/router.rs` | `ruma_route` → `route` for `/.well-known/matrix/client` |
| `src/service/client/mod.rs` | `pusher_protected` client, `ProtectedGateway` snapshot, URL matcher |
| `src/service/pusher/request.rs` | client selection by final URL; pinned-peer check |
| `src/service/pusher/mod.rs` | **no change** |

## Tests

**Acceptance, against a real TLS server** — `src/main/tests/pusher_protected_route.rs`
boots a homeserver with `ip_range_denylist` at its **shipped default** (loopback
denied — the condition that makes a co-located gateway unreachable in the first
place), a `rustls` listener on loopback holding a certificate issued by a CA
generated in the harness, and the route pinned to it. The notification must
arrive, with the expected path and a real notification body.

Its near-miss controls share the boot: a pusher at the same host and port but a
different path, one at a different port, and one over plaintext. None may reach
the listener. These matter more than the positive case — a matcher that is too
generous is the failure that hands a denylist exemption to an unreviewed URL,
and the positive case passes either way.

**The port-squatter control** — `src/main/tests/pusher_protected_route_squat.rs`
is the same boot with exactly one thing changed: the listener holds a
certificate for a *different* name, issued by the same trusted CA. So the chain
validates and the connection must still fail, on the name alone. Verified
non-vacuous: issuing that certificate for the gateway's own name makes the test
fail, so an empty channel really is the handshake being refused rather than the
request never being sent.

**Unit** — the URL matcher (8 cases: wrong path, wrong port, suffix host,
plaintext, credentials, query, case-folding, unconfigured), configuration
validation (12 cases, one per refusal above plus both-off-by-default), and the
pinned-peer check (4 cases including absent).

The peer-mismatch and peer-absent branches are **not reachable through
configuration** — the pin decides where the connection goes, and startup refuses
a pin whose port could diverge. That is a property of the design rather than a
gap, and it is why those two branches are covered directly by unit test instead
of through the live gate.

## Scope

Code, tests and review only. No deployment, no unit installation, no homeserver
configuration change, no production traffic. The shipped defaults leave both
capabilities off, so building this changes nothing until an operator configures
it.
