# Push gateway discovery and restricted routing — design

Status: **proposed**. Design for review; no implementation in this branch.

Two capabilities, deliberately separable. Either can ship without the other, and
neither changes behaviour for a deployment that does not configure it.

1. **Discovery publication** — advertise an operator-configured push gateway to
   clients through `/.well-known/matrix/client`.
2. **Restricted routing** — let the pusher client reach exactly one configured
   gateway origin at a pinned address, without widening `ip_range_denylist` for
   anything else.

Both are shaped as generic server capabilities, because the fork's goal is
upstreaming. Nothing here names a specific host, network or deployment: the
effective configuration is operator input, and the shipped default is *off*.

---

## The problem, stated precisely

A push gateway co-located with the homeserver is unreachable from it today, and
that is not an accident of configuration — it is the default denylist working
as intended:

```
src/core/config/mod.rs  default_ip_range_denylist
    "127.0.0.0/8"        loopback
    "100.64.0.0/10"      CGNAT — the range Tailscale and similar overlays use
```

The pusher client resolves through `Validating` (`src/service/resolver/dns.rs`),
which filters every resolved address against that denylist and fails closed when
none survives:

```rust
if filtered.peek().is_none() {
    return Err(Box::new(io::Error::new(
        PermissionDenied,
        "All resolved addresses are denied by ip_range_denylist",
    )));
}
```

So a gateway on loopback, or on an overlay address, cannot be pushed to. Turning
the denylist entries off would restore reachability by removing the protection
for **every** host the server talks to, which is the outcome this design exists
to avoid.

### What the existing chain actually checks

Worth stating exactly, because two of the three checks are weaker than they look
and the design must not lean on them.

| Stage | Code | What it checks |
|---|---|---|
| Registration | `check_http_pusher_url` (`src/service/pusher/mod.rs`) | scheme is http/https; not a proxy alias; `valid_cidr_range_url` |
| Resolution | `Validating::resolve` (`src/service/resolver/dns.rs`) | every resolved address against the denylist; `proxy_hosts` bypasses entirely |
| Redirect | `guarded_redirect(services, 2)` (`src/service/client/mod.rs`) | proxy alias and `valid_cidr_range_url` on each hop |

`valid_cidr_range_url` inspects **IP literals only**:

```rust
| Some(Host::Domain(_)) | None => return true,
```

A hostname therefore passes registration and every redirect check regardless of
what it resolves to. The resolver is the only stage that sees addresses, which
means **the resolver is the whole boundary**. A design that adds an exemption
anywhere else is adding it to a check that was never load-bearing.

---

## Capability 1 — discovery publication

### Config

```
[global.well_known]
push_gateway = "https://gateway.example.com:3101/_matrix/push/v1/notify"
```

`Option<Url>` on `WellKnownConfig`, alongside the existing `client` and `server`.
Absent by default. When absent, the response is byte-identical to today's.

### Response

`GET /.well-known/matrix/client` gains one key. Standard fields are untouched:
`m.homeserver` keeps its meaning and `org.matrix.msc4143.rtc_foci` keeps being
emitted by `get_transports()`.

The key is namespaced as unstable until an MSC exists for it. Proposed:
`org.matrix.msc_unstable.push_gateway`, replaced by the stable name if and when
one is assigned. Publishing an unnamespaced `m.` key for a field with no MSC
would be the one thing here that actively obstructs upstreaming.

### Open question — ruma's response type

`well_known_client` returns `ruma::api::client::discovery::discover_homeserver::Response`,
whose fields are fixed. Two ways to add a key, and I would like the reviewer's
preference before implementation:

- **(a) Custom response type.** Replace the ruma `Response` in this one handler
  with a local struct that serializes `m.homeserver`, `rtc_foci` and the new
  key. Self-contained, no dependency work, but it hand-rolls a wire shape that
  ruma currently guarantees, and drift becomes possible.
- **(b) Extend ruma.** Correct long-term and required for upstream anyway, but
  it puts an external dependency on the critical path of a local change.

My recommendation is **(a) for the fork, (b) as the upstream path**, with the
custom struct written so its serialization is asserted field-for-field against
the ruma type in a test — so drift fails CI rather than reaching clients.

---

## Capability 2 — restricted routing

### The shape

An operator-configured mapping from an exact origin to an exact socket address,
consulted **only** by the pusher client:

```
[global]
pusher_gateway_routes = ["gateway.example.com:3101=127.0.0.1:3101"]
```

Semantics, each chosen to keep the exemption as narrow as it can be:

- **Exact origin match.** Host compared ASCII-case-insensitively, port compared
  numerically, both required. No wildcards, no suffix matching, no bare-host
  entries. A suffix rule would let a name nobody reviewed inherit the exemption.
- **Replacement, not permission.** For a matching origin the resolver returns
  *exactly* the configured address and does not consult DNS at all. This is
  strictly stronger than an allowlist: it does not matter what DNS answers, so
  DNS rebinding, a poisoned cache and a compromised resolver are all out of the
  picture rather than mitigated.
- **Pusher client only.** Federation, appservice, URL-preview and OAuth clients
  are untouched. The override is wired where the pusher client is built
  (`src/service/client/mod.rs:175`), not into the shared resolver.
- **TLS is not relaxed.** Ordinary hostname verification against the ordinary CA
  store, for the origin's hostname. See below — this is load-bearing, not a
  courtesy.
- **Redirects get no exemption.** A hop away from the pinned origin goes through
  `guarded_redirect` and the normal resolver exactly as today. A hop *to* the
  pinned origin resolves to the pinned address like any other request.

### Why not reuse `proxy_hosts`

`Validating::resolve` already has an exemption path:

```rust
if self.proxy_hosts.iter().any(|host| host.eq_ignore_ascii_case(name.as_str())) {
    return self.inner.resolve(name);
}
```

It is the wrong tool twice over. It is a **full bypass** — the host resolves
wherever DNS says, unvalidated, so the protection is removed rather than
narrowed. And it means "this host is reached through the proxy", so overloading
it would make the proxy configuration silently control push egress. A reviewer
reading `proxy_hosts` later would have no reason to think push routing depended
on it.

### Why TLS validation is load-bearing

The pinned address is a loopback port, and loopback ports are not owned. If the
gateway process dies, any local process can bind that port and start receiving
whatever the homeserver pushes — room IDs, event IDs, unread counts.

Ordinary TLS validation is what closes that, and it closes it completely: the
homeserver requests `https://gateway.example.com:3101/...` and verifies the
presented certificate against the CA store **for that hostname**. A squatter on
the port cannot present a valid certificate for a name it does not control, so
the connection fails closed and no data leaves.

This is the reason the design keeps hostname/CA validation rather than pinning
by IP and skipping it — and the reason "just use `http://127.0.0.1:3101`" is not
an acceptable simplification. It is also why the gateway must hold a real
certificate for a real name; `gateway-main.ts` already refuses to start without
a complete TLS pair and has no plaintext fallback.

### Registration

`check_http_pusher_url` needs **no change**, and I would rather it stayed that
way. The gateway URL is a hostname, so `valid_cidr_range_url` already returns
`true` for it. Adding an exemption here would relax a check that never gated the
gateway in the first place.

One consequence to accept deliberately: an operator cannot configure a route for
an **IP-literal** origin, because such a URL is refused at registration and the
design does not touch that. This is correct — an IP literal has no hostname to
validate a certificate against, which is exactly the property the previous
section depends on.

### Implementation question — reqwest's own override

`reqwest::ClientBuilder::resolve(domain, addr)` exists for precisely this and
would be less code than a wrapping resolver. But the pusher client already
installs a custom `dns_resolver`, and the precedence between the two is not
something I want to assume. The first implementation task is a test that settles
it: configure both, assert which one the connection uses. If `resolve()` wins,
use it (and the pinned route never reaches `Validating` at all). If the custom
resolver wins, wrap it.

Either way the wrapping resolver is the fallback, and it is small: match the
name against the route table before delegating, return the pinned address on a
hit. Note that a wrapping resolver sees the **hostname only** — reqwest resolves
by name, so the port must be matched from configuration rather than read off the
resolve call. That asymmetry is worth stating because it is where a
"host matches, wrong port" bug would live.

---

## Touched files

| File | Change |
|---|---|
| `src/core/config/mod.rs` | `well_known.push_gateway`; `pusher_gateway_routes` + parser + defaults (both empty/absent) |
| `src/api/client/well_known.rs` | publish the gateway key |
| `src/service/client/mod.rs` | wire the route table into the pusher client only |
| `src/service/resolver/dns.rs` | pinned-route resolution ahead of denylist filtering |
| `src/service/pusher/mod.rs` | **no change** — stated so its absence is reviewed, not assumed |
| `src/main/tests/pusher_notify.rs` | acceptance test (below) |
| `src/service/resolver/tests.rs` | unit tests for matching and precedence |

## Tests

Unit, on the route table:

- exact host+port hits; **wrong port on a matching host misses**; case-insensitive
  host match; unrelated host unaffected and still denylist-filtered;
- a malformed route entry is a **startup error**, not a skipped line — a silently
  dropped route is a gateway that stops receiving push with nothing in the log;
- an empty route table leaves resolution byte-identical to today's;
- a route does **not** leak to the federation, appservice or URL-preview clients:
  the same host resolves normally there and stays denylist-filtered.

Acceptance — the actual-homeserver-request gate:

`src/main/tests/pusher_notify.rs` already binds a real `TcpListener` on
`127.0.0.1:0` and drives the real push path, so the gate extends it rather than
inventing a harness. A **TLS** listener on loopback, a certificate for a test
hostname, a route pinning that hostname to the listener's address, then: register
a pusher at `https://<test-host>:<port>/_matrix/push/v1/notify`, trigger a
notification, and assert the request **arrives** with the expected path and body.

That is the gate ch-pi asked for: it proves the pin and TLS validation work
together against a real server, rather than proving a resolver returns an
address.

Two negative controls alongside it, because the positive test passes for the
wrong reason if either fails:

- **same request, no route configured** → must fail with the denylist error.
  Without this, a test that accidentally reaches the gateway some other way
  still passes.
- **route configured, certificate for the wrong name** → must fail TLS. This is
  the port-squatting scenario, and it is the one property the whole loopback pin
  depends on.

Open question: the test needs a CA the homeserver trusts. Preference between a
test-only trust anchor injected through configuration, or generating a cert
chain in the harness — I have not established which the existing test
infrastructure supports.

---

## Scope

Code, tests and independent review only. No deployment, no unit installation, no
homeserver configuration change, no production traffic. The shipped defaults
leave both capabilities off, so building this changes nothing until an operator
configures it.
