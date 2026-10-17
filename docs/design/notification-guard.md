# Notification guard

A Chat Harness extension in this fork. It decides, per pushed event and per
pusher, whether the push gateway is called at all. The full contract lives in
the notification guard spec (R10), which is shared with the client team. This
page covers how the server implements it.

## Where it runs

`send_push_notice` (`src/service/pusher/send.rs`), after the push rules decide
to notify and before `send_notice`. Every event push goes through it: live
sends, retries from the sending queue, federation invites, and the flush of
the pushes `suppress_push_when_active` deferred. These are never guarded:
badge-only pushes (`send_badge_notice`), unread and highlight counts, and the
push rules themselves.

A dropped push returns `Ok(())`, so the sending queue neither retries it nor
counts it as a failure. Each drop is logged at debug level with a reason code
and IDs only, never content.

## Rules, in order

The first rule that matches drops the push.

| Code | Drops when |
|---|---|
| `dropped_before` | this pusher already dropped this event (see *Memory*) |
| `viewed_elsewhere` | `activeDeviceSuppression` is on and another device of the account reported the room as active |
| `room_muted` | the room's effective level is `muted` |
| `room_snoozed` | the room's `snoozeUntil` is in the future |
| `thread_muted` | the event's thread root is muted, or all the room's threads are |
| `bot_filtered` | the level is `humans` and the sender is on the bot list or marks itself `sender_kind: "bot"` |
| `not_mentioned` | the level is `mentions` and `m.mentions.user_ids` lacks the user (body fallback only without `m.mentions`) |
| `paused` | `pauseUntil` is in the future; break-through does not apply |
| `quiet_hours` | a window covers now in the schedule's timezone, and neither room nor sender breaks through |
| `burst` | another event alerted from this room on this pusher inside the burst window |

The effective level is the room's own `level`. If the room has none, it is
`botRoomLevel` when that is set and every other joined member is on the bot
list; otherwise it is `accountLevel`. Rooms with more than 64 other members are
never bot rooms, which bounds the member scan.

All times are judged when the guard decides, not by `origin_server_ts`, so an
old event flushed at night is judged at night.

## Settings

The guard reads four global account-data keys of the pushing user:
`com.chat-harness.notify.defaults`, `.rooms`, `.schedule` and `.bots`. Each
key falls back to its own defaults when it is absent, over 128 KiB, of a `v`
other than 1, or has any invalid field. A missing or unknown timezone only
turns quiet hours off. Zone and link names match in any case (`utc`,
`us/eastern`); offsets such as `+01:00` are not names. The parser runs the shared fixture file
(`src/service/pusher/guard/fixtures/parse-cases.json`) in its unit tests.

With no settings at all, delivery is what it was before the guard. In
particular there is no burst limit until a `defaults` key exists; when one
exists without `burstWindowSec`, the window is 120 s.

## Discovery

`GET /_matrix/client/versions` lists `com.chat-harness.notify_guard: true` in
`unstable_features`. Clients show guard settings and send activity only when
it is there, so the app and the server can ship in either order.

## Device activity

`POST /_matrix/client/unstable/com.chat-harness/activity` with
`{"room_id": "!room", "seq": n}` (heartbeat) or `{"room_id": null, "seq": n}`
(clear). The access token names the account and device, so a report can only
quiet the same account's other devices. A pusher is tied to its device through
`pushkey_deviceid`; a pusher with no known device is never suppressed.

A heartbeat counts for 60 s (`ttl_ms` in the response). The highest `seq`
wins. A clear ends a claim only up to its own `seq`, and it fences off any
heartbeat with that `seq` or older that arrives late.

## Memory

All guard state is in memory. It is bounded, and it errs towards alerting:

- **Drops:** `(user, pushkey, event)` stays dropped for 24 h. At most 65,536
  are kept; the oldest go first. A restart forgets them, so an event retried
  after a restart is judged again.
- **Burst slots:** one per pusher and room. A slot is taken when the guard
  decides to send, and released if the gateway call fails, so a retry still
  alerts. At most 16,384 are kept; past that, expired slots are reaped first,
  then all of them. A restart costs at most one extra alert per room.
- **Activity:** one record per device. Ordering records outlive the claim by
  24 h, up to 16,384 devices; past that, the least recently reported go first.
  A restart ends every claim early.

## Legacy presence suppression

`suppress_push_when_active` (default `false`) runs before the guard. When it
is on, it holds every push while the user is online anywhere, which defeats
per-device rules. Keep it off. If it is on, the guard still judges the pushes
when they are flushed.
