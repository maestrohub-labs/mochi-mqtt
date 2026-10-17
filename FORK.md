# MaestroHub fork of mochi-mqtt/server

A MaestroHub fork of [mochi-mqtt/server](https://github.com/mochi-mqtt/server),
the embedded MQTT broker behind UNS.

It exists to carry fixes that have not shipped upstream. Every patch here must
also have an open upstream PR — the fork is a bridge, not a destination. The
end state is zero patches and we delete the fork. Same contract as the Pebble
fork; see `apps/backend/third_party/pebble/FORK.md`.

## Branch layout

| Branch | Purpose |
| --- | --- |
| `mh-stable` | **Default.** Last upstream release tag we adopted, plus our patches. MaestroHub consumes tagged releases of this branch as a plain `require`. |
| `main` | The fork's mirror of upstream `main`. No edits. |

## Tag scheme

`<upstream-tag>-mh.<n>` — `v2.7.9-mh.1` is the first MaestroHub release based on
upstream v2.7.9. Rebasing onto a newer upstream tag resets the counter.

`v2.7.9-mh.4` does not exist: it was pushed with the files' contents replaced by
their paths (a `gh api -f` instead of `-F`), deleted minutes later, and never
consumed by a release. The Go module proxy may still hold it; a published tag is
never moved, so the corrected release is `v2.7.9-mh.5`.

## How MaestroHub consumes this fork

This fork declares `module github.com/maestrohub-labs/mochi-mqtt/v2` and
rewrites its self-imports to match (52 `.go` files at v2.7.9-mh.7; the sed in
"Upstream sync" finds them all, so the count is informational). MaestroHub requires that path directly:

```text
require github.com/maestrohub-labs/mochi-mqtt/v2 v2.7.9-mh.7
```

No submodule, no `replace`, no clone-time setup. It is the same shape as every
other maestrohub-labs fork in the monorepo — `go-smb2`, `gocanopen/v2`, `fins`,
`gos7`, `bacnet-go`.

**The rename is a carried patch, not a migration.** A rebase onto a new
upstream tag restores upstream's module line and self-imports, so it must be
re-applied on every sync — see "Upstream sync". It is mechanical, never a merge
decision.

The three remaining references to the upstream path are in docs and an example;
none is compiled into the module we publish.

## Patches carried

### 1. `GetByListener` recursive RLock — deadlock (v2.7.9-mh.1)

**Upstream PR:** https://github.com/mochi-mqtt/server/pull/507 (filed 2026-08-03).
Drop this patch when it merges and we adopt a release containing it.

`Clients.GetByListener` held `RLock` and then called `Clients.Len()`, which
takes `RLock` again. `sync.RWMutex` documents this as prohibited:

> If a goroutine holds a RWMutex for reading and another goroutine might call
> Lock, no goroutine should expect to be able to acquire a read lock until the
> initial read lock is released. In particular, this prohibits recursive read
> locking.

A client connecting as a listener closes hits exactly that interleaving:

```
closeListenerClients -> GetByListener -> RLock ....... held
attachClient         -> Clients.Delete -> Lock() ..... queued, stops new readers
GetByListener        -> Len()          -> RLock ...... blocked behind the writer
```

The writer waits on the read lock the reader still holds. Permanent deadlock,
not a slow path.

**Impact.** Hung two MaestroHub CI packages (`protocols/lorawan`,
`protocols/azureiothub`) for the full 30-minute test timeout — an hour per
nightly — and is reachable on production broker shutdown, where it hangs
`Broker.Stop()` forever.

**Fix.** Read `len(cl.internal)` directly for the capacity hint. The map is
already guarded by the `RLock` this function holds, so the nested call was
never buying anything. No API or behaviour change.

**Regression test.** `clients_maestrohub_deadlock_test.go` drives concurrent
`GetByListener` and `Add`/`Delete` and requires completion inside a deadline.
Verified by reverting the patch: it hangs to the 20s deadline, and passes in
0.04s with it. The deadline IS the assertion — the unpatched code does not fail
an assertion, it stops.

**Upstream:** report/PR pending — see the tracking issue in the monorepo.

### 2. `ClientsWg` incremented on the wrong goroutine — shutdown race (v2.7.9-mh.2) — SUPERSEDED by patch 7 in v2.7.9-mh.7

**No longer carried.** The acceptor-side counting below still raced `Close`:
an acceptor that had accepted a connection and seen the listener open could
`Add(1)` while `Close` set `end`, closed the listener and reached `Wait()` at
zero. The race detector reports it under patch 7's stress test. Patch 7 counts
the connection under the lock `Server.Close` takes before `CloseAll` waits,
which closes that window. The `clientCounter` hand-off, the listeners'
`clientsWg` fields and their `Add`/`Done` were removed in v2.7.9-mh.7, and the
listeners match upstream again apart from patch 6. Upstream #508 is superseded
by https://github.com/mochi-mqtt/server/pull/551 (commented on #508, left open
for the maintainers). The history below is kept for context.

**Upstream PR:** https://github.com/mochi-mqtt/server/pull/508 (filed 2026-08-03), superseded.

`Server.attachClient` did `ClientsWg.Add(1)` on the per-connection goroutine the
acceptor spawns, so `Listeners.CloseAll` could reach `ClientsWg.Wait()` while the
counter was still zero and an `Add` was in flight. `sync.WaitGroup` permits `Add`
to race `Wait` only when the counter is already positive; the documented failure
mode is a panic, `WaitGroup misuse: Add called concurrently with Wait`.

**Fix.** The acceptor knows a connection exists strictly earlier than the worker
does, so the acceptor owns the counter. `Listeners.Add` hands each listener the
WaitGroup through an optional unexported `clientCounter` interface; `TCP`,
`UnixSock` and `Net` `Add(1)` before spawning; `Websocket` does it in its HTTP
handler, which already runs off the accept path but must still be counted or
`Wait()` would stop waiting for websocket clients once `attachClient` no longer
counts them. The `Add`/`Done` pair is removed from `attachClient`.

The interface is optional by design: a listener that does not implement it keeps
working and is simply not waited on, which is exactly the pre-patch behaviour for
anything `attachClient` never saw.

**Note on the originally proposed patch.** MaestroHub issue #3506 proposed
`l.ClientsWg.Add(1)` inside `TCP.Serve()`. That does not compile — `ClientsWg`
lives on `Listeners`, and the listener structs hold no reference to it. Hence the
`clientCounter` hand-off. The proposal also covered only `TCP`, which would have
silently stopped counting websocket clients.

**Upstream:** report/PR pending.

### 3. A client's SUBSCRIBE packet id is refused as "in use" by the server's own outbound publish (v2.7.9-mh.5)

**Upstream PR:** https://github.com/mochi-mqtt/server/pull/549 (filed 2026-10-07; one PR for patches 3–5).
Drop these three patches when it merges and we adopt a release containing it.
MaestroHub issue: maestrohub-labs/maestrohub#6639.

`processSubscribe` and `processUnsubscribe` answered `ErrPacketIdentifierInUse`
(0x91) whenever ANY inflight entry carried the packet identifier — including the
server's own outbound QoS 1/2 publishes, whose identifiers the server chose from
its own space. Per MQTT 2.2.1 the client's and the server's identifier spaces are
independent; only the client's own inbound QoS 2 flow (held as the server's
PUBREC, [MQTT-4.3.3-10]) can conflict, and `processPublish` already tells that
entry apart by type.

**Impact.** With both sides counting from 1, a SUBSCRIBE sent while a retained
replay at QoS 1 was still in flight to the client was refused most of the time
— seen as `Packet Identifier in use (0x91)` from the MaestroHub MQTT connector
against the UNS embedded broker.

**Fix.** `inboundPacketIDInUse`: the identifier is in use only when the inflight
entry is a `Pubrec`. The two existing `PacketIDInUse` tests now model that case;
`server_maestrohub_subscriptions_test.go` adds the outbound twin for subscribe
and unsubscribe.

### 4. The retained replay of a SUBSCRIBE carries no subscription identifier (v2.7.9-mh.5)

**Upstream PR:** https://github.com/mochi-mqtt/server/pull/549. MaestroHub issue:
maestrohub-labs/maestrohub#6637.

[MQTT-3.3.4-3] requires the identifier on every message published as a result of
the subscription, the retained ones sent at subscribe time included.
`publishRetainedToClient` passed the decoded `Subscription`, whose `Identifiers`
map (what `publishToClient` reads) is only filled by `Merge`, so the replay went
out without the property while live deliveries carried it.

**Impact.** The MaestroHub MQTT connector routes a SUBSCRIBE's retained replay to
the functions that SUBSCRIBE was for by its identifier (a late joiner gets the
retained messages, the holders do not); without the property every function on
the filter received the replay. Any external MQTT 5 client of the UNS broker that
routes by identifier saw the same.

**Fix.** Fill `Identifiers` from `Identifier` before the replay.

### 5. `SubIDAvailable` / `SharedSubAvailable` never announced nor enforced (v2.7.9-mh.5)

**Upstream PR:** https://github.com/mochi-mqtt/server/pull/549. MaestroHub issue:
maestrohub-labs/maestrohub#6638.

`Capabilities.SubIDAvailable` and `SharedSubAvailable` existed but were never
written to the CONNACK (3.2.2.3.12, 3.2.2.3.13 — absent means available), so a
client was never told the server lacks them, and a SUBSCRIBE using them was
accepted and silently not honoured.

**Fix.** Written when off (a default server's CONNACK is byte-for-byte unchanged,
pinned by a test), and such a SUBSCRIBE is refused with 0xA1 / 0x9E.

### 6. A connection accepted while the server closes is never closed (v2.7.9-mh.6)

**Upstream PR:** https://github.com/mochi-mqtt/server/pull/550 (filed 2026-10-07).
Drop this patch when it merges and we adopt a release containing it.

`Server.Close` sets the listener's `end` flag, disconnects its clients, then
closes the listening socket. A client that reconnects the moment it is
disconnected (paho.golang's autopaho redials with no delay on the first
attempt) lands in that window: `Accept` returns its connection, `Serve` sees
`end == 1` and skips `establish`, and the connection is dropped on the floor,
open. Nobody reads it and nobody closes it, so the client waits for a CONNACK
that never comes, until its own connect timeout (30 s for the MaestroHub MQTT
connector) — through a broker restart that took 50 ms. `TCP`, `UnixSock` and
`Net` all did it; `Websocket` closes its connection in a `defer`.

**Impact.** Every MQTT client of the UNS embedded broker that reconnects at
once when the broker restarts can stall for its connect timeout. In the
MaestroHub test suite the compliance cases `Ping_RecoversAfterServerRestart`
and `Connect_AfterLinkDeath_RealRedial` failed about one run in sixteen locally
and more often on loaded CI runners, each on a 10 s budget.

**Fix.** A connection accepted after `end` is set is closed. The client sees
EOF, reports a failed attempt and retries after its backoff, against the
restarted listener. Verified on the MaestroHub side: 100 of 100 runs of the
restart case pass with the patch; 3 of 50 failed without it.

**Regression test.** `listeners/shutdown_maestrohub_test.go`: `Serve` is
blocked in `Accept`, `end` is set as `Close` would, a client dials and reads;
the read must end with EOF promptly. Unpatched, it waits for the whole
deadline.

### 7. `Server.Close` hangs on a late CONNECT or a silent connection (v2.7.9-mh.7)

**Upstream PR:** https://github.com/mochi-mqtt/server/pull/551 (filed 2026-10-11; supersedes #508,
and includes #507's one-line `GetByListener` change verbatim because its stress test deadlocks
without it).
Drop this patch when it merges and we adopt a release containing it.

`Close` disconnects the clients registered in `Clients`, closes each listener,
then waits on `ClientsWg` for every connection being served. Two kinds of
connection escaped the disconnect pass and held that wait forever:

1. **Late CONNECT.** A connection accepted just before `Close` whose CONNECT
   completes just after the disconnect pass registers (`attachClient` →
   `Clients.Add`) too late to be disconnected, then blocks in `Client.Read` on
   its open socket. MaestroHub's load gate caught it as a 29-minute hang. The
   goroutine dump showed `Server.Close → Listeners.CloseAll → ClientsWg.Wait`
   on one side and `attachClient → Client.Read` on the other.
2. **Silent connection.** A TCP connection that never sends CONNECT has no
   read deadline before CONNECT, so `readConnectionPacket` blocks forever, and
   `Close` with it.

**Fix.** The server tracks every connection `attachClient` is serving,
registered or not (`connections` in `server.go`), under one mutex that also
holds a `closing` flag. `Close` sets `closing` before any listener is closed.
After its existing DISCONNECT pass, `closeListenerClients` stops every
tracked connection on that listener. `attachClient` refuses and closes a
connection that arrives once `closing` is set. A connection is therefore either
tracked before `closing` and swept, or refused, with no window in between. The
same lock owns the `ClientsWg` `Add`, which replaces patch 2.

A pre-CONNECT deadline is set from a new `Capabilities.ConnectTimeout`
(seconds, default 15, where 0 means the default), so a silent connection is
dropped even when the server is not closing ([MQTT-3.1.4]: "SHOULD close the
Network Connection" if no CONNECT arrives "within a reasonable amount of
time"). The client's keepalive takes over once CONNECT is read.

**Regression tests.** `server_close_maestrohub_test.go`. Deterministic: a
connection accepted before `Close` sends CONNECT after the listener has
closed; a client is held in `OnConnect` until after the sweep and then
registers; a silent TCP connection; `attachClient` after `Close`;
`ConnectTimeout` closes a silent connection and is lifted after CONNECT.
Stress: `TestServerCloseRacesConnectingClients` (16 clients dialling and
CONNECTing around each of 20 `Close`s). Unpatched, all four `Close` tests and
the stress test hang to their 5 s budget. Patched, `-race -cpu 1,2,8
-count=100` is clean.

**MaestroHub side.** `embedded.CloseServer`, the sweep-and-timeout guard in
`modules/uns/infrastructure/broker/embedded/close.go`, was removed when
v2.7.9-mh.7 was adopted. Its regression test now runs against this library.

### 8. One write(2) per PUBACK — ack coalescing and a cheap ack encoder (perf, v2.7.9-mh.8)

**Upstream PR:** https://github.com/mochi-mqtt/server/pull/552 (filed 2026-10-11; the
same change rebuilt on upstream `main`, including the `TestServerProcessPublishACLCheckDeny`
fix). It merges cleanly with #507, #508, #549, #550 and #551.
Drop this patch when it merges and we adopt a release containing it.

**Evidence.** CPU profile of MaestroHub Lite, 1,000 MQTT 5 clients publishing
QoS 1 at 88k msg/s on 24 cores: `syscall.Syscall6` was 13.6% of all CPU, and
`Client.WritePacket` was 13.5% cumulative, 98.7% of it from `processPublish`.
Each QoS 1 publish had its PUBACK encoded and written synchronously with its own
`write(2)`. The existing `outbuf` only batched while WriteLoop's queue was
non-empty, and a publisher's queue is empty. `PubackEncode` alone was 7.56 s of
the 47.8 s, mostly spent walking every property kind through nested map
lookups for a packet that can carry two.

**Fix.**

- *Coalescing* (`Client.Read`, `writeAck`). After decoding a packet, the reader
  checks whether the next packet is already complete in its `bufio.Reader`
  (`nextPacketBuffered`, a Peek over bytes already buffered, so no I/O). While
  it is, success acks from `processPublish` (PUBACK/PUBREC), `processPubrec`
  (PUBREL) and `processPubrel` (PUBCOMP) are appended to the client's existing
  `outbuf` under the existing write lock instead of being written. The buffer is
  flushed when any of these happens:
  - the reader would next block on the socket (no complete packet buffered),
  - the burst has run for `maxAckCoalesceDelay` (1 ms),
  - `outbuf` reaches `ClientNetWriteBufferSize`, which is the same threshold
    and the same blocking write as before,
  - any other packet is written to the client, through any path,
  - DISCONNECT is processed (before `Stop` closes the socket),
  - `Read` returns.

  A lone packet is acked immediately as before, because nothing is buffered
  behind it. Ordering per client is the order of the calls, since every write
  still goes through `outbuf` under one lock. `OnPacketSent` fires when the
  bytes are queued, the same as the pre-existing queued-write case. Write
  deadlines are unchanged.
- *Ack encoder.* PUBACK/PUBREC/PUBREL/PUBCOMP with no Reason String and no User
  Properties (or problem info disallowed) are written directly as their fixed
  2–3 byte body. `TestEncodePubAckRelRecCompFastPathMatchesGeneral` compares
  this byte for byte with the upstream encoder over 1,200 combinations.
  ~620 ns → ~10 ns per PUBACK.
- `ReadPacket` copied every inbound packet's bytes a second time, into a slice
  that was already freshly allocated. Dropped (−1 alloc per inbound packet).

**Numbers** (`ack_coalesce_bench_test.go`, real TCP listener, loopback, 32-thread
host under unrelated load, benchstat n=8 interleaved, unpatched vs patched;
"~" = not significant):

| case (each client keeps 32 publishes in flight) | writes/msg | msg/s | ack p99 |
| --- | --- | --- | --- |
| 1 client | 1.00 → 0.15 | 68k → 109k (+61%) | 1.69 → 0.94 ms |
| 50 clients | 1.00 → 0.10 | 531k → 826k (+56%) | −42% |
| 500 clients | 1.00 → 0.10 | 703k → 983k (+40%) | ~ |
| 1 client, 20 µs OnPublish | 1.00 → 0.065 | 25.0k → 34.5k (+38%) | −26% |
| 50 clients, 20 µs OnPublish | 1.00 → 0.071 | 288k → 332k (+15%) | ~ |
| 500 clients, 20 µs OnPublish | 1.00 → 0.078 | 383k → 465k (+21%) | −25% |
| idle: one publish at a time | 1.00 → 1.00 | — | p50 9.2 → 8.5 µs ~, p99 50.2 → 49.8 µs ~ |

allocs/op 11 → 10 and B/op −5% in every case.

**Tests.** `ack_coalesce_tcp_test.go` (real TCP) covers: burst order with fewer
writes, a lone publish acked immediately, a partial next packet not holding
acks, DISCONNECT in the same segment keeping its acks, and the delay bound.
`ack_coalesce_test.go` covers: slow-reader back-pressure bounded at the buffer
size, held acks flushed in order by another packet, close while held, the
`Read` exit flush, and a table for `nextPacketBuffered`. Each guard was checked
by mutation: removing the DISCONNECT flush, the delay bound, the exit flush, or
the partial-packet check makes its test fail.

Also fixes a pre-existing flake in `TestServerProcessPublishACLCheckDeny`
(2/20 under `-race` on the v2.7.9-mh.7 tag): it asserted `cl.Closed()` before
waiting for `processPublish`. `Stop` closes the conn, which ends the test's
`ReadAll`, before it marks the client closed.

### 9. Acknowledge a publish only after the hooks that handle it are done — `OnPublishedSettle` (v2.7.9-mh.9)

**Upstream PR:** not yet filed. The fork contract above requires one; the
change is a new opt-in hook, not MaestroHub-specific, and changes nothing for
a server without a hook that provides `OnPublishedSettle`.

**Problem.** `processPublish` wrote the QoS 1 PUBACK (QoS 2 PUBREC) before
`OnPublished` ran. MaestroHub stores every value from a hook (handed to a
worker pool), so the client was told "the broker has it" while the value was
still only in memory: a crash in between lost values the client had
discarded. Measured on MaestroHub Lite, kill -9 mid-stream: acknowledged values
missing from the historian afterwards.

**Fix.**

- New hook `OnPublishedSettle(cl, pk, *PublishSettlement)`, called for every
  publish right after `OnPublished`. A hook that must finish with a message
  before it is acknowledged calls `s.Hold()` (before returning) and later the
  returned `release(code)` exactly once, from any goroutine. QoS 0 and
  inline-client publishes get a settlement whose `Hold` is a no-op, so a hook
  treats every publish alike.
- With such a hook present, the ack of a QoS 1/2 publish is reserved on the
  client's `ackSequencer` (`acks.go`) by the reader, in arrival order, and
  written once every hold is released. Settled acks leave strictly in arrival
  order ([MQTT-4.6.0-2]); a settled prefix goes out in one write
  (`writeAcks`), still subject to the read-burst coalescing of patch 8.
  Refusal acks decided on arrival (not authorized, packet id in use, a reason
  code from `OnPublish`) queue behind pending acks instead of overtaking them.
  Each client has its own sequencer: one client's slow message never holds
  another client's acks.
- `release(code)` with a code ≥ 0x80: an MQTT 5 client gets it as the
  PUBACK/PUBREC reason code; an MQTT 3 client (no reason codes) gets neither
  that ack nor the ones behind it, and is disconnected so it sends them again.
- Nothing enters the inflight store until its ack is written (a QoS 2 PUBREC
  then does, to await PUBREL): a session resumed meanwhile never resends an
  ack for a message that was not handled.
- Receive quota returns when the ack is written. An MQTT 5 client that exceeds
  its Receive Maximum is still disconnected (0x93). An MQTT 3 client, which
  knows no Receive Maximum, is instead not read from until an ack returns
  quota (TCP back-pressure).

**API.** The `Hook` interface gains `OnPublishedSettle`; a hook that embeds
`HookBase` (every hook here and in MaestroHub) needs no change. The
constant is appended after `StoredSysInfo`, so no existing hook constant
changes value. `PublishSettlement.Withholds()` says whether an ack waits
(QoS 1/2 from a network client); `Done()` closes when that client goes away.

**Trade-off.** One client's acks wait for its slowest pending message, so a
client's in-flight window (and the server's Receive Maximum) bounds its
throughput by the handler's latency, as on any broker that acknowledges after
storing. (Two defects of this first version are fixed in patch 10: acks were
written on the releasing goroutine, and a QoS 2 PUBLISH resent while its first
copy was still being handled was handled twice.)

**Tests.** `acks_test.go`: ack waits for release; arrival order with
out-of-order releases; settled prefix in one write; refusal behind a pending
ack; MQTT 5 reason code; MQTT 3 disconnect without acks; QoS 0 no-op; QoS 2
PUBREC → inflight → PUBCOMP; other clients not held back; MQTT 3
back-pressure; MQTT 5 still disconnected past Receive Maximum; no hook →
immediate ack; a second release ignored; `Withholds`/`Done`; a refusal code
reaching an MQTT 5 client. Mutation: disabling the deferral makes
10 of them fail.

### 10. Acks are written by the client's own writer; a QoS 2 resend is not handled twice (v2.7.9-mh.10)

**Upstream PR:** to be folded into patch 9's upstream PR
(mochi-mqtt/server#553) — same feature.

**Problems (independent review of patch 9, 2026-10-15).**

- `ackSequencer.settle` wrote the settled acks while holding the sequencer's
  lock, on the goroutine that released the hold. In MaestroHub that is a
  shared worker; a client that stops reading its socket blocks the write
  (`cl.Lock` in `writePacket`), and every worker that settles one of its
  publishes then waits too — 7–8 of 8 workers in the reviewer's reproduction,
  after which healthy clients' publishes ran without workers.
- A QoS 2 PUBLISH resent while its PUBREC was held (not yet in the inflight
  store) was handled again: on the same connection, and on the connection
  that resumed the session after a reconnect (the reviewer's handler ran
  twice). QoS 2 is exactly once.

**Fix.**

- Releasing a hold never writes. `settle` moves the settled prefix to the
  client's `ready` list and wakes the client's `WriteLoop` (the goroutine
  that already writes its outbound publishes for the life of the
  connection), which writes them in order (`writeReady`) — only it can block
  on a slow client. (v2.7.9-mh.10/11 started a goroutine per run of settled
  acks instead: thousands per second at device-ingest rates.) WriteLoop
  writes its run through; only the reader may leave acks in the write buffer
  for its own flush (`writeAckCoalesced`), since the reader can be blocked in
  a handler — a store run inline, `waitForQuota` — while it holds a buffered
  packet, and acks another goroutine left there waited as long as it did. A
  connection's end resets the sequencer (WriteLoop drains what it was handed;
  `settle` drops runs for a closed client, which resends them). Runs are
  written from `ready` itself, with no copy of each 800-byte ack packet.
  Only the acks written count: an ack that cannot be written on an open
  connection (larger than the client's Maximum Packet Size, a write that
  timed out) returns no quota and completes no flow, and ends the
  connection — the acks behind it must not overtake it, and upstream's
  reader ends the connection on a failed ack write too; the acks before it
  are flushed first. WriteLoop flushes the write buffer after a publish it
  fails to write, since bytes held because that publish was queued waited
  for its write.
  The server's own hold, released on the client's
  reader at the end of `processPublish`, writes inline when no writer runs:
  hooks that settle synchronously cost no goroutine, and the reader blocking
  on its own client is back-pressure. A refusal decided on arrival
  (`writeNowOrQueue`) goes behind acks that are settled but not yet written.
  Receive quota returns when the writer has written the ack. After a refusal
  an MQTT 3 client cannot be told, the sequencer is closed under its lock:
  nothing is written after it, even before the connection is stopped.
- QoS 2: a session-scoped registry (`acks_qos2.go`) of the packet ids
  received and not yet released, keyed by client id and packet id, so it
  outlives a connection. Each entry holds the publish's verdict once its
  hooks are done, and the PUBRECs waiting for it: the first copy's, and one
  per resend — each reserved in the queue of the connection its copy arrived
  on, in that connection's arrival order, and bound to it for life. Settling
  the first copy settles them all; a resend after that is answered at once.
  The entry is registered before `OnPublish` (after the topic alias, which a
  resend may establish), ends at PUBREL (also when answered 0x92), on a
  refusal (the flow ends), when its PUBLISH is dropped or downgraded in
  `OnPublish` (resends already waiting get that verdict), or with the session;
  a PUBLISH reusing a registered id without DUP is a new message (its entry
  replaces the old). Receive quota is taken once per connection for an id: a
  resend on the connection that already holds it takes none. While the client
  follows the protocol there is an entry per open QoS 2 flow (at most the
  Receive Maximum); an id whose PUBREL never comes stays until the session
  ends — at most one per packet id, 65,536 per session. Sharded by client id
  (64 shards): sessions never contend.
- A PUBLISH resent (DUP) after its PUBREC was written is answered with its
  stored verdict (upstream answered 0x91 "packet identifier in use", which an
  MQTT 5 client reads as a failure of a stored message); a DUP 0 PUBLISH
  reusing that id keeps upstream's 0x91. The verdict lives as long as the
  process: a session restored from a storage hook after a restart has none,
  and its resends get 0x91 again. An MQTT 5 QoS 2 PUBLISH refused with a
  reason code in `OnPublish` gets a PUBREC (upstream wrote a PUBACK).
- A copy a hook lowers to QoS 1 keeps its entry until its own verdict: resends
  waiting on it are answered with that verdict, never before it; then the id
  is dropped (no PUBREL will come for it). A resend on a connection that
  already holds a reservation for the id is queued as an answer only: written,
  it returns no receive quota and enters nothing in the inflight store.
- A PUBREL carrying a failure reason ends the flow and frees the id.
- `Inflight.NextImmediate` read-locked twice (an upstream bug, in mh.9 too):
  Go's RWMutex is not reentrant for readers — once a writer waits, the nested
  RLock blocks and the outer one never returns. With acks written off the
  reader, the ack writer's `Set` (ackWritten) raced the reader's
  `NextImmediate` and both waited forever; any publisher routing to that
  client then blocked too. The outer lock is gone (`GetAll` takes it).
- The old client of a takeover is marked taken over before it is
  disconnected, on a clean start too. A clean start then removes the old
  session's subscriptions from the topic index itself (`unsubscribeAll`):
  `UnsubscribeClient` skips the index for a client taken over, which is
  why upstream marked only after unsubscribing ([MQTT-3.1.2-4]).
- A session's fate is decided once, by one compare-and-swap on the old
  client (`claimTakeover` / `claimEnd`): taken over by a new connection, or
  ended — by its connection's `attachClient` tail when it expires with it,
  or by the expiry sweep. Whoever wins does all of it; the others do
  nothing. The end (`endSessionOf`) clears the inflights, removes the
  subscriptions from the topic index (upstream's sweep never did: the
  index leaked, and the next clean session under that id was routed the
  old filters), deletes the client id only while it still maps to that
  client (`Clients.deleteIf`), and only then ends its QoS 2 ids. A
  takeover that finds the session already ended inherits nothing. (The
  fourth round's taken-over check followed by a delete was two steps a
  takeover could interleave.)
- `PublishSettlement.Done` follows the session for a QoS 2 publish while its
  id is registered: once a takeover moved the session to another connection,
  `Done` is that connection's; in the moment between the old connection's
  disconnect and the new one's registration it reports "not done" (ask
  again) — the old client is marked taken over before it is disconnected.
  A clean-start takeover ends the id, and `Done` is the old connection's.

**Design note (rule 27).** A first version moved held PUBRECs between
connections at takeover (`adopt`) and swapped each ack's owner; the review
found a race and lost wakeup between a release and the move, plus three
gaps. Recording the id in the session's inflight store at receipt (as
upstream does) recognises a resend but cannot answer it after a takeover:
the store is cloned, and the verdict lives with the old connection, so no
PUBREC would ever reach the new one. The registry is the smallest shape that
does both — nothing moves, no owner is swapped.

**Remaining window.** A QoS 2 PUBLISH is registered after the receive-quota
wait and the ACL check: a resend processed on a new connection while the old
connection's reader sits between reading the PUBLISH and that point is
handled a second time. A refused PUBREC (a hook's failure code) ends the
flow; a later resend is a new attempt.

**Second review (bcaa56b → this).** R1 a same-connection resend took receive
quota twice (an MQTT 3 client was disconnected after nine exchanges); R2 a
resend waiting on a copy then dropped in `OnPublish` was never settled and
held every later ack of its connection; R3 a resend returned before its topic
alias was registered; R4 a 0x92 PUBREL left the id registered; R5 `Done`
during the takeover window; R6 one registry lock for all sessions; plus the
two upstream answers above. All fixed; the reviewer's reproductions are kept
as `acks_qos2_review_test.go`.

**Third review (a71a7a7 → this).** The `NextImmediate` deadlock (reproduced
after 440–1,240 exchanges at a71a7a7, 24,440 at mh.9); a copy lowered to QoS 1
let a waiting resend be acknowledged before the copy was settled; a failed
PUBREL left the id registered (a later first-seen DUP copy was answered from
it and never handled); a DUP 0 reuse got 0x00; two answers on one connection
returned quota twice on a refusal; the clean-start takeover tail; the mutation
gap on the early taken-over mark. Fixed; the reviewer's tests are kept as
`acks_qos2_review3_test.go` and `inflight_deadlock_test.go` (160,000 QoS 2
exchanges with PUBRECs settled off the reader), and
`TestTakeoverMarksTheOldClientBeforeDisconnectingIt` now pins the order the
second review could not (a hook sees the DISCONNECT the old client is sent).

**Tests.** `acks_nonblocking_test.go`: a release does not block on a client
that does not read (unread `net.Pipe`), and the acks arrive in order once it
reads; a refusal does not overtake acks being written (with a watchdog);
nothing is written after an MQTT 3 refusal closed the sequencer.
`acks_qos2_resend_test.go`: a resend while the PUBREC is held, after it is
settled but unwritten, after a takeover, after a takeover of a settled
publish, after two takeovers; a DUP 0 reuse of an id is handled; a release
racing the takeover always answers the resend (200 iterations); PUBREL and a
session end free the registry; `Done` follows the session.
`acks_qos2_more_test.go`: a 0x92 PUBREL frees the id; an ack for a closed
connection leaves its inflight store alone; a clean takeover, the expiry
sweep and a session ending with its connection (end to end) free the ids;
`Done` is not done while the session moves; a QoS 2 refusal in `OnPublish`
is a PUBREC; a resend after the PUBREC was written gets its verdict.
`acks_qos2_review_test.go`: the second review's reproductions (quota leak,
stranded waiter, topic alias, the old reader after a takeover, an MQTT 3
refusal withholding later acks, a release/takeover/PUBREL stress).
`acks_qos2_review4_test.go` and `clients_deleteif_test.go`: a clean start
drops the old subscriptions; a lowered copy's full flow and freed entry;
three answers on one connection; a DUP 0 reuse while held keeps quota; an
ending connection and the expiry sweep keep a session that replaced theirs.

**Fourth review (7a3701b → this).** The clean-start takeover regression
(the old session kept routing — the mark before `UnsubscribeClient`), the
session-end check-then-act, a DUP 0 reuse while held losing a quota unit
(it now inherits the connection's reservation: no second unit, its PUBREC
an answer only), and tests that asserted nothing or wrote to a host path.
MaestroHub reads `Done` on every attempt (`heldPublish.expired`), so the
"not done" answer during a takeover is re-asked, never a lasting verdict.

**Fifth review (e4e7d74 → this).** The sweep winning the end during a
connection's tail left its subscriptions behind (a regression of the fourth
round's compare-and-delete), the sweep never unsubscribed expired sessions
(upstream), a DUP 0 reuse of a held id answered only — a refused first copy
left the stored one without an inflight entry, its PUBREL answered 0x92 — and
a reuse lowered to QoS 1 returned a quota unit it never took; and the
takeover-versus-end decision, made atomic (`fate`). The reviewer's
reproductions are `acks_review5_test.go`; `session_fate_test.go` pins the
decision both ways and the sweep leaving a session being taken over.

Mutation: each of the 35 fixes of the five review rounds, reverted, fails
at least one test;
the cross-shard stress runs clean under `-race -count=10`. `-race -count=5` clean
(`TestServerAddListenersFromConfig` fails on `mh-stable` too, unrelated).

### 11. Optional properties are judged on the whole packet size (v2.7.9-mh.13)

**Problem.** A client's Maximum Packet Size forbids the server to send a
Reason String or User Properties that would make a packet larger than it
([MQTT-3.4.2-2] [MQTT-3.4.2-3], and the same rule for PUBREC, PUBREL,
PUBCOMP, SUBACK, UNSUBACK, CONNACK, DISCONNECT, AUTH). Upstream's
`Properties.Encode` compared only the bytes before the properties plus the
property against the limit — not the fixed header, a PUBACK's reason code,
an UNSUBACK's reason codes or the property length — so a packet a few
bytes over the limit kept the property, and `writePacket` then refused the
whole packet (`ErrPacketTooLarge`). Since patch 10 an ack that cannot be
written ends the connection, so this became a disconnect: an MQTT 5 refusal
PUBACK with Reason String "abcdefghij" (19 bytes) to a client whose maximum
is 16–18.

**Fix.** `Encode` encodes the properties, and while the whole packet is over
the limit encodes them again without the User Properties, then without the
Reason String, then without both (the Reason String is kept in preference).
The callers pass the true length of the rest of the remaining length
(CONNACK no longer counts its two bytes twice; PUBACK/PUBREC/PUBREL/PUBCOMP
count the reason code that precedes properties; UNSUBACK counts its reason
codes). The upstream fixture `TConnackDropPropertiesPartial`, which expected
a 24-byte packet for a maximum of 18, now expects the 15-byte packet.

**Tests.** `packets/properties_maxsize_test.go`: every size from the bare
packet to one past the full packet, for both properties and every packet
type above; the 15–19-byte PUBACK boundary; remaining lengths that cross
the Variable Byte Integer boundaries (127, 16383); the Reason String kept
before the User Properties when only one fits.

**A PUBLISH is never altered (also in v2.7.9-mh.13, fork PR #14; earlier
drafts of this note said mh.14).** #13 put PUBLISH through
the same drop loop, so any PUBLISH too large for the client (big
correlation data, say) went out with its User Properties stripped. They
are the message's own and reach the receiver unaltered ([MQTT-3.3.2-17]);
a PUBLISH too large for the client is discarded whole ([MQTT-3.1.2-25],
`writePacket`: `ErrPacketTooLarge`, fixture `TPublishDropOversize`) — as
upstream intends, which stripped them only when they alone were too big.
Separately (upstream too), Request Problem Information 0
(`DisallowProblemInfo`) stripped a PUBLISH's User Properties, though
[MQTT-3.1.2-29] exempts PUBLISH; it no longer does. Tests:
`TestPublishUserPropertiesAreNeverStripped` (packets),
`TestOversizePublishIsDiscardedNotStripped`.

### 12. A slow subscriber's messages wait in a bounded queue; the oldest is dropped, not the newest (v2.7.9-mh.14)

**Upstream PR:** not yet filed (same status as patch 9).

**Problem.** `publishToClient` dropped the NEW message for a subscriber
that could not take one more:

- its in-flight store held `MaximumInflight` QoS 1/2 messages — WARN
  `client store quota reached` per message, no hook (`// add hook?`);
- no packet id was free — WARN `packet ids exhausted` per message;
- its outbound channel held `MaximumClientWritesPending` messages —
  `OnPublishDropped`.

A slow dashboard therefore kept the oldest values it had been sent and
lost every newer one until it caught up. Separately, a message beyond the
client's own Receive Maximum was parked in the in-flight store with
`Expiry -1`, sent later in the order of its creation *second* (map order
within a second), and deleted from the store once written: its PUBACK
found nothing, returned no send quota, and after Receive Maximum such
messages the client received nothing more, for good (reproduced:
`TestReceiveMaximumQueuedMessagesAllArriveInOrder` times out on mh.13).

**Fix** (`pending.go`). One per-client FIFO, `pendingQueue`, replaces both
the drops and the `Expiry -1` parking:

- A message the client cannot take now — in-flight store full, send quota
  under the client's Receive Maximum used up, no free packet id, outbound
  channel full — is queued. Once anything is queued, later messages queue
  behind it, so delivery order is arrival order.
- The queue holds at most `Capabilities.MaximumClientPendingMessages`
  (default 8192; 0 means the default). When full, the OLDEST queued
  message is dropped for each new one: counted in `Info.MessagesDropped`
  and reported through `OnPublishDropped` (now the one drop signal for a
  subscriber). No per-message WARN.
- A queued message holds no packet id, no in-flight entry and no send
  quota; it gets them when it is handed out. QoS 1/2 semantics are
  unchanged from there (in flight until PUBACK/PUBCOMP, resent on resume).
- The topic alias is assigned when a message is handed out, not on
  arrival: a message dropped from the queue never claims an alias the
  client was not told (upstream had the same hole for its drops).
- Drained, oldest first, whenever room may have appeared: after the
  reader processes a packet (PUBACK/PUBREC/PUBCOMP), after WriteLoop
  writes an outbound packet, after the in-flight expiry sweep, and after a
  resumed session's in-flight messages are resent. A queued message whose
  expiry passed is deleted, not sent ([MQTT-3.3.2-5]).
- Every send to the outbound channel happens under the queue's lock, which
  makes the "room in outbound" check exact (WriteLoop only takes).
- A persistent session's queue moves with it on takeover, behind its
  in-flight messages; a clean start or session end discards it
  (`ClearInflights`). A client without a connection is never drained
  (v2.7.9-mh.15): mh.14 moved its queued QoS 1/2 messages into its
  in-flight store, and when that happened after a takeover had cloned the
  store (the closed client's WriteLoop finishing a write, or the expiry
  sweep), they were lost to the resumed session.
  `TestDrainPendingLeavesAClosedClientsQueueWhole` (fails without the
  guard).
- Memory: a ring of pointers grown on demand up to the limit and released
  when it empties; the queued packets share their payload with every other
  subscriber's copy. Bound per slow subscriber: limit × packet size.

**Behaviour change for embedders.** `publishToClient` no longer returns
`ErrQuotaExceeded` / `ErrPendingClientWritesExceeded` for these cases
(the message is queued: `nil`), `Info.InflightDropped` is no longer
incremented by it, and the two WARN lines are gone. MaestroHub counted
drops from those WARNs (`drop_log_filter.go`); from mh.14 it counts
`OnPublishDropped` only.

**Tests.** `pending_test.go`: the ring drops strictly oldest first, never
allocates past the limit, frees its buffer when empty, keeps order across
wrap and growth; publishes queue behind queued ones; a queued QoS 1 message
takes its packet id and in-flight entry when a PUBACK frees a slot; an
expired queued message is deleted; a dropped queued message claims no
topic alias; `ClearInflights` empties the queue; WriteLoop drains the queue
as the outbound channel frees; 8 concurrent publishers against a draining
writer lose and duplicate nothing (`-race`). `pending_tcp_test.go` (real
TCP): a subscriber that stops acknowledging gets its window, then the 10
newest of 100 messages in order, with the 86 oldest-queued reported
dropped in order; queued messages follow a resumed session; a client's
Receive Maximum is honoured and every message over it arrives in order.
Updated upstream tests: `TestPublishToClientExceedClientWritesPending`,
`…ExceedMaximumInflight`, `…ExhaustedPacketID` now expect the message
queued; `TestServerProcessOutboundQos2Flow` shares the server's `Info`
with its client. Mutation: dropping the newest instead fails 3 tests;
removing the reader's drain fails `TestQueuedQoS1TakesItsPacketIDWhenDelivered`;
removing WriteLoop's drain fails `TestWriteLoopDrainsTheQueueAsOutboundFrees`.

**Not changed.** `ResendInflightMessages` still resends a resumed session's
in-flight messages in creation-second order (upstream), so the resent
window's own order is not guaranteed; the queued messages follow it in
order.

### 13. A resumed session gets its backlog in the order it was sent; an embedder can withdraw queued messages before it resumes (v2.7.9-mh.16)

**Upstream PR:** the ordering half overlaps mochi-mqtt/server#529 (a
`Created`-then-packet-id tie-break, not filed by MaestroHub); the rest is
not yet filed.

**Problem.** Found while making MaestroHub keep persistent sessions
(maestrohub#7101): with a session kept across a disconnect, every QoS 1
message published while it was offline was delivered on resume, but in a
random order.

- `Inflight.GetAll` (the re-send path, `ResendInflightMessages`) sorted on
  `uint16(Created)`. `Created` is in seconds, so a whole backlog shares one
  key; `sort.Slice` over a map then returns it in random order — against
  [MQTT-4.6.0-1] ("re-send them in the order in which the original PUBLISH
  packets were sent"). The `uint16` truncation also wrapped every 18.2 h.
  A packet-id tie-break (#529) is not enough: ids wrap at 65535, and an
  offline backlog of a few thousand messages crosses the wrap.
- A message published while a session was resuming — registered under its
  client id, CONNACK written, in-flight messages not re-sent yet — was
  handed out at once, overtook the whole backlog, and was then sent a
  second time by the re-send (it was in the in-flight store by then).
- An embedder that re-decides what a session may receive (a permission
  withdrawn while it was offline) could reach the in-flight store but not
  the pending queue (patch 12, unexported), so it could neither withhold
  queued messages nor count them.

**Fix.**

- `inflight.go`: the store numbers each packet id when it is first stored
  (`order`, `next`); `GetAll` returns that order. Replacing a packet under
  its id (a QoS 2 PUBLISH by its PUBREL) keeps its place; a deleted id that
  is reused goes to the end; `Clone` (a takeover) keeps and continues the
  numbering. `Created` is no longer part of the order (a wall clock that
  steps back would reorder it too).
- `pending.go` / `server.go`: a resumed session's queue is held
  (`holdPending`, set before the client is registered) until its in-flight
  messages are re-sent; a message arriving meanwhile queues behind the
  backlog, and `releasePending` then drains it in order. WriteLoop and the
  expiry sweep do not drain a held queue.
- `pending.go`: `Client.RemoveQueued(remove func(packets.Packet) bool) int`
  removes the matching PUBLISH packets from the in-flight store (returning
  their send quota and `Info.Inflight`, reported via `OnQosDropped` as
  `ClearInflights` does) and from the pending queue (the rest keeps its
  order), under the queue lock so nothing moves between the two. PUBREL
  and inbound QoS 2 entries are never touched.

**Tests.** `resume_order_test.go`: same-second packets whose ids wrap come
back in send order (50 trials); no reorder across a 65536 s boundary;
order survives replace / delete-reuse / clone; `RemoveQueued` filters both
stores, returns quota and the in-flight count, keeps PUBREL/PUBREC and the
queue's order. `resume_order_tcp_test.go` (real TCP): a 200-message
offline backlog arrives in publish order; a backlog deeper than the window
arrives in order (window then queue); messages removed before the resume
are not delivered; a publish made at the CONNACK of a resuming session
arrives after the backlog, once. `TestQueuedMessagesFollowAResumedSession`
now asserts the full order (it allowed the window in any order). All fail
before the fix. Mutations: a packet-id-only sort fails the three ordering
unit tests; dropping the pending pass of `RemoveQueued` fails both removal
tests; dropping the hold fails `TestPublishDuringResumeFollowsTheBacklog`.

**Not changed.** Messages published to the OLD client after
`inheritClientSession` has moved its queue and in-flight store, but before
the new client is registered, still land on the old client and are lost
with it (upstream; a window of a few microseconds per takeover).
`loadInflight` (storage hooks) stores restored packets in the storage's
order.

## Upstream sync

```sh
git remote add upstream https://github.com/mochi-mqtt/server.git
git fetch upstream --tags
git rebase <new-tag> mh-stable     # replay patches

# Re-apply the module-path rename — the rebase brings back upstream's.
sed -i '1s#module github.com/mochi-mqtt/server/v2#module github.com/maestrohub-labs/mochi-mqtt/v2#' go.mod
grep -rl 'github.com/mochi-mqtt/server/v2' --include='*.go' . \
  | xargs sed -i 's#github.com/mochi-mqtt/server/v2#github.com/maestrohub-labs/mochi-mqtt/v2#g'
git commit -am "chore: re-apply module-path rename after upstream sync"

go build ./...
go test ./... -count=1             # TestServerAddListenersFromConfig binds a
                                   # hardcoded :1883, so it fails whenever
                                   # anything local holds that port — including
                                   # a running MaestroHub broker. Verified to
                                   # fail identically on pristine upstream
                                   # v2.7.9; not a fork regression.

# Never move a published tag: the Go module proxy caches immutably, so a
# moved tag becomes a checksum mismatch for anyone who already fetched it.
git tag <new-tag>-mh.1 && git push origin mh-stable --tags

# Then in maestrohub:
#   go get github.com/maestrohub-labs/mochi-mqtt/v2@<new-tag>-mh.1
```
