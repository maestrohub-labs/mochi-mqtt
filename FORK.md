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

## How MaestroHub consumes this fork

This fork declares `module github.com/maestrohub-labs/mochi-mqtt/v2` and
rewrites its 50 self-imports to match. MaestroHub requires that path directly:

```text
require github.com/maestrohub-labs/mochi-mqtt/v2 v2.7.9-mh.3
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

### 2. `ClientsWg` incremented on the wrong goroutine — shutdown race (v2.7.9-mh.2)

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
