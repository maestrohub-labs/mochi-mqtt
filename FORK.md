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
| `mh-stable` | **Default.** Last upstream release tag we adopted, plus our patches. MaestroHub consumes this via git submodule. |
| `main` | The fork's mirror of upstream `main`. No edits. |

## Tag scheme

`<upstream-tag>-mh.<n>` — `v2.7.9-mh.1` is the first MaestroHub release based on
upstream v2.7.9. Rebasing onto a newer upstream tag resets the counter.

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

## Not carried (yet)

**WaitGroup misuse on shutdown** (monorepo issue #3506). `Listeners.CloseAll`
reaches `ClientsWg.Wait()` while an `Add(1)` is in flight inside
`Server.attachClient`, which `sync.WaitGroup` permits only when the counter is
already positive. Documented failure mode is a panic on shutdown.

The patch proposed in #3506 — `l.ClientsWg.Add(1)` in `TCP.Serve()` — **does not
compile**: `ClientsWg` lives on `Listeners`, and the individual listener structs
(`TCP`, `UnixSock`, `Net`, `Websocket`) hold no reference to it. Fixing it
properly means giving the acceptors access to the WaitGroup, and covering
`Websocket`, which calls `establish` synchronously from its HTTP handler rather
than from a spawned goroutine — so a fix that only touches the three raw
acceptors would stop counting websocket clients and make `Wait()` return early.

Left for a considered change rather than a rushed one.

## Upstream sync

```sh
git remote add upstream https://github.com/mochi-mqtt/server.git
git fetch upstream --tags
git rebase <new-tag> mh-stable     # replay patches
go test ./... -count=1             # TestServerAddListenersFromConfig fails on
                                   # pristine v2.7.9 too — pre-existing upstream
git tag <new-tag>-mh.1 && git push origin mh-stable --tags
```
