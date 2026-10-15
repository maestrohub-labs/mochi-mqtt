// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

package mqtt

// MAESTROHUB PATCH (ack after handled): QoS 2 exactly once while PUBRECs
// are held.
//
// Upstream writes a QoS 2 PUBREC on receipt and puts it in the session's
// inflight store, where a resent PUBLISH is recognised. With the PUBREC
// held until the hooks are done (acks.go), nothing is inflight yet, so a
// PUBLISH resent meanwhile — on the same connection, or on the connection
// that resumed the session after a reconnect — looked new and was handled
// twice.
//
// The registry is the session's memory of every QoS 2 packet id received
// and not yet released (PUBREL): keyed by client id (the session) and
// packet id, so it outlives a connection. Each entry holds the publish's
// verdict once its hooks are done, and the acks waiting for it: the
// PUBREC of the first copy, and one for every resend, each reserved in the
// queue of the connection the copy arrived on — in that connection's
// arrival order — and bound to it for life. Settling the first copy settles
// them all; a resend after that is answered at once. Nothing moves between
// connections, so nothing needs to know which connection holds the session.
//
// Receive quota: a connection's first reservation for an id takes one unit
// of its quota, returned at PUBREL; a further resend on the same connection
// takes none (one PUBREL ends the flow there).
//
// An entry ends at PUBREL, with a refusal (the flow ends; a resend is a new
// attempt), when its PUBLISH is dropped before being handled (waiting
// resends are settled with the drop's verdict), or with the session. A
// PUBLISH reusing a registered id without the DUP flag is a new message
// (the client restarted its packet ids): its entry replaces the old one.
// While the client follows the protocol an entry per open QoS 2 flow
// exists, so at most the Receive Maximum per session; an id whose PUBREL
// never comes (a client that gave up on it) stays until its session ends,
// at most one per packet id (65,536 per session).
//
// Sharded by client id: sessions never contend with each other.

import (
	"hash/maphash"
	"sync"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
)

// qos2Waiter is one PUBREC waiting for a QoS 2 publish's verdict, on the
// connection its copy arrived on.
type qos2Waiter struct {
	cl *Client
	p  *pendingAck
}

// qos2Entry is one QoS 2 packet id of a session, received and not yet
// released.
type qos2Entry struct {
	settled bool
	reason  packets.Code
	waiters []qos2Waiter
	// reserved are the connections that hold a reservation (and a unit
	// of receive quota) for the id.
	reserved map[*Client]struct{}
}

const qos2Shards = 64

type qos2Shard struct {
	mu sync.Mutex
	m  map[string]map[uint16]*qos2Entry
}

// qos2Registry is the server's per-session QoS 2 memory. The zero value is
// ready to use.
type qos2Registry struct {
	seedOnce sync.Once
	seed     maphash.Seed
	shards   [qos2Shards]qos2Shard
}

func (r *qos2Registry) shard(clientID string) *qos2Shard {
	r.seedOnce.Do(func() { r.seed = maphash.MakeSeed() })
	return &r.shards[maphash.String(r.seed, clientID)%qos2Shards]
}

// claim registers a QoS 2 PUBLISH before it is handled. A resend (DUP) of
// a registered id returns that entry and resent=true: it is not handled
// again; the caller adds its PUBREC with wait. Anything else gets a new
// entry, replacing a stale one on a DUP 0 reuse of the id; if cl already
// holds that id's quota (inherited), the new entry carries the
// reservation over and the caller takes no second unit.
func (r *qos2Registry) claim(clientID string, id uint16, dup bool, cl *Client) (e *qos2Entry, resent, inherited bool) {
	sh := r.shard(clientID)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.m == nil {
		sh.m = map[string]map[uint16]*qos2Entry{}
	}
	ids := sh.m[clientID]
	if ids == nil {
		ids = map[uint16]*qos2Entry{}
		sh.m[clientID] = ids
	}
	old, ok := ids[id]
	if ok && dup {
		return old, true, false
	}
	e = &qos2Entry{}
	if ok {
		if _, held := old.reserved[cl]; held {
			e.reserved = map[*Client]struct{}{cl: {}}
			inherited = true
		}
	}
	ids[id] = e
	return e, false, inherited
}

// firstReservation reports whether cl holds no reservation for e yet (and
// records that it now does): only then does a reservation take receive
// quota.
func (r *qos2Registry) firstReservation(clientID string, e *qos2Entry, cl *Client) bool {
	sh := r.shard(clientID)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := e.reserved[cl]; ok {
		return false
	}
	if e.reserved == nil {
		e.reserved = map[*Client]struct{}{}
	}
	e.reserved[cl] = struct{}{}
	return true
}

// wait adds w to e: settled now (with e's verdict) if e is, else when e
// is.
func (r *qos2Registry) wait(clientID string, e *qos2Entry, w qos2Waiter) {
	sh := r.shard(clientID)
	sh.mu.Lock()
	if !e.settled {
		e.waiters = append(e.waiters, w)
		sh.mu.Unlock()
		return
	}
	reason := e.reason
	sh.mu.Unlock()
	w.cl.State.acks.settle(w.cl, w.p, reason, false)
}

// settle records e's verdict and settles every waiting PUBREC with it. A
// refusal ends the flow: the entry is dropped (a resend is handled anew).
// reader, when set, is the connection whose own reader is releasing: it may
// write its acks itself (see PublishSettlement.release).
// lowered: a hook handled the copy as QoS 1; no PUBREL will release the
// id, so it is dropped too (a resend answered here is released on its own
// connection).
func (r *qos2Registry) settle(clientID string, id uint16, e *qos2Entry, reason packets.Code, reader *Client, lowered bool) {
	r.finish(clientID, id, e, reason, reader, lowered || reason.Code >= packets.ErrUnspecifiedError.Code)
}

// abandon ends e when its PUBLISH was dropped before being handled (a hook
// refused it in OnPublish, or lowered its QoS): resends already waiting on
// it are settled with that verdict — never left waiting, which would hold
// every later ack of their connection — and the entry is dropped.
func (r *qos2Registry) abandon(clientID string, id uint16, e *qos2Entry, reason packets.Code) {
	r.finish(clientID, id, e, reason, nil, true)
}

func (r *qos2Registry) finish(clientID string, id uint16, e *qos2Entry, reason packets.Code, reader *Client, drop bool) {
	sh := r.shard(clientID)
	sh.mu.Lock()
	e.settled, e.reason = true, reason
	waiters := e.waiters
	e.waiters = nil
	if drop {
		sh.forgetLocked(clientID, id, e)
	}
	sh.mu.Unlock()
	for _, w := range waiters {
		w.cl.State.acks.settle(w.cl, w.p, reason, reader != nil && w.cl == reader)
	}
}

// verdict is the settled verdict of a registered id (ok false: none known
// yet, or not registered) — the answer to a PUBLISH resent after its PUBREC
// was written.
func (r *qos2Registry) verdict(clientID string, id uint16) (reason packets.Code, ok bool) {
	sh := r.shard(clientID)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e := sh.m[clientID][id]; e != nil && e.settled {
		return e.reason, true
	}
	return packets.Code{}, false
}

// holds reports whether e is still the entry registered for the id.
func (r *qos2Registry) holds(clientID string, id uint16, e *qos2Entry) bool {
	sh := r.shard(clientID)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.m[clientID][id] == e
}

// release drops the entry of a released id (PUBREL), whatever it is.
func (r *qos2Registry) release(clientID string, id uint16) {
	sh := r.shard(clientID)
	sh.mu.Lock()
	if ids := sh.m[clientID]; ids != nil {
		delete(ids, id)
		if len(ids) == 0 {
			delete(sh.m, clientID)
		}
	}
	sh.mu.Unlock()
}

func (sh *qos2Shard) forgetLocked(clientID string, id uint16, e *qos2Entry) {
	ids := sh.m[clientID]
	if ids == nil || ids[id] != e {
		return
	}
	delete(ids, id)
	if len(ids) == 0 {
		delete(sh.m, clientID)
	}
}

// endSession drops every entry of a session that ended (clean start,
// expiry): its packet ids mean nothing any more.
func (r *qos2Registry) endSession(clientID string) {
	sh := r.shard(clientID)
	sh.mu.Lock()
	delete(sh.m, clientID)
	sh.mu.Unlock()
}

// size is the number of registered ids (tests).
func (r *qos2Registry) size() int {
	n := 0
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for _, ids := range sh.m {
			n += len(ids)
		}
		sh.mu.Unlock()
	}
	return n
}
