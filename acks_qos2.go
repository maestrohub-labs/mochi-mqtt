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
// An entry ends at PUBREL, with a refusal (the flow ends; a resend is a new
// attempt), or with the session. A PUBLISH reusing a registered id without
// the DUP flag is a new message (the client restarted its packet ids): its
// entry replaces the old one. Entries are bounded per session by the
// server's Receive Maximum: each holds receive quota until PUBCOMP.

import (
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
}

// qos2Registry is the server's per-session QoS 2 memory. The zero value is
// ready to use.
type qos2Registry struct {
	mu sync.Mutex
	m  map[string]map[uint16]*qos2Entry
}

// claim registers a QoS 2 PUBLISH before it is handled. A resend (DUP) of
// a registered id returns that entry and resent=true: it is not handled
// again, and the caller adds its PUBREC as a waiter. Anything else gets a
// new entry (replacing a stale one on a DUP 0 reuse of the id).
func (r *qos2Registry) claim(clientID string, id uint16, dup bool) (e *qos2Entry, resent bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]map[uint16]*qos2Entry{}
	}
	ids := r.m[clientID]
	if ids == nil {
		ids = map[uint16]*qos2Entry{}
		r.m[clientID] = ids
	}
	if old, ok := ids[id]; ok && dup {
		return old, true
	}
	e = &qos2Entry{}
	ids[id] = e
	return e, false
}

// wait adds w to e: settled now (with e's verdict) if e is, else when e
// is.
func (r *qos2Registry) wait(e *qos2Entry, w qos2Waiter) {
	r.mu.Lock()
	if !e.settled {
		e.waiters = append(e.waiters, w)
		r.mu.Unlock()
		return
	}
	reason := e.reason
	r.mu.Unlock()
	w.cl.State.acks.settle(w.cl, w.p, reason, false)
}

// settle records e's verdict and settles every waiting PUBREC with it. A
// refusal ends the flow: the entry is dropped (a resend is handled anew).
// onReader is the caller's (see PublishSettlement.release): it applies to
// the waiter on the releasing reader's own connection only.
func (r *qos2Registry) settle(clientID string, id uint16, e *qos2Entry, reason packets.Code, reader *Client) {
	r.mu.Lock()
	e.settled, e.reason = true, reason
	waiters := e.waiters
	e.waiters = nil
	if reason.Code >= packets.ErrUnspecifiedError.Code {
		r.forgetLocked(clientID, id, e)
	}
	r.mu.Unlock()
	for _, w := range waiters {
		w.cl.State.acks.settle(w.cl, w.p, reason, reader != nil && w.cl == reader)
	}
}

// forget drops e (registered under clientID and id) if it is still the
// registered one: its PUBLISH was dropped before being handled, or the id
// was released (PUBREL).
func (r *qos2Registry) forget(clientID string, id uint16, e *qos2Entry) {
	r.mu.Lock()
	r.forgetLocked(clientID, id, e)
	r.mu.Unlock()
}

// release drops the entry of a released id (PUBREL), whatever it is.
func (r *qos2Registry) release(clientID string, id uint16) {
	r.mu.Lock()
	if ids := r.m[clientID]; ids != nil {
		delete(ids, id)
		if len(ids) == 0 {
			delete(r.m, clientID)
		}
	}
	r.mu.Unlock()
}

func (r *qos2Registry) forgetLocked(clientID string, id uint16, e *qos2Entry) {
	ids := r.m[clientID]
	if ids == nil || ids[id] != e {
		return
	}
	delete(ids, id)
	if len(ids) == 0 {
		delete(r.m, clientID)
	}
}

// endSession drops every entry of a session that ended (clean start,
// expiry): its packet ids mean nothing any more.
func (r *qos2Registry) endSession(clientID string) {
	r.mu.Lock()
	delete(r.m, clientID)
	r.mu.Unlock()
}

// size is the number of registered ids (tests).
func (r *qos2Registry) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, ids := range r.m {
		n += len(ids)
	}
	return n
}
