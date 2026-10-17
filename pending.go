// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 MaestroHub

package mqtt

// MAESTROHUB PATCH (subscriber queue, FORK.md patch 12): a message the
// server cannot hand to a subscriber right now waits in that subscriber's
// pending queue instead of being dropped.
//
// Upstream dropped the NEW message for a subscriber whenever it could not
// take one more: its in-flight store held MaximumInflight QoS 1/2 messages
// (WARN "client store quota reached"), no packet id was free (WARN "packet
// ids exhausted"), or its outbound channel held MaximumClientWritesPending
// messages (OnPublishDropped). A message beyond the client's own Receive
// Maximum was parked in the in-flight store with Expiry -1, sent later in
// the order of its creation SECOND, and deleted from the store once sent —
// so its PUBACK found nothing, never returned the send quota, and after
// Receive Maximum such messages the client stalled for good.
//
// Now every one of those conditions queues the message, in arrival order,
// in a per-client FIFO bounded by Capabilities.MaximumClientPendingMessages.
// When the queue is full the OLDEST queued message is dropped (counted in
// Info.MessagesDropped, reported through OnPublishDropped), so a slow
// subscriber always receives the newest values. Once anything is queued,
// later messages queue behind it, so delivery order is arrival order. The
// queue is drained, oldest first, whenever room may have appeared: after
// the client's reader processes a packet (a PUBACK, PUBREC or PUBCOMP frees
// an in-flight slot and returns send quota), after WriteLoop writes an
// outbound packet (a slot in the outbound channel), after the in-flight
// expiry sweep, and after a resumed session's in-flight messages are resent.

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
)

// pendingQueue is a client's FIFO of messages waiting for room. A ring of
// pointers grown on demand up to the limit and released when it empties, so
// an idle client holds no buffer and a slow one at most limit pointers plus
// the queued packets (whose payloads are shared with every other
// subscriber's copy).
type pendingQueue struct {
	mu   sync.Mutex
	buf  []*packets.Packet
	head int
	n    int
	size atomic.Int32 // n, for the lock-free emptiness check on hot paths

	// held: the session is resuming and its in-flight messages have not
	// been re-sent yet, so nothing is handed out — a new message queues
	// behind the backlog instead of overtaking it [MQTT-4.6.0-1]. Under mu.
	// MAESTROHUB PATCH 13.
	held bool
}

// Len returns the number of queued messages.
func (q *pendingQueue) Len() int {
	return int(q.size.Load())
}

// push appends pk. If the queue already holds limit (at least 1) messages,
// the oldest is removed first and returned. Callers hold q.mu.
func (q *pendingQueue) push(pk *packets.Packet, limit int) (evicted *packets.Packet) {
	if q.n >= limit {
		evicted = q.pop()
	}
	if q.n == len(q.buf) {
		q.grow(limit)
	}
	q.buf[(q.head+q.n)%len(q.buf)] = pk
	q.n++
	q.size.Store(int32(q.n))
	return evicted
}

// grow doubles the ring (at least 16 slots, never more than limit),
// keeping the order. Callers hold q.mu.
func (q *pendingQueue) grow(limit int) {
	c := 2 * len(q.buf)
	if c < 16 {
		c = 16
	}
	if c > limit {
		c = limit
	}
	nb := make([]*packets.Packet, c)
	for i := 0; i < q.n; i++ {
		nb[i] = q.buf[(q.head+i)%len(q.buf)]
	}
	q.buf, q.head = nb, 0
}

// front returns the oldest queued message, or nil. Callers hold q.mu.
func (q *pendingQueue) front() *packets.Packet {
	if q.n == 0 {
		return nil
	}
	return q.buf[q.head]
}

// pop removes and returns the oldest queued message, or nil. The ring is
// released when it empties. Callers hold q.mu.
func (q *pendingQueue) pop() *packets.Packet {
	if q.n == 0 {
		return nil
	}
	pk := q.buf[q.head]
	q.buf[q.head] = nil
	q.head = (q.head + 1) % len(q.buf)
	q.n--
	if q.n == 0 {
		q.buf, q.head = nil, 0
	}
	q.size.Store(int32(q.n))
	return pk
}

// takeAll empties the queue and returns its messages, oldest first.
func (q *pendingQueue) takeAll() []*packets.Packet {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]*packets.Packet, 0, q.n)
	for q.n > 0 {
		out = append(out, q.pop())
	}
	return out
}

// pendingLimit is the most messages one client may have queued. Options
// set it (ensureDefaults); a client built without them gets the default.
func (cl *Client) pendingLimit() int {
	if n := cl.ops.options.Capabilities.MaximumClientPendingMessages; n > 0 {
		return int(n)
	}
	return defaultMaximumClientPendingMessages
}

// publish hands out to the client now if it can take it and nothing is
// queued ahead of it, and queues it otherwise. It returns the packet as
// handed out (with its packet id), or as queued; the error is
// packets.CodeDisconnect when the client is not connected, as upstream.
func (cl *Client) publish(out packets.Packet) (packets.Packet, error) {
	closed := cl.Net.Conn == nil || cl.Closed()
	if closed && out.FixedHeader.Qos == 0 {
		return out, packets.CodeDisconnect // QoS 0 is not kept for a session without a connection
	}

	q := &cl.State.pending
	q.mu.Lock()
	if q.n == 0 && !q.held {
		if ok, err := cl.deliverNow(&out); ok {
			q.mu.Unlock()
			return out, err
		}
	}
	queued := out
	evicted := q.push(&queued, cl.pendingLimit())
	q.mu.Unlock()

	if evicted != nil {
		cl.droppedOldest(*evicted)
	}
	if closed {
		return out, packets.CodeDisconnect
	}
	return out, nil
}

// droppedOldest accounts for a queued message dropped to make room for a
// newer one.
func (cl *Client) droppedOldest(pk packets.Packet) {
	atomic.AddInt64(&cl.ops.info.MessagesDropped, 1)
	cl.ops.hooks.OnPublishDropped(cl, pk)
}

// deliverNow hands out to the client if it can take it now: room in its
// outbound channel and, for QoS 1/2, room in its in-flight store, send
// quota left under the client's Receive Maximum, and a free packet id.
// ok is false, and out untouched, when it cannot; out must then be queued.
// For a client without a connection a QoS 1/2 message goes to the in-flight
// store only (resent when the session resumes), with err
// packets.CodeDisconnect, as upstream. Callers hold cl.State.pending.mu,
// which makes the outbound-room check exact: every send to the channel is
// made under it, and WriteLoop only takes from it.
func (cl *Client) deliverNow(out *packets.Packet) (ok bool, err error) {
	closed := cl.Net.Conn == nil || cl.Closed()
	if !closed && len(cl.State.outbound) >= cap(cl.State.outbound) {
		return false, packets.ErrPendingClientWritesExceeded
	}

	if out.FixedHeader.Qos > 0 {
		if cl.State.Inflight.Len() >= int(cl.ops.options.Capabilities.MaximumInflight) {
			return false, packets.ErrQuotaExceeded
		}
		if atomic.LoadInt32(&cl.State.Inflight.maximumSendQuota) > 0 && atomic.LoadInt32(&cl.State.Inflight.sendQuota) <= 0 {
			return false, packets.ErrQuotaExceeded // [MQTT-4.9.0-1] the client's Receive Maximum
		}
		id, err := cl.NextPacketID() // [MQTT-4.3.2-1] [MQTT-4.3.3-1]
		if err != nil {
			cl.ops.hooks.OnPacketIDExhausted(cl, *out)
			return false, packets.ErrQuotaExceeded
		}
		out.PacketID = uint16(id) // [MQTT-2.2.1-4]
	}

	// The alias is assigned here, not when the message arrived: a message
	// that establishes an alias must be one that is actually sent, never
	// one that waits in the queue and may be dropped from it.
	if cl.Properties.Props.TopicAliasMaximum > 0 {
		var aliasExists bool
		out.Properties.TopicAlias, aliasExists = cl.State.TopicAliases.Outbound.Set(out.TopicName)
		if out.Properties.TopicAlias > 0 {
			out.Properties.TopicAliasFlag = true
			if aliasExists {
				out.TopicName = ""
			}
		}
	}

	if out.FixedHeader.Qos > 0 {
		if ok := cl.State.Inflight.Set(*out); ok { // [MQTT-4.3.2-3] [MQTT-4.3.3-3]
			atomic.AddInt64(&cl.ops.info.Inflight, 1)
			cl.ops.hooks.OnQosPublish(cl, *out, out.Created, 0)
			cl.State.Inflight.DecreaseSendQuota()
		}
	}

	if closed {
		return true, packets.CodeDisconnect
	}

	cl.State.outbound <- out // never blocks: room was checked under the lock every sender holds
	atomic.AddInt32(&cl.State.outboundQty, 1)
	return true, nil
}

// drainPending hands queued messages to the client, oldest first, for as
// long as it can take them. A message whose expiry has passed is deleted
// instead ([MQTT-3.3.2-5]).
func (cl *Client) drainPending() {
	q := &cl.State.pending
	if q.Len() == 0 {
		return
	}
	// A client without a connection keeps its queue as it is: the queue
	// moves whole to the connection that resumes the session
	// (inheritClientSession). Moving queued QoS 1/2 messages into this
	// client's in-flight store instead would lose them whenever it happens
	// after a takeover cloned that store — and the closed client's own
	// WriteLoop and the expiry sweep do drain at that moment.
	if cl.Net.Conn == nil || cl.Closed() {
		return
	}

	now := time.Now().Unix()
	maxExpiry := cl.ops.options.Capabilities.MaximumMessageExpiryInterval

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.held {
		return // resuming: the backlog is re-sent first (releasePending)
	}
	for pk := q.front(); pk != nil; pk = q.front() {
		if pendingExpired(pk, now, maxExpiry) {
			q.pop()
			continue
		}
		if ok, _ := cl.deliverNow(pk); !ok {
			return
		}
		q.pop()
	}
}

// holdPending stops handing messages out until releasePending: set while a
// resumed session's backlog has not been re-sent. MAESTROHUB PATCH 13.
func (cl *Client) holdPending() {
	cl.State.pending.mu.Lock()
	cl.State.pending.held = true
	cl.State.pending.mu.Unlock()
}

// releasePending ends holdPending and hands out what queued meanwhile.
func (cl *Client) releasePending() {
	cl.State.pending.mu.Lock()
	cl.State.pending.held = false
	cl.State.pending.mu.Unlock()
	cl.drainPending()
}

// pendingExpired reports whether a queued message may no longer be
// delivered: its Message Expiry Interval has passed, or it is older than
// the server's maximum message expiry (as for in-flight messages,
// ClearExpiredInflights).
func pendingExpired(pk *packets.Packet, now, maximumExpiry int64) bool {
	expired := pk.Expiry > 0 && pk.Expiry < now
	enforced := maximumExpiry > 0 && now-pk.Created > maximumExpiry
	return expired || enforced
}

// RemoveQueued removes from the client's session every queued PUBLISH for
// which remove returns true — in its in-flight store (sent or waiting to be
// resent, not yet acknowledged) and in its pending queue — and returns how
// many it removed. Every other packet stays, in its order: a PUBREL the
// client is owed, an inbound QoS 2 flow, the publishes remove keeps.
//
// For an embedder that re-decides what a session may receive, typically a
// persistent session about to resume (OnSessionEstablish, on the session
// still held under the client id): a permission withdrawn while it was
// offline then withholds what was queued for it before. A removed in-flight
// message returns its send quota and is reported through OnQosDropped, as
// when a session's in-flight messages are cleared; a removed pending one
// is not reported (it was never handed out). MAESTROHUB PATCH 13.
func (cl *Client) RemoveQueued(remove func(pk packets.Packet) bool) int {
	q := &cl.State.pending
	// Held throughout: every move from the queue into the in-flight store
	// (deliverNow) happens under it, so no message slips between the two
	// passes.
	q.mu.Lock()
	defer q.mu.Unlock()

	n := 0
	for _, pk := range cl.State.Inflight.GetAll(false) {
		if pk.FixedHeader.Type != packets.Publish || !remove(pk) {
			continue
		}
		if cl.State.Inflight.Delete(pk.PacketID) {
			if pk.FixedHeader.Qos > 0 {
				cl.State.Inflight.IncreaseSendQuota()
			}
			atomic.AddInt64(&cl.ops.info.Inflight, -1)
			cl.ops.hooks.OnQosDropped(cl, pk)
			n++
		}
	}

	if q.n == 0 {
		return n
	}
	kept := make([]*packets.Packet, 0, q.n)
	for pk := q.pop(); pk != nil; pk = q.pop() {
		if remove(*pk) {
			n++
			continue
		}
		kept = append(kept, pk)
	}
	limit := cl.pendingLimit()
	for _, pk := range kept {
		q.push(pk, limit)
	}
	return n
}
