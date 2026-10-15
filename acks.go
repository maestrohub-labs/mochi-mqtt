// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

package mqtt

// MAESTROHUB PATCH (ack after handled): a QoS 1 PUBACK / QoS 2 PUBREC is
// written only once every hook that took the publish's PublishSettlement
// released it, in the order the publishes arrived on the client.
//
// Without it the broker acknowledges a publish before a hook that stores
// the message (MaestroHub's historian) has done so: a crash between the
// two loses a message the client was told the broker has. With it, an
// acknowledgement means "handled", and a client that is not acknowledged
// keeps its message and sends it again.
//
// Order is per client and required by MQTT ([MQTT-4.6.0-2]: PUBACKs in
// the order the PUBLISHes were received). One client's acks wait for its
// slowest pending publish; other clients are not involved — each client
// has its own sequencer.
//
// Releasing a hold never writes to the connection: the goroutine that
// releases belongs to the hook (often a shared worker pool), and a client
// that stops reading its socket would block it — and every other worker
// that settles one of its publishes. Settled acks are handed to the
// client's own ack writer, a goroutine that runs while there are acks to
// write and exits when there are none; it alone blocks on a slow client.

import (
	"sync"
	"sync/atomic"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
)

// PublishSettlement holds back the acknowledgement of one inbound publish
// until every hook that took a hold has released it. Hooks get it in
// OnPublishedSettle.
//
// For a QoS 0 publish, or one from an inline client, there is nothing to
// hold back: Hold returns a release that does nothing, so hooks treat every
// publish the same way.
type PublishSettlement struct {
	cl    *Client
	ack   *pendingAck // nil: nothing to acknowledge
	holds atomic.Int32
	// QoS 2 only: the session's registry entry for the packet id, which
	// settles this PUBREC and every resend's (acks_qos2.go).
	srv  *Server
	qos2 *qos2Entry
	id   uint16
	// failed is the first refusal a release reported (nil: none).
	failed atomic.Pointer[packets.Code]
}

// Hold takes a hold on the publish's acknowledgement and returns the
// function that releases it. It must be called before OnPublishedSettle
// returns; release may be called later, from any goroutine, and must be
// called exactly once (later calls are ignored).
//
// release(code): a code below 0x80 means the hook is done with the message
// (handled, or refused for good and accounted for by the hook). A code of
// 0x80 or above means the hook could not handle it: an MQTT 5 client gets
// that reason code in its PUBACK/PUBREC; an MQTT 3 client, which has no
// reason codes, is disconnected without the acknowledgement, so it sends
// the message again.
func (s *PublishSettlement) Hold() (release func(code packets.Code)) {
	if !s.Withholds() {
		return func(packets.Code) {}
	}
	s.holds.Add(1)
	var once atomic.Bool
	return func(code packets.Code) {
		if once.Swap(true) {
			return
		}
		s.release(code, false)
	}
}

// Withholds reports whether an acknowledgement waits for this settlement:
// a QoS 1/2 publish from a network client. For any other publish Hold is a
// no-op and nobody can be told the message was not handled.
func (s *PublishSettlement) Withholds() bool {
	return s != nil && s.ack != nil
}

// Done is closed when no connection can be told the verdict any more:
// the client the publish came from is gone. For a QoS 2 publish whose
// session another connection has resumed, the verdict still reaches the
// client (a resend of it waits for it, acks_qos2.go), so Done is then
// that connection's. Call it again for a fresh answer after it fires.
// Nil (never closed) when the settlement withholds nothing.
func (s *PublishSettlement) Done() <-chan struct{} {
	if !s.Withholds() || s.cl.State.open == nil {
		return nil
	}
	if s.qos2 != nil && s.cl.Closed() && s.srv != nil {
		if c, ok := s.srv.Clients.Get(s.cl.ID); ok && c != s.cl && !c.Closed() && c.State.open != nil {
			return c.State.open.Done()
		}
	}
	return s.cl.State.open.Done()
}

// release drops one hold. onReader: the caller is the client's own reader
// (the server's hold, released at the end of processPublish), which may
// write the acks itself — blocking only on its own client — instead of
// handing them to the writer goroutine.
func (s *PublishSettlement) release(code packets.Code, onReader bool) {
	if code.Code >= packets.ErrUnspecifiedError.Code {
		s.failed.CompareAndSwap(nil, &code)
	}
	if s.holds.Add(-1) != 0 {
		return
	}
	reason := packets.Code{}
	if f := s.failed.Load(); f != nil {
		reason = *f
	}
	if s.qos2 != nil {
		var reader *Client
		if onReader {
			reader = s.cl
		}
		s.srv.qos2.settle(s.cl.ID, s.id, s.qos2, reason, reader)
		return
	}
	s.cl.State.acks.settle(s.cl, s.ack, reason, onReader)
}

// pendingAck is one acknowledgement waiting for its turn.
type pendingAck struct {
	ack     packets.Packet
	settled bool
	reason  packets.Code // refusal (Code >= 0x80) or zero
	// reserved: the ack of a publish that took receive quota and is
	// handled by hooks (reserve), as opposed to a refusal queued behind
	// the pending ones (writeNowOrQueue).
	reserved bool
}

// ackSequencer writes one client's publish acknowledgements in arrival
// order, each once it is settled. All of a client's QoS 1/2 publish acks go
// through it while any is pending or being written, so a refusal written
// at once cannot overtake an earlier publish still being handled.
type ackSequencer struct {
	mu      sync.Mutex
	pending []*pendingAck
	// ready are settled acks, in order, handed to the writer goroutine;
	// writing is true while it runs (it writes ready, then exits once
	// ready is empty).
	ready   []*pendingAck
	writing bool
	// closed: a refusal an MQTT 3 client cannot be told ended the
	// connection; no ack is written after it ([MQTT-4.6.0-2]: no PUBACK
	// for a later publish without the earlier one's).
	closed bool
	// freed gets a token whenever an ack returned receive quota; the
	// reader waits on it (waitForQuota).
	freed chan struct{}
}

func (q *ackSequencer) init() {
	q.freed = make(chan struct{}, 1)
}

// reserve appends the ack of a publish being handled. Called by the
// client's reader, so the queue order is the arrival order.
func (q *ackSequencer) reserve(ack packets.Packet) *pendingAck {
	p := &pendingAck{ack: ack, reserved: true}
	q.mu.Lock()
	q.pending = append(q.pending, p)
	q.mu.Unlock()
	return p
}

// busy reports whether acknowledgements are waiting (to be settled, or
// to be written).
func (q *ackSequencer) busy() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending) > 0 || len(q.ready) > 0 || q.writing
}

// writeNowOrQueue writes an ack that needs no handling (a refusal decided
// on arrival) — at once if nothing is pending or being written, else
// behind those. Called by the client's reader.
func (q *ackSequencer) writeNowOrQueue(cl *Client, ack packets.Packet) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	if len(q.pending) == 0 && len(q.ready) == 0 && !q.writing {
		q.mu.Unlock()
		return cl.WritePacket(ack)
	}
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		// Nothing to wait for but acks being written: after them.
		q.ready = append(q.ready, &pendingAck{ack: ack, settled: true})
		return nil
	}
	q.pending = append(q.pending, &pendingAck{ack: ack, settled: true})
	return nil
}

// settle marks p settled and hands every settled ack at the head of the
// queue to the client's writer, starting it if it is not running. It
// never writes on a hook's goroutine (see the file comment); called on
// the client's reader (onReader) with no writer running, the reader
// writes them itself — the common case of hooks that settle before
// processPublish returns, at no goroutine's cost.
func (q *ackSequencer) settle(cl *Client, p *pendingAck, reason packets.Code, onReader bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	p.settled, p.reason = true, reason
	if q.closed || !q.flushSettledLocked(cl) || q.writing {
		return
	}
	q.writing = true
	if onReader {
		q.mu.Unlock()
		q.writeReady(cl)
		q.mu.Lock() // for the deferred Unlock
		return
	}
	go q.writeReady(cl)
}

// flushSettledLocked moves the settled prefix of pending to ready and
// reports whether there was one. Caller holds q.mu.
func (q *ackSequencer) flushSettledLocked(cl *Client) bool {
	n := 0
	for n < len(q.pending) && q.pending[n].settled {
		n++
	}
	if n == 0 {
		return false
	}
	q.ready = append(q.ready, q.pending[:n]...)
	q.pending = append(q.pending[:0:0], q.pending[n:]...)
	return true
}

// writeReady is the client's ack writer: it writes the settled acks in
// order until none are left, then exits. Only one runs per client
// (writing), so order holds without a lock around the writes.
func (q *ackSequencer) writeReady(cl *Client) {
	for {
		q.mu.Lock()
		ready := q.ready
		q.ready = nil
		if len(ready) == 0 || q.closed {
			q.writing = false
			q.mu.Unlock()
			return
		}
		q.mu.Unlock()
		if q.write(cl, ready) {
			return // disconnected: what is left is the client's to resend
		}
	}
}

// write writes one run of settled acks; reports whether it ended the
// connection (a failure an MQTT 3 client cannot be told).
func (q *ackSequencer) write(cl *Client, ready []*pendingAck) (disconnected bool) {
	acks := make([]packets.Packet, 0, len(ready))
	reserved := make([]bool, 0, len(ready))
	disconnect := false
	for _, r := range ready {
		if r.reason.Code >= packets.ErrUnspecifiedError.Code {
			if cl.Properties.ProtocolVersion < 5 {
				// No reason codes before MQTT 5: withhold this ack and
				// everything behind it, and end the connection so the
				// client sends them again.
				disconnect = true
				break
			}
			r.ack.ReasonCode = r.reason.Code
			r.ack.Properties.ReasonString = r.reason.Reason
		}
		acks = append(acks, r.ack)
		reserved = append(reserved, r.reserved)
	}
	if disconnect {
		// Closed under the lock: no settle after this writes anything,
		// even before the Stop below has ended the connection.
		q.mu.Lock()
		q.pending, q.ready, q.writing, q.closed = nil, nil, false, true
		q.mu.Unlock()
	}

	if len(acks) > 0 {
		_ = cl.writeAcks(acks) // a closed client: the acks go nowhere, the client resends
		if !cl.Closed() {      // a dead connection's session state is not this client's any more
			for i := range acks {
				if reserved[i] {
					cl.ackWritten(acks[i])
				}
			}
		}
		select {
		case q.freed <- struct{}{}:
		default:
		}
	}
	if disconnect {
		cl.Stop(packets.ErrUnspecifiedError)
	}
	return disconnect
}

// ackWritten does, for an ack the sequencer wrote, what processPublish does
// around an ack it writes at once: a PUBACK ends the QoS 1 flow and returns
// the receive quota; a PUBREC enters the inflight store to await PUBREL (or,
// carrying a refusal, ends the flow).
func (cl *Client) ackWritten(ack packets.Packet) {
	switch ack.FixedHeader.Type {
	case packets.Puback:
		cl.ops.hooks.OnQosPublish(cl, ack, ack.Created, 0)
		cl.State.Inflight.IncreaseReceiveQuota()
		cl.ops.hooks.OnQosComplete(cl, ack)
	case packets.Pubrec:
		if ack.ReasonCode >= packets.ErrUnspecifiedError.Code {
			cl.State.Inflight.IncreaseReceiveQuota() // a refused PUBREC ends the QoS 2 flow
			return
		}
		if ok := cl.State.Inflight.Set(ack); ok {
			atomic.AddInt64(&cl.ops.info.Inflight, 1)
			cl.ops.hooks.OnQosPublish(cl, ack, ack.Created, 0)
		}
	}
}

// waitForQuota blocks the client's reader until an acknowledgement returns
// receive quota, or the client closes (false). Back-pressure for a client
// that sends faster than its publishes are handled: its next packet is not
// read, so its TCP window closes, instead of the connection being dropped.
func (q *ackSequencer) waitForQuota(cl *Client) bool {
	for atomic.LoadInt32(&cl.State.Inflight.receiveQuota) == 0 {
		if !q.busy() {
			return true // nothing pending will return quota; let the caller decide
		}
		select {
		case <-q.freed:
		case <-cl.State.open.Done():
			return false
		}
	}
	return true
}
