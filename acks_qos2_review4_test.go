package mqtt

import (
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

// A clean-start takeover removes the old session's subscriptions from the
// topic index ([MQTT-3.1.2-4]) although the old client is marked taken over
// first (UnsubscribeClient skips the index for such a client).
func TestCleanTakeoverDropsOldSubscriptions(t *testing.T) {
	s := newServer()
	existing, er, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, er) }()
	existing.ID = "mochi"
	existing.Properties.ProtocolVersion = 5
	sub := packets.Subscription{Filter: "a/b/c", Qos: 1}
	existing.State.Subscriptions.Add(sub.Filter, sub)
	s.Topics.Subscribe(existing.ID, sub)
	atomic.AddInt64(&s.Info.Subscriptions, 1)
	s.Clients.Add(existing)

	cl, _, _ := newTestClient()
	cl.ID = "mochi"
	cl.Properties.ProtocolVersion = 5
	require.False(t, s.inheritClientSession(packets.Packet{Connect: packets.ConnectParams{ClientIdentifier: "mochi", Clean: true}}, cl))
	subs := s.Topics.Subscribers("a/b/c")
	require.Equal(t, 0, len(subs.Subscriptions), "clean start: old session's subscription still routes to client id %v", subs.Subscriptions)
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.Subscriptions))
}

func newLowerServer(t *testing.T) (*Server, *lowerHook) {
	s := newServer()
	h := &lowerHook{settleHook: *newSettleHook(), gate: make(chan struct{}), entered: make(chan struct{}), to: 1}
	require.NoError(t, s.AddHook(h, nil))
	return s, h
}

// Lowered to QoS 1, resend waiting on the resumed connection: verdict, flow, quota, registry.
func TestQos2LoweredResendFullFlow(t *testing.T) {
	for _, code := range []packets.Code{packets.CodeSuccess, packets.ErrUnspecifiedError} {
		s, h := newLowerServer(t)
		old := connClient(t, s)
		old.ID = "low"
		done := make(chan error, 1)
		go func() { done <- s.processPacket(old, publishQos(7, 2)) }()
		<-h.entered
		cl, wire := takeover(t, s, old)
		max := atomic.LoadInt32(&cl.State.Inflight.maximumReceiveQuota)
		require.NoError(t, s.processPacket(cl, qos2Dup(7)))
		close(h.gate)
		require.NoError(t, <-done)
		expectNone(t, wire)
		h.releaseFor(t, "low", 7, code)
		p := expectAck(t, wire, packets.Pubrec, 7)
		require.Equal(t, code.Code, p.reason)
		require.Equal(t, 0, s.qos2.size(), "lowered entry leaked")
		require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
		if code == packets.CodeSuccess {
			require.NoError(t, s.processPacket(cl, pubrelPk(7)))
			p = expectAck(t, wire, packets.Pubcomp, 7)
			require.Equal(t, byte(0), p.reason)
		}
		require.Equal(t, max, atomic.LoadInt32(&cl.State.Inflight.receiveQuota), "code %v", code)
		require.Equal(t, 0, cl.State.Inflight.Len())
	}
}

// Lowered to QoS 1, no resend: entry freed at settle.
func TestQos2LoweredNoResendFreesEntry(t *testing.T) {
	s, h := newLowerServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)
	max := atomic.LoadInt32(&cl.State.Inflight.receiveQuota)
	go func() { <-h.entered; close(h.gate) }()
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	require.Equal(t, 1, s.qos2.size())
	// resend on the same connection while the lowered copy is held
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	require.Equal(t, int32(1), h.seen.Load())
	h.release(t, 7, packets.CodeSuccess)
	expectAck(t, wire, packets.Puback, 7)
	p := expectAck(t, wire, packets.Pubrec, 7)
	require.Equal(t, byte(0), p.reason)
	require.Equal(t, 0, s.qos2.size())
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
	require.NoError(t, s.processPacket(cl, pubrelPk(7)))
	p = expectAck(t, wire, packets.Pubcomp, 7)
	require.Equal(t, byte(0), p.reason)
	require.Equal(t, max, atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
}

// Three copies on one connection: three PUBRECs, one inflight entry, and
// PUBREL answered 0x00 then 0x92.
func TestQos2PlainAnswersPubrelFlow(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)
	max := atomic.LoadInt32(&cl.State.Inflight.receiveQuota)
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	require.Equal(t, max-1, atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
	h.release(t, 7, packets.CodeSuccess)
	for i := 0; i < 3; i++ {
		p := expectAck(t, wire, packets.Pubrec, 7)
		require.Equal(t, byte(0), p.reason)
	}
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
	require.NoError(t, s.processPacket(cl, pubrelPk(7)))
	p := expectAck(t, wire, packets.Pubcomp, 7)
	require.Equal(t, byte(0), p.reason)
	require.NoError(t, s.processPacket(cl, pubrelPk(7)))
	p = expectAck(t, wire, packets.Pubcomp, 7)
	require.Equal(t, packets.ErrPacketIdentifierNotFound.Code, p.reason)
	require.Equal(t, max, atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
	require.Equal(t, 0, cl.State.Inflight.Len())
	require.Equal(t, 0, s.qos2.size())
}

// A DUP 0 reuse of an id while its first copy is held: both handled, one
// receive quota unit taken for the id on this connection, returned by its
// one PUBREL.
func TestQos2Dup0WhileHeldKeepsQuota(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)
	max := atomic.LoadInt32(&cl.State.Inflight.receiveQuota)
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	h.mu.Lock()
	first := h.releases[key("mochi", 7)]
	h.mu.Unlock()
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	first(packets.CodeSuccess)
	h.release(t, 7, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 7)
	expectAck(t, wire, packets.Pubrec, 7)
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
	require.NoError(t, s.processPacket(cl, pubrelPk(7)))
	expectAck(t, wire, packets.Pubcomp, 7)
	require.Equal(t, int32(2), h.seen.Load())
	require.Equal(t, 0, s.qos2.size())
	require.Equal(t, max, atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
}
