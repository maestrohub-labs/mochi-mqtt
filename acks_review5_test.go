package mqtt

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

// sweepOnDisconnect runs the expiry sweep while the connection's tail is
// between OnDisconnect and its deleteIf (the sweep ticker firing there).
type sweepOnDisconnect struct {
	HookBase
	s *Server
}

func (h *sweepOnDisconnect) ID() string           { return "sweep-on-disconnect" }
func (h *sweepOnDisconnect) Provides(b byte) bool { return b == OnDisconnect }
func (h *sweepOnDisconnect) OnDisconnect(cl *Client, err error, expire bool) {
	h.s.clearExpiredClients(time.Now().Unix() + 5)
}

func TestReview5SweepDuringTailLeaksSubscriptions(t *testing.T) {
	s := newServer()
	s.Options.Capabilities.MaximumSessionExpiryInterval = 0 // as a v5 client with Session Expiry Interval 0 set
	require.NoError(t, s.AddHook(&sweepOnDisconnect{s: s}, nil))
	defer s.Close()
	r, w := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", r) }()
	go func() { _, _ = io.Copy(io.Discard, w) }()
	connect := packets.TPacketData[packets.Connect].Get(packets.TConnectClean)
	_, _ = w.Write(connect.RawBytes)
	id := connect.Packet.Connect.ClientIdentifier
	require.Eventually(t, func() bool { _, ok := s.Clients.Get(id); return ok }, time.Second, time.Millisecond)
	sub := packets.TPacketData[packets.Subscribe].Get(packets.TSubscribe)
	_, _ = w.Write(sub.RawBytes)
	filter := sub.Packet.Filters[0].Filter
	require.Eventually(t, func() bool { return len(s.Topics.Subscribers(filter).Subscriptions) == 1 }, time.Second, time.Millisecond)
	_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("no end")
	}
	_, ok := s.Clients.Get(id)
	require.False(t, ok)
	require.Equal(t, 0, len(s.Topics.Subscribers(filter).Subscriptions), "ended clean session still routes %q to %q", filter, id)
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.Subscriptions))
}

// Pre-existing (upstream): the sweep expiring a session never removes its
// filters from the topic index; a new clean session under the id inherits
// the routing.
func TestReview5SweepExpiryLeavesIndex(t *testing.T) {
	s := newServer()
	old := connClient(t, s)
	old.ID = "exp"
	sub := packets.Subscription{Filter: "a/b/c", Qos: 1}
	old.State.Subscriptions.Add(sub.Filter, sub)
	s.Topics.Subscribe(old.ID, sub)
	s.Clients.Add(old)
	old.Properties.Props.SessionExpiryInterval, old.Properties.Props.SessionExpiryIntervalFlag = 1, true
	old.Stop(nil)
	old.State.disconnected = time.Now().Unix() - 10
	s.clearExpiredClients(time.Now().Unix())
	_, ok := s.Clients.Get("exp")
	require.False(t, ok)
	require.Equal(t, 0, len(s.Topics.Subscribers("a/b/c").Subscriptions), "expired session still routes")
}

// The old copy refused, the DUP 0 reuse handled: its success PUBREC is an
// answer only, so nothing awaits the PUBREL.
func TestReview5Dup0WhileHeldFirstRefused(t *testing.T) {
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
	first(packets.ErrUnspecifiedError)
	h.release(t, 7, packets.CodeSuccess)
	p := expectAck(t, wire, packets.Pubrec, 7)
	require.Equal(t, packets.ErrUnspecifiedError.Code, p.reason)
	p = expectAck(t, wire, packets.Pubrec, 7)
	require.Equal(t, byte(0), p.reason)
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
	t.Logf("inflight=%d quota=%d max=%d", cl.State.Inflight.Len(), atomic.LoadInt32(&cl.State.Inflight.receiveQuota), max)
	require.NoError(t, s.processPacket(cl, pubrelPk(7)))
	p = expectAck(t, wire, packets.Pubcomp, 7)
	require.Equal(t, byte(0), p.reason, "PUBREL of the stored copy answered 0x92")
	require.Equal(t, max, atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
}

type lowerSecond struct {
	settleHook
	n atomic.Int32
}

func (h *lowerSecond) Provides(b byte) bool { return b == OnPublishedSettle || b == OnPublish }
func (h *lowerSecond) OnPublish(cl *Client, pk packets.Packet) (packets.Packet, error) {
	if pk.PacketID == 7 && !pk.FixedHeader.Dup && h.n.Add(1) == 2 {
		pk.FixedHeader.Qos = 1
	}
	return pk, nil
}

// The DUP 0 reuse lowered to QoS 1: no unit taken (inherited) but its
// PUBACK returns one while the first copy still holds its unit.
func TestReview5Dup0WhileHeldLoweredQuota(t *testing.T) {
	s := newServer()
	h := &lowerSecond{settleHook: *newSettleHook()}
	require.NoError(t, s.AddHook(h, nil))
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)
	max := atomic.LoadInt32(&cl.State.Inflight.receiveQuota)
	t.Logf("max=%d", max)
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	h.mu.Lock()
	first := h.releases[key("mochi", 7)]
	h.mu.Unlock()
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	first(packets.CodeSuccess)
	h.release(t, 7, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 7)
	expectAck(t, wire, packets.Puback, 7)
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
	require.Equal(t, max-1, atomic.LoadInt32(&cl.State.Inflight.receiveQuota), "PUBREC 7 outstanding holds one unit")
}
