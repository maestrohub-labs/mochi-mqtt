// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

// Reproductions from the second independent review of patch 10 (R1-R3,
// F3/F5 end to end, a release/takeover/PUBREL stress), kept as tests.

package mqtt

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

func pubrelPk(id uint16) packets.Packet {
	p := *packets.TPacketData[packets.Pubrel].Get(packets.TPubrel).Packet
	p.PacketID = id
	return p
}

// R1: a same-connection QoS 2 resend while the PUBREC is held decreases the
// receive quota a second time; only one PUBREL returns it.
func TestReview_SameConnResendLeaksReceiveQuota(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 4
	wire := readWire(r)
	before := atomic.LoadInt32(&cl.State.Inflight.receiveQuota)

	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	h.release(t, 7, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 7)
	expectAck(t, wire, packets.Pubrec, 7)
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
	require.NoError(t, s.processPacket(cl, pubrelPk(7)))
	expectAck(t, wire, packets.Pubcomp, 7)
	after := atomic.LoadInt32(&cl.State.Inflight.receiveQuota)
	require.Equal(t, 0, s.qos2.size())
	require.Equal(t, before, after, "receive quota leaked by the same-connection resend")
}

// R1b: repeat until the client is disconnected for exceeding Receive Maximum.
func TestReview_SameConnResendLeakDisconnects(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 4
	wire := readWire(r)
	for i := uint16(1); i <= 12; i++ {
		require.NoError(t, s.processPacket(cl, publishQos(i, 2)))
		errc := make(chan error, 1)
		go func() { errc <- s.processPacket(cl, qos2Dup(i)) }()
		time.Sleep(20 * time.Millisecond)
		h.release(t, i, packets.CodeSuccess)
		err := <-errc
		if err != nil || cl.Closed() {
			t.Fatalf("compliant client disconnected after %d exchanges: %v (quota=%d)", i-1, err, atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
		}
		expectAck(t, wire, packets.Pubrec, i)
		expectAck(t, wire, packets.Pubrec, i)
		require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
		require.NoError(t, s.processPacket(cl, pubrelPk(i)))
		expectAck(t, wire, packets.Pubcomp, i)
	}
}

// rejectHook: OnPublish blocks the first copy of "victim"/7 and then
// rejects it (MaestroHub's disk floor, MQTT 3.1.1 → ErrRejectPacket).
type rejectHook struct {
	settleHook
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (h *rejectHook) Provides(b byte) bool { return b == OnPublishedSettle || b == OnPublish }
func (h *rejectHook) OnPublish(cl *Client, pk packets.Packet) (packets.Packet, error) {
	if pk.PacketID == 7 && !pk.FixedHeader.Dup {
		first := false
		h.once.Do(func() { first = true })
		if first {
			close(h.entered)
			<-h.gate
			return pk, packets.ErrRejectPacket
		}
	}
	return pk, nil
}

// R2: the original copy is dropped in OnPublish while a resend on the
// resumed connection already waits on its entry: the waiter is never
// settled and blocks every later ack of the connection.
func TestReview_StrandedWaiterWhenOriginalDroppedInOnPublish(t *testing.T) {
	s := newServer()
	h := &rejectHook{settleHook: *newSettleHook(), gate: make(chan struct{}), entered: make(chan struct{})}
	require.NoError(t, s.AddHook(h, nil))
	old := connClient(t, s)
	old.ID = "victim"
	done := make(chan error, 1)
	go func() { done <- s.processPacket(old, publishQos(7, 2)) }()
	<-h.entered

	cl, wire := takeover(t, s, old)
	require.NoError(t, s.processPacket(cl, qos2Dup(7))) // registered as a waiter on the original
	close(h.gate)
	require.NoError(t, <-done)

	require.NoError(t, s.processPacket(cl, publishQos(8, 1)))
	h.releaseFor(t, "victim", 8, packets.CodeSuccess)
	select {
	case p := <-wire:
		t.Logf("got %+v", p)
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("no ack on the resumed connection: the resend's PUBREC is stranded (registry size %d) and PUBACK 8 is stuck behind it", s.qos2.size())
	}
}

// R3: a resend on a new connection that establishes a topic alias returns
// before the alias is registered.
type topicHook struct {
	settleHook
	mu     sync.Mutex
	topics []string
}

func (h *topicHook) OnPublishedSettle(cl *Client, pk packets.Packet, st *PublishSettlement) {
	h.mu.Lock()
	h.topics = append(h.topics, pk.TopicName)
	h.mu.Unlock()
	h.settleHook.OnPublishedSettle(cl, pk, st)
}

func TestReview_ResendSkipsTopicAlias(t *testing.T) {
	s := newServer()
	h := &topicHook{settleHook: *newSettleHook()}
	require.NoError(t, s.AddHook(h, nil))
	old := connClient(t, s)
	old.ID = "alias"
	first := publishQos(7, 2)
	first.TopicName = "a/b"
	require.NoError(t, s.processPacket(old, first))

	cl, _ := takeover(t, s, old)
	re := qos2Dup(7)
	re.TopicName = "a/b"
	re.Properties.TopicAlias, re.Properties.TopicAliasFlag = 1, true
	require.NoError(t, s.processPacket(cl, re))

	next := publishQos(8, 1)
	next.TopicName = ""
	next.Properties.TopicAlias, next.Properties.TopicAliasFlag = 1, true
	require.NoError(t, s.processPacket(cl, next))
	h.mu.Lock()
	defer h.mu.Unlock()
	require.Equal(t, []string{"a/b", "a/b"}, h.topics, "the alias the resend established was not registered")
}

// F3-style: the old reader takes in one more QoS 2 publish after the
// takeover; the resend on the new connection must be answered by it, once.
func TestReview_OldReaderAfterTakeover(t *testing.T) {
	s, h := newSettleServer(t)
	old := connClient(t, s)
	old.ID = "late"
	cl, wire := takeover(t, s, old)
	require.NoError(t, s.processPacket(old, publishQos(9, 2))) // the old reader's last packet
	require.NoError(t, s.processPacket(cl, qos2Dup(9)))
	require.Equal(t, int32(1), h.seen.Load())
	h.releaseFor(t, "late", 9, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 9)
	require.Eventually(t, func() bool { _, ok := cl.State.Inflight.Get(9); return ok }, time.Second, time.Millisecond)
	require.NoError(t, s.processPacket(cl, pubrelPk(9)))
	expectAck(t, wire, packets.Pubcomp, 9)
	require.Equal(t, 0, s.qos2.size())
}

// F5 end to end: MQTT 3 refusal of 1 must withhold the later success of 2.
func TestReview_Mqtt3RefusalWithholdsLater(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 4
	wire := readWire(r)
	require.NoError(t, s.processPacket(cl, publishQos(1, 2)))
	require.NoError(t, s.processPacket(cl, publishQos(2, 2)))
	require.NoError(t, s.processPacket(cl, qos2Dup(2))) // waiter on the same conn
	h.release(t, 2, packets.CodeSuccess)
	h.release(t, 1, packets.ErrUnspecifiedError)
	for {
		select {
		case p, ok := <-wire:
			if !ok {
				return
			}
			t.Fatalf("ack written after an MQTT 3 refusal: %+v", p)
		case <-time.After(300 * time.Millisecond):
			require.True(t, cl.Closed())
			return
		}
	}
}

// Stress: release vs takeover vs resend vs PUBREL.
func TestReview_StressReleaseTakeoverPubrel(t *testing.T) {
	for i := 0; i < 300; i++ {
		s, h := newSettleServer(t)
		old := connClient(t, s)
		old.ID = "st"
		require.NoError(t, s.processPacket(old, publishQos(7, 2)))
		require.NoError(t, s.processPacket(old, publishQos(8, 2)))
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); h.releaseFor(t, "st", 7, packets.CodeSuccess) }()
		go func() { defer wg.Done(); h.releaseFor(t, "st", 8, packets.CodeSuccess) }()
		cl, wire := takeover(t, s, old)
		quota := atomic.LoadInt32(&cl.State.Inflight.receiveQuota)
		require.NoError(t, s.processPacket(cl, qos2Dup(7)))
		require.NoError(t, s.processPacket(cl, qos2Dup(8)))
		wg.Wait()
		expectAck(t, wire, packets.Pubrec, 7)
		expectAck(t, wire, packets.Pubrec, 8)
		require.Equal(t, int32(2), h.seen.Load())
		require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
		require.NoError(t, s.processPacket(cl, pubrelPk(7)))
		require.NoError(t, s.processPacket(cl, pubrelPk(8)))
		expectAck(t, wire, packets.Pubcomp, 7)
		expectAck(t, wire, packets.Pubcomp, 8)
		require.Equal(t, 0, s.qos2.size(), "iter %d", i)
		require.Equal(t, quota, atomic.LoadInt32(&cl.State.Inflight.receiveQuota), "iter %d quota", i)
	}
}

func BenchmarkReview_RegistryParallel(b *testing.B) {
	var r qos2Registry
	var n atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		id := "c" + string(rune('a'+n.Add(1)))
		var pid uint16
		for pb.Next() {
			pid++
			e, _, _ := r.claim(id, pid, false, nil)
			r.settle(id, pid, e, packets.CodeSuccess, nil, false)
			r.release(id, pid)
		}
	})
}
