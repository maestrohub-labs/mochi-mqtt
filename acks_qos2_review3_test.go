package mqtt

import (
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

// A1: same-connection resend + refusal: the quota was taken once, but
// two refused PUBRECs each return one unit.
func TestR3_SameConnRefusalReturnsQuotaTwice(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)
	max := atomic.LoadInt32(&cl.State.Inflight.receiveQuota)

	require.NoError(t, s.processPacket(cl, publishQos(1, 2))) // flow 1 stays open
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	require.Equal(t, max-2, atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
	h.release(t, 1, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 1)
	h.release(t, 7, packets.ErrUnspecifiedError)
	p := expectAck(t, wire, packets.Pubrec, 7)
	require.Equal(t, byte(0x80), p.reason)
	p = expectAck(t, wire, packets.Pubrec, 7)
	require.Equal(t, byte(0x80), p.reason)
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
	// flow 1 is still open (PUBREC written, no PUBREL): quota must be max-1
	require.Equal(t, max-1, atomic.LoadInt32(&cl.State.Inflight.receiveQuota), "refused resend returned quota it never took")
}

// A1b: same on the resuming connection: two resends there, one unit taken.
func TestR3_TakeoverTwoResendsRefusedReturnsQuotaTwice(t *testing.T) {
	s, h := newSettleServer(t)
	old := connClient(t, s)
	old.ID = "q"
	require.NoError(t, s.processPacket(old, publishQos(7, 2)))
	cl, wire := takeover(t, s, old)
	max := atomic.LoadInt32(&cl.State.Inflight.receiveQuota)
	require.NoError(t, s.processPacket(cl, publishQos(1, 2))) // open flow on the new conn
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	require.Equal(t, max-2, atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
	h.releaseFor(t, "q", 1, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 1)
	h.releaseFor(t, "q", 7, packets.ErrUnspecifiedError)
	expectAck(t, wire, packets.Pubrec, 7)
	expectAck(t, wire, packets.Pubrec, 7)
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
	require.Equal(t, max-1, atomic.LoadInt32(&cl.State.Inflight.receiveQuota), "refused resends returned quota twice")
}

// A2: a client PUBREL carrying a failure reason (MQTT 5 0x92) ends the flow
// (inflight deleted) but leaves the registry entry: a later DUP copy of a
// NEW message with that id is answered from the stale entry, never handled.
func TestR3_PubrelWithReasonLeavesStaleEntry(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	h.release(t, 7, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 7)
	require.Eventually(t, func() bool { _, ok := cl.State.Inflight.Get(7); return ok }, time.Second, time.Millisecond)
	rel := pubrelPk(7)
	rel.ReasonCode = packets.ErrPacketIdentifierNotFound.Code
	require.NoError(t, s.processPacket(cl, rel))
	_, inflight := cl.State.Inflight.Get(7)
	t.Logf("after PUBREL 0x92: inflight=%v registry=%d", inflight, s.qos2.size())
	require.NoError(t, s.processPacket(cl, qos2Dup(7))) // new message, its first copy lost
	require.Equal(t, int32(2), h.seen.Load(), "new message with a freed id was not handled (stale registry entry, size %d)", s.qos2.size())
}

// A3: after the PUBREC was written, a DUP=0 PUBLISH reusing the id (a
// device that restarted its packet ids, session kept) gets 0x00 now (was
// 0x91, "session state mismatch") and is not handled.
func TestR3_Dup0ReuseAfterPubrecWritten(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	h.release(t, 7, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 7)
	require.Eventually(t, func() bool { _, ok := cl.State.Inflight.Get(7); return ok }, time.Second, time.Millisecond)
	require.NoError(t, s.processPacket(cl, publishQos(7, 2))) // DUP 0
	p := expectAck(t, wire, packets.Pubrec, 7)
	t.Logf("DUP=0 reuse answered 0x%02x, handled=%d", p.reason, h.seen.Load())
	require.Equal(t, byte(0x91), p.reason, "DUP=0 reuse of an id awaiting PUBREL told success, message not handled")
}

// A3b: while held, DUP=0 reuse IS handled as new (inconsistent with A3).
func TestR3_Dup0ReuseWhileHeld(t *testing.T) {
	s, h := newSettleServer(t)
	cl, _, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	t.Logf("DUP=0 reuse while held: handled %d times", h.seen.Load())
}

// A4: stored verdict after restart (storage hook restores inflight Pubrec,
// registry empty): the same DUP resend now gets 0x91.
func TestR3_VerdictAfterRestart(t *testing.T) {
	s, _ := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)
	rec := s.buildAck(7, packets.Pubrec, 0, packets.Properties{}, packets.CodeSuccess)
	cl.State.Inflight.Set(rec) // as restored by a storage hook
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	p := expectAck(t, wire, packets.Pubrec, 7)
	t.Logf("resend after restart answered 0x%02x", p.reason)
}

// lowerHook: blocks the first copy of 7 in OnPublish, then lowers it to QoS 1.
type lowerHook struct {
	settleHook
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
	to      byte
}

func (h *lowerHook) Provides(b byte) bool { return b == OnPublishedSettle || b == OnPublish }
func (h *lowerHook) OnPublish(cl *Client, pk packets.Packet) (packets.Packet, error) {
	if pk.PacketID == 7 && !pk.FixedHeader.Dup {
		first := false
		h.once.Do(func() { first = true })
		if first {
			close(h.entered)
			<-h.gate
			pk.FixedHeader.Qos = h.to
		}
	}
	return pk, nil
}

// A5: original lowered to QoS 1 by a hook; the resend waiting on the
// resumed connection is told "received" (PUBREC 0x00) before the hook
// has settled the original — ack before handled.
func TestR3_LoweredToQos1AcksResendBeforeHandled(t *testing.T) {
	s := newServer()
	h := &lowerHook{settleHook: *newSettleHook(), gate: make(chan struct{}), entered: make(chan struct{}), to: 1}
	require.NoError(t, s.AddHook(h, nil))
	old := connClient(t, s)
	old.ID = "low"
	done := make(chan error, 1)
	go func() { done <- s.processPacket(old, publishQos(7, 2)) }()
	<-h.entered
	cl, wire := takeover(t, s, old)
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	close(h.gate)
	require.NoError(t, <-done)
	// the original is held by the hook (QoS 1 now): nothing should be acked yet
	select {
	case p := <-wire:
		h.releaseFor(t, "low", 7, packets.ErrUnspecifiedError) // the store then fails
		t.Fatalf("resend acked %+v before the original (lowered to QoS 1) was settled; the hook later refused it", p)
	case <-time.After(200 * time.Millisecond):
	}
}

// A6: stress across shards: many sessions, each racing release, takeover,
// resend, PUBREL, OnPublish rejection.
type stressHook struct {
	settleHook
	rejectOdd atomic.Bool
}

func (h *stressHook) Provides(b byte) bool { return b == OnPublishedSettle || b == OnPublish }
func (h *stressHook) OnPublish(cl *Client, pk packets.Packet) (packets.Packet, error) {
	if pk.PacketID%5 == 0 && !pk.FixedHeader.Dup {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Microsecond)
		return pk, packets.ErrRejectPacket
	}
	return pk, nil
}

func TestR3_StressAcrossShards(t *testing.T) {
	const sessions = 48
	for iter := 0; iter < 20; iter++ {
		s := newServer()
		h := &stressHook{settleHook: *newSettleHook()}
		require.NoError(t, s.AddHook(h, nil))
		var wg sync.WaitGroup
		errs := make(chan error, sessions)
		for n := 0; n < sessions; n++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				id := fmt.Sprintf("c%d", n)
				old := connClient(t, s)
				old.ID = id
				ids := []uint16{3, 4, 5}
				pubDone := make(chan struct{})
				go func() {
					for _, i := range ids {
						_ = s.processPacket(old, publishQos(i, 2))
					}
					close(pubDone)
				}()
				// release concurrently
				var rw sync.WaitGroup
				for _, i := range []uint16{3, 4} {
					rw.Add(1)
					go func(i uint16) {
						defer rw.Done()
						deadline := time.Now().Add(2 * time.Second)
						for time.Now().Before(deadline) {
							h.mu.Lock()
							r := h.releases[key(id, i)]
							h.mu.Unlock()
							if r != nil {
								time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)
								r(packets.CodeSuccess)
								return
							}
							time.Sleep(50 * time.Microsecond)
						}
					}(i)
				}
				time.Sleep(time.Duration(rand.Intn(300)) * time.Microsecond)
				s.Clients.Add(old)
				cl, r, _ := newTestClient()
				cl.Properties.ProtocolVersion = 5
				cl.ID = id
				go func() { _, _ = io.Copy(io.Discard, r) }()
				s.inheritClientSession(packets.Packet{Connect: packets.ConnectParams{ClientIdentifier: id}}, cl)
				s.Clients.Add(cl)
				<-pubDone
				for _, i := range ids {
					_ = s.processPacket(cl, qos2Dup(i))
				}
				rw.Wait()
				h.releaseFor(t, id, 5, packets.CodeSuccess)
				// the resends on cl must all be answered (no stranded waiter)
				deadline := time.Now().Add(2 * time.Second)
				for cl.State.acks.busy() && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if cl.State.acks.busy() {
					errs <- fmt.Errorf("%s: acks stuck", id)
					return
				}
				for _, i := range ids {
					_ = s.processPacket(cl, pubrelPk(i))
				}
				if q := atomic.LoadInt32(&cl.State.Inflight.receiveQuota); q != 10 {
					errs <- fmt.Errorf("%s: quota %d after all flows ended", id, q)
				}
			}(n)
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			t.Error(e)
		}
		require.Equal(t, 0, s.qos2.size(), "iter %d registry", iter)
		// each id handled at most once per session (id 5 rejected, then its resend handled anew)
		require.LessOrEqual(t, h.seen.Load(), int32(sessions*3), "iter %d handled", iter)
	}
}
