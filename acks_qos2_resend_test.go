// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

package mqtt

import (
	"sync"
	"testing"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

// QoS 2 is exactly once: a PUBLISH resent while its first copy's PUBREC is
// held must not be handled again — on the same connection, or on the one
// that resumed the session — and must be answered by the first copy's
// verdict (acks_qos2.go).

func qos2Dup(id uint16) packets.Packet {
	pk := publishQos(id, 2)
	pk.FixedHeader.Dup = true
	return pk
}

// connClient is a test client whose writes go nowhere (an old connection
// whose socket is gone). Set before anything is written, so no writer is
// running when the test inspects it.
func connClient(t *testing.T, s *Server) *Client {
	t.Helper()
	cl, _, _ := newTestClient()
	cl.Net.Conn = nil
	cl.Properties.ProtocolVersion = 5
	return cl
}

// takeover resumes old's session on a new connection, as a CONNECT with
// Clean Start 0 does.
func takeover(t *testing.T, s *Server, old *Client) (*Client, <-chan wirePacket) {
	t.Helper()
	s.Clients.Add(old)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.ID = old.ID // inheritClientSession finds the session by the new client's id
	require.True(t, s.inheritClientSession(packets.Packet{Connect: packets.ConnectParams{ClientIdentifier: old.ID}}, cl))
	s.Clients.Add(cl)
	return cl, readWire(r)
}

func TestQos2ResendWhilePubrecHeldIsNotHandledAgain(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	wire := readWire(r)

	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	require.Equal(t, int32(1), h.seen.Load(), "the resend was handled again")
	expectNone(t, wire)

	h.release(t, 7, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 7) // the first copy
	expectAck(t, wire, packets.Pubrec, 7) // the resend, answered by the same verdict
	expectNone(t, wire)
}

// F2: the first copy is settled, its PUBREC not yet written (it waits behind
// another publish): a resend then is answered, not handled.
func TestQos2ResendWhilePubrecSettledButUnwrittenIsNotHandledAgain(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	wire := readWire(r)

	require.NoError(t, s.processPacket(cl, publishQos(6, 1))) // held: 7's PUBREC waits behind it
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	h.release(t, 7, packets.CodeSuccess) // settled, unwritten
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	require.Equal(t, int32(2), h.seen.Load(), "the resend of a settled, unwritten publish was handled again")

	h.release(t, 6, packets.CodeSuccess)
	expectAck(t, wire, packets.Puback, 6)
	expectAck(t, wire, packets.Pubrec, 7)
	expectAck(t, wire, packets.Pubrec, 7)
}

func TestQos2ResendAfterTakeoverIsAnsweredByTheOriginal(t *testing.T) {
	s, h := newSettleServer(t)
	old := connClient(t, s)
	require.NoError(t, s.processPacket(old, publishQos(7, 2))) // held

	cl, wire := takeover(t, s, old)
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	require.Equal(t, int32(1), h.seen.Load(), "the resend on the resumed session was handled again")
	expectNone(t, wire)

	h.release(t, 7, packets.CodeSuccess) // the original's handler finishes
	expectAck(t, wire, packets.Pubrec, 7)
	require.Eventually(t, func() bool { _, ok := cl.State.Inflight.Get(7); return ok }, time.Second, time.Millisecond,
		"the PUBREC enters the resumed session's inflight store, awaiting PUBREL")
}

func TestQos2ResendAfterTakeoverOfASettledPublish(t *testing.T) {
	s, h := newSettleServer(t)
	old := connClient(t, s)
	require.NoError(t, s.processPacket(old, publishQos(7, 2)))
	require.NoError(t, s.processPacket(old, publishQos(8, 2)))
	h.release(t, 8, packets.CodeSuccess) // settled, waiting behind 7 on the old connection

	cl, wire := takeover(t, s, old)
	require.NoError(t, s.processPacket(cl, qos2Dup(8)))
	expectAck(t, wire, packets.Pubrec, 8) // its verdict is known: answered at once
	require.Equal(t, int32(2), h.seen.Load(), "a resend of a settled publish was handled again")
}

// F3a: two takeovers before the client resends.
func TestQos2ResendAfterTwoTakeovers(t *testing.T) {
	s, h := newSettleServer(t)
	first := connClient(t, s)
	require.NoError(t, s.processPacket(first, publishQos(7, 2)))
	second, _ := takeover(t, s, first)
	second.Net.Conn = nil
	third, wire := takeover(t, s, second)

	require.NoError(t, s.processPacket(third, qos2Dup(7)))
	require.Equal(t, int32(1), h.seen.Load(), "handled again after a second takeover")
	h.release(t, 7, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 7)
}

// F4: a client that restarted its packet ids sends a NEW message (DUP 0)
// with a registered id: it is handled, not swallowed.
func TestQos2NewMessageReusingAnIdIsHandled(t *testing.T) {
	s, h := newSettleServer(t)
	old := connClient(t, s)
	require.NoError(t, s.processPacket(old, publishQos(7, 2))) // held, never released

	cl, wire := takeover(t, s, old)
	require.NoError(t, s.processPacket(cl, publishQos(7, 2))) // DUP 0: a new message
	require.Equal(t, int32(2), h.seen.Load(), "a new message reusing a held id was swallowed")
	h.releaseFor(t, cl.ID, 7, packets.CodeSuccess) // the new one's hold (the map keeps the latest)
	expectAck(t, wire, packets.Pubrec, 7)
}

// F1: the original's release races the takeover and the resend; the PUBREC
// must reach the resumed connection whatever the interleaving.
func TestQos2ReleaseRacingTakeoverAlwaysAnswersTheResend(t *testing.T) {
	for i := 0; i < 200; i++ {
		s, h := newSettleServer(t)
		old := connClient(t, s)
		old.ID = "racer"
		require.NoError(t, s.processPacket(old, publishQos(7, 2)))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.releaseFor(t, "racer", 7, packets.CodeSuccess)
		}()
		cl, wire := takeover(t, s, old)
		require.NoError(t, s.processPacket(cl, qos2Dup(7)))
		wg.Wait()
		select {
		case p := <-wire:
			require.Equal(t, byte(packets.Pubrec), p.typ, "iteration %d", i)
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: the resend on the resumed connection never got its PUBREC", i)
		}
	}
}

// The registry forgets a released id (PUBREL) and a session that ends.
func TestQos2RegistryIsFreed(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	wire := readWire(r)
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	require.Equal(t, 1, s.qos2.size())
	h.release(t, 7, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 7)
	require.Eventually(t, func() bool { _, ok := cl.State.Inflight.Get(7); return ok }, time.Second, time.Millisecond)
	pubrel := *packets.TPacketData[packets.Pubrel].Get(packets.TPubrel).Packet
	pubrel.PacketID = 7
	require.NoError(t, s.processPacket(cl, pubrel))
	expectAck(t, wire, packets.Pubcomp, 7)
	require.Equal(t, 0, s.qos2.size(), "PUBREL frees the id")

	require.NoError(t, s.processPacket(cl, publishQos(8, 2)))
	s.qos2.endSession(cl.ID)
	require.Equal(t, 0, s.qos2.size(), "an ended session frees its ids")
}

// F6: Done follows the session for a QoS 2 publish: after a takeover the
// verdict still reaches the client, so Done is the live connection's.
func TestQos2DoneFollowsTheSession(t *testing.T) {
	s := newServer()
	h := &doneHook{got: make(chan *PublishSettlement, 2)}
	require.NoError(t, s.AddHook(h, nil))
	old := connClient(t, s)
	old.ID = "sess"
	require.NoError(t, s.processPacket(old, publishQos(7, 2)))
	st := <-h.got

	cl, _ := takeover(t, s, old)
	old.Stop(packets.ErrSessionTakenOver)
	select {
	case <-st.Done():
		t.Fatal("Done fired although the session lives on another connection")
	case <-time.After(50 * time.Millisecond):
	}
	cl.Stop(nil)
	select {
	case <-st.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not fire once no connection holds the session")
	}
}
