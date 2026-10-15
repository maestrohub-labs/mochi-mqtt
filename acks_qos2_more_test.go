// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

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

// Second review of patch 10: R4, R5, the mutation gaps (the closed-client
// guard before ackWritten, every endSession path) and two pre-existing
// QoS 2 answers.

// R4: a PUBREL for an id whose PUBREC never entered the inflight store
// (the connection closed as it was written) still frees the registry.
func TestQos2PubrelWithoutInflightFreesTheRegistry(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	h.release(t, 7, packets.CodeSuccess)
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
	cl.State.Inflight.Delete(7) // as if the PUBREC was written to a closing connection
	require.NoError(t, s.processPacket(cl, pubrelPk(7)))
	require.Equal(t, 0, s.qos2.size(), "a PUBREL answered 0x92 left the id registered")
}

// Mutation gap: an ack written for a connection already closed does not
// touch that connection's session state.
func TestAckForAClosedConnectionLeavesItsInflightAlone(t *testing.T) {
	s, h := newSettleServer(t)
	cl, _, _ := newTestClient()
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	cl.Stop(packets.ErrSessionTakenOver)
	h.release(t, 7, packets.CodeSuccess)
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
	require.Equal(t, 0, cl.State.Inflight.Len(), "a PUBREC for a closed connection entered its inflight store")
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.Inflight))
}

// Mutation gap: a clean-start takeover ends the session's QoS 2 ids.
func TestQos2CleanTakeoverEndsTheRegistry(t *testing.T) {
	s, _ := newSettleServer(t)
	old := connClient(t, s)
	old.ID = "clean"
	require.NoError(t, s.processPacket(old, publishQos(7, 2)))
	s.Clients.Add(old)
	cl, _, _ := newTestClient()
	cl.ID = "clean"
	require.False(t, s.inheritClientSession(packets.Packet{Connect: packets.ConnectParams{ClientIdentifier: "clean", Clean: true}}, cl))
	require.Equal(t, 0, s.qos2.size(), "a clean start kept the old session's ids")
}

// Mutation gap: an expired session's ids go with it (the expiry sweep).
func TestQos2ExpiredSessionEndsTheRegistry(t *testing.T) {
	s, _ := newSettleServer(t)
	cl := connClient(t, s)
	cl.ID = "exp"
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	s.Clients.Add(cl)
	cl.Properties.Props.SessionExpiryInterval, cl.Properties.Props.SessionExpiryIntervalFlag = 1, true
	cl.Stop(nil)
	atomic.StoreInt64(&cl.State.disconnected, time.Now().Unix()-10)
	s.clearExpiredClients(time.Now().Unix())
	require.Equal(t, 0, s.qos2.size(), "an expired session kept its ids")
}

// Mutation gap, end to end: a connection whose session ends with it (clean
// session) leaves no ids behind (attachClient's expire path).
func TestQos2SessionEndingWithItsConnectionEndsTheRegistry(t *testing.T) {
	s, _ := newSettleServer(t)
	s.Options.Capabilities.ReceiveMaximum = 10 // newServer's is 0: no publish would be taken in
	defer s.Close()
	r, w := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", r) }()
	go func() { _, _ = io.Copy(io.Discard, w) }()
	_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
	_, _ = w.Write(packets.TPacketData[packets.Publish].Get(packets.TPublishQos2).RawBytes)
	require.Eventually(t, func() bool { return s.qos2.size() == 1 }, time.Second, time.Millisecond)
	_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection did not end")
	}
	require.Equal(t, 0, s.qos2.size(), "a session that ended with its connection kept its ids")
}

// R5 (a): between the old connection's disconnect and the new one's
// registration, Done must not report the publish's client gone: the
// session is moving, and the verdict will reach the new connection.
func TestQos2DoneDuringTakeoverIsNotDone(t *testing.T) {
	s := newServer()
	h := &doneHook{got: make(chan *PublishSettlement, 2)}
	require.NoError(t, s.AddHook(h, nil))
	old := connClient(t, s)
	old.ID = "moving"
	require.NoError(t, s.processPacket(old, publishQos(7, 2)))
	st := <-h.got
	s.Clients.Add(old)
	cl, _, _ := newTestClient()
	cl.ID = "moving"
	cl.Properties.ProtocolVersion = 5
	require.True(t, s.inheritClientSession(packets.Packet{Connect: packets.ConnectParams{ClientIdentifier: "moving"}}, cl))
	old.Stop(packets.ErrSessionTakenOver)
	// The new client is not in Clients yet (attachClient adds it next).
	select {
	case <-st.Done():
		t.Fatal("Done fired while the session was moving to a new connection")
	case <-time.After(30 * time.Millisecond):
	}
}

// Pre-existing: an MQTT 5 QoS 2 PUBLISH refused in OnPublish with a reason
// code is answered by a PUBREC, not a PUBACK.
type codeRefuseHook struct{ HookBase }

func (h *codeRefuseHook) ID() string           { return "code-refuse" }
func (h *codeRefuseHook) Provides(b byte) bool { return b == OnPublish }
func (h *codeRefuseHook) OnPublish(*Client, packets.Packet) (packets.Packet, error) {
	return packets.Packet{}, packets.ErrQuotaExceeded
}

func TestQos2RefusedInOnPublishGetsAPubrec(t *testing.T) {
	s := newServer()
	require.NoError(t, s.AddHook(&codeRefuseHook{}, nil))
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	p := expectAck(t, wire, packets.Pubrec, 7)
	require.Equal(t, packets.ErrQuotaExceeded.Code, p.reason)
}

// Pre-existing: a resend after the PUBREC was written is answered with
// the publish's own verdict (here: stored), not 0x91 "packet identifier
// in use", which an MQTT 5 client reads as a failure.
func TestQos2ResendAfterPubrecWrittenGetsTheVerdict(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)
	require.NoError(t, s.processPacket(cl, publishQos(7, 2)))
	h.release(t, 7, packets.CodeSuccess)
	expectAck(t, wire, packets.Pubrec, 7)
	require.Eventually(t, func() bool { _, ok := cl.State.Inflight.Get(7); return ok }, time.Second, time.Millisecond)
	require.NoError(t, s.processPacket(cl, qos2Dup(7)))
	p := expectAck(t, wire, packets.Pubrec, 7)
	require.Equal(t, packets.CodeSuccess.Code, p.reason, "a resend of a stored message was answered as a failure")
	require.Equal(t, int32(1), h.seen.Load())
}

// takeoverWitness records, as the old connection is sent its DISCONNECT
// (inside the takeover), whether it was already marked taken over.
type takeoverWitness struct {
	HookBase
	marked atomic.Int32 // 1: marked, 2: not marked
}

func (h *takeoverWitness) ID() string           { return "takeover-witness" }
func (h *takeoverWitness) Provides(b byte) bool { return b == OnPacketEncode }
func (h *takeoverWitness) OnPacketEncode(cl *Client, pk packets.Packet) packets.Packet {
	if pk.FixedHeader.Type == packets.Disconnect {
		if cl.IsTakenOver() {
			h.marked.Store(1)
		} else {
			h.marked.Store(2)
		}
	}
	return pk
}

// Third review, item 8 (and the old connection's own tail, item 7): the old
// client is marked taken over BEFORE it is disconnected — for a resumed
// session and a clean start alike — so nothing that sees it closed takes
// the session for ended, and its attachClient tail cannot delete the session
// the new client is about to hold.
func TestTakeoverMarksTheOldClientBeforeDisconnectingIt(t *testing.T) {
	for _, clean := range []bool{false, true} {
		s := newServer()
		h := &takeoverWitness{}
		require.NoError(t, s.AddHook(h, nil))
		old, r, _ := newTestClient()
		old.ops.hooks = s.hooks // its writes go through the server's hooks, as a real client's do
		go func() { _, _ = io.Copy(io.Discard, r) }()
		old.ID = "witness"
		old.Properties.ProtocolVersion = 5
		s.Clients.Add(old)
		cl, _, _ := newTestClient()
		cl.ID = "witness"
		cl.Properties.ProtocolVersion = 5
		s.inheritClientSession(packets.Packet{Connect: packets.ConnectParams{ClientIdentifier: "witness", Clean: clean}}, cl)
		require.Equal(t, int32(1), h.marked.Load(), "clean=%v: the old client was disconnected before it was marked taken over (witness %d: 0 none seen, 2 unmarked)", clean, h.marked.Load())
	}
}
