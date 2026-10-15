// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

package mqtt

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

// Acks a hook settles are written by the client's own WriteLoop, not by a
// goroutine started per run of acks (v2.7.9-mh.10/11 started one for every
// run a hook settled while no writer was running: at device-ingest rates,
// thousands per second). Without a WriteLoop nothing writes them; once it
// runs, it writes them all, in order.
func TestSettledAcksAreWrittenByWriteLoop(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClientNoWriteLoop()
	wire := readWire(r)

	for id := uint16(1); id <= 3; id++ {
		require.NoError(t, s.processPacket(cl, publishQos(id, 1)))
	}
	for id := uint16(1); id <= 3; id++ {
		h.release(t, id, packets.CodeSuccess) // a hook's goroutine: never writes
	}
	expectNone(t, wire)
	require.True(t, cl.State.acks.busy(), "the settled acks wait for the writer")

	go cl.WriteLoop()
	for id := uint16(1); id <= 3; id++ {
		expectAck(t, wire, packets.Puback, id)
	}
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond)
}

// A connection that ends while its WriteLoop holds a wake-up leaves the
// sequencer idle (not busy forever), and acks settled after the end are
// dropped: the client resends them.
func TestSettledAcksAfterCloseLeaveTheSequencerIdle(t *testing.T) {
	s, h := newSettleServer(t)
	cl, _, _ := newTestClientNoWriteLoop()

	require.NoError(t, s.processPacket(cl, publishQos(1, 1)))
	require.NoError(t, s.processPacket(cl, publishQos(2, 1)))
	h.release(t, 1, packets.CodeSuccess) // handed over, WriteLoop not running
	// Take the wake-up, so the WriteLoop below can only see the close: the
	// close branch alone must leave the sequencer idle.
	<-cl.State.acks.wake
	cl.Stop(packets.ErrSessionTakenOver)
	done := make(chan struct{})
	go func() { cl.WriteLoop(); close(done) }() // sees the close, drains, exits
	<-done
	cl.State.acks.mu.Lock()
	writing := cl.State.acks.writing
	cl.State.acks.mu.Unlock()
	require.False(t, writing, "the ended WriteLoop left the sequencer writing")
	h.release(t, 2, packets.CodeSuccess) // after the end: dropped
	require.False(t, cl.State.acks.busy())
}

// An ack WriteLoop writes is not left in the write buffer for the reader to
// flush: the reader may be blocked in a handler (a store run inline,
// waitForQuota) while it holds a complete packet buffered (readAhead), and
// the ack would wait as long as it does. (mh.9–mh.11 held it there.)
func TestSettledAckIsNotHeldForABusyReader(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.ops.options.ClientNetWriteBufferSize = 2048 // the server default (Options.ensureDefaults)
	wire := readWire(r)

	require.NoError(t, s.processPacket(cl, publishQos(1, 1)))
	cl.State.readAhead.Store(true) // the reader is mid-burst, busy in a handler
	h.release(t, 1, packets.CodeSuccess)
	expectAck(t, wire, packets.Puback, 1)
}

// An ack that cannot be written on an open connection returns no receive
// quota and completes no flow, and ends the connection: the acks behind it
// must not overtake it, so the client resends what was not acknowledged.
// The ack before it still goes out. (mh.9–mh.11 counted every ack of the
// run as written.)
//
// Here the refusal's reason code alone (a 5-byte PUBACK) is over a 4-byte
// Maximum Packet Size. Optional properties never cause this: the encoder
// leaves out a Reason String or User Properties that would make the whole
// packet too large (packets: TestOptionalPropertiesRespectTheWholePacketSize;
// before mh.13 its check ignored the fixed header, the reason code and the
// property length, so a packet a few bytes over the limit kept them and
// failed here). The other real cause is a write that times out on a slow
// client.
func TestAckThatCannotBeWrittenIsNotCountedAndEndsTheConnection(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.MaximumPacketSize = 4      // a bare PUBACK (4 bytes) fits, one with a reason code does not
	cl.ops.options.ClientNetWriteBufferSize = 2048 // the server default: the run is buffered, then written once
	wire := readWire(r)

	for id := uint16(1); id <= 3; id++ {
		require.NoError(t, s.processPacket(cl, publishQos(id, 1)))
	}
	require.Equal(t, int32(7), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))

	// 2 (refused) and 3 settle first and wait for 1; then 1 settles and the
	// three go out as one run: 1 is written, 2 cannot be, 3 must not be.
	h.release(t, 2, packets.ErrUnspecifiedError)
	h.release(t, 3, packets.CodeSuccess)
	h.release(t, 1, packets.CodeSuccess)
	expectAck(t, wire, packets.Puback, 1)
	require.Eventually(t, cl.Closed, time.Second, time.Millisecond, "an ack that cannot be written ends the connection")
	for p := range wire {
		t.Fatalf("written after the ack that could not be: %+v", p)
	}
	require.Equal(t, int32(8), atomic.LoadInt32(&cl.State.Inflight.receiveQuota), "quota returned for acks never sent")
	require.False(t, cl.State.acks.busy())

	// The sequencer is closed, not merely reset: nothing more is written for
	// this connection, not even a refusal decided on arrival.
	cl.State.acks.mu.Lock()
	closed := cl.State.acks.closed
	cl.State.acks.mu.Unlock()
	require.True(t, closed, "the failure left the sequencer open")
	require.NoError(t, cl.State.acks.writeNowOrQueue(cl, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: 4}),
		"a closed sequencer writes nothing (an attempted write fails on the closed connection)")
}

// An ack held in the write buffer because a publish was queued for
// WriteLoop (writePacket holds while outbound is not empty) is flushed
// even when that publish then fails before it is written.
func TestAckHeldBehindAQueuedPublishThatFailsIsFlushed(t *testing.T) {
	cl, r, _ := newTestClientNoWriteLoop()
	cl.ops.options.ClientNetWriteBufferSize = 2048 // the server default (Options.ensureDefaults)
	cl.Properties.Props.MaximumPacketSize = 64
	wire := readWire(r)

	big := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	big.Payload = make([]byte, 256) // too large for the client: never written
	cl.State.outbound <- &big
	atomic.AddInt32(&cl.State.outboundQty, 1)

	ack := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: 9}
	require.NoError(t, cl.WritePacket(ack)) // held: the queued publish is to flush it
	expectNone(t, wire)

	go cl.WriteLoop()
	expectAck(t, wire, packets.Puback, 9)
}
