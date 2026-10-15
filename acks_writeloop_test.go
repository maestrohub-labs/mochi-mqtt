// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

package mqtt

import (
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
