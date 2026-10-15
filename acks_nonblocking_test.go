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

// release must not write: the goroutine that releases a hold belongs to
// the hook (a shared worker pool in MaestroHub), and a client that stops
// reading its socket would block every worker that settles one of its
// publishes — then healthy clients' publishes run without their workers.
// The pipe is never read here, so any write on the releasing goroutine
// blocks it.
func TestReleaseDoesNotBlockOnAClientThatStopsReading(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()

	for id := uint16(1); id <= 3; id++ {
		require.NoError(t, s.processPacket(cl, publishQos(id, 1)))
	}
	released := make(chan struct{})
	go func() {
		for id := uint16(1); id <= 3; id++ {
			h.release(t, id, packets.CodeSuccess)
		}
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("releasing a hold blocked on a client that does not read: the releasing goroutine wrote the ack itself")
	}

	// The acks still reach the client, in order, once it reads again.
	wire := readWire(r)
	for id := uint16(1); id <= 3; id++ {
		expectAck(t, wire, packets.Puback, id)
	}
	require.Eventually(t, func() bool { return !cl.State.acks.busy() }, time.Second, time.Millisecond,
		"nothing is left waiting once the acks are written")
}

// A refusal decided on arrival must not overtake acks that are settled
// but not yet written (still with the client's writer).
func TestRefusalDoesNotOvertakeAcksBeingWritten(t *testing.T) {
	s, h := newSettleServer(t)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5

	require.NoError(t, s.processPacket(cl, publishQos(1, 1)))
	// A regression writes on these goroutines and blocks (nobody reads
	// yet): fail fast instead of hanging the package.
	queued := make(chan error, 1)
	go func() {
		h.release(t, 1, packets.CodeSuccess) // settled; its write blocks (nobody reads yet)
		refused := publishQos(2, 1)
		queued <- cl.State.acks.writeNowOrQueue(cl, s.buildAck(refused.PacketID, packets.Puback, 0, refused.Properties, packets.ErrNotAuthorized))
	}()
	select {
	case err := <-queued:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("releasing or queueing a refusal blocked on a client that does not read")
	}

	wire := readWire(r)
	expectAck(t, wire, packets.Puback, 1)
	p := expectAck(t, wire, packets.Puback, 2)
	require.Equal(t, packets.ErrNotAuthorized.Code, p.reason)
}

// After a refusal an MQTT 3 client cannot be told has ended the connection,
// no later ack is written — not even in the moment before the connection is
// actually stopped ([MQTT-4.6.0-2]: no PUBACK for N+1 without N).
func TestNoAckAfterMqtt3RefusalClosedTheSequencer(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.Net.Conn = nil
	cl.Properties.ProtocolVersion = 4
	q := &cl.State.acks
	refused := q.reserve(publishQos(1, 1))
	refused.settled, refused.reason = true, packets.ErrUnspecifiedError
	q.mu.Lock()
	q.flushSettledLocked(cl)
	ready := q.ready
	q.ready = nil
	q.mu.Unlock()
	require.True(t, q.write(cl, ready), "the refusal ends the connection")

	// A publish the reader took in before the connection was stopped.
	later := q.reserve(publishQos(2, 1))
	q.settle(cl, later, packets.CodeSuccess, false)
	q.mu.Lock()
	defer q.mu.Unlock()
	require.Empty(t, q.ready, "an ack was queued for writing after the refusal")
	require.False(t, q.writing, "a writer started after the refusal")
}
