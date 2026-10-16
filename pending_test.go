// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 MaestroHub

package mqtt

// MAESTROHUB PATCH (pending.go): the per-client pending queue. The queue's
// own bounds and order here; the behaviour a subscriber sees over a real
// TCP connection is in pending_tcp_test.go.

import (
	"bufio"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

func numbered(i int) *packets.Packet {
	return &packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish},
		TopicName:   "q",
		Payload:     []byte(strconv.Itoa(i)),
	}
}

func payloads(pks []*packets.Packet) []string {
	out := make([]string, 0, len(pks))
	for _, pk := range pks {
		out = append(out, string(pk.Payload))
	}
	return out
}

// The queue never holds more than its limit, evicts strictly oldest first,
// keeps the rest in arrival order, never allocates more slots than the
// limit, and gives its buffer back when it empties.
func TestPendingQueueDropsOldestAndStaysBounded(t *testing.T) {
	const limit = 7
	var q pendingQueue
	var evicted []*packets.Packet

	q.mu.Lock()
	for i := 0; i < 1000; i++ {
		if e := q.push(numbered(i), limit); e != nil {
			evicted = append(evicted, e)
		}
		require.LessOrEqual(t, q.n, limit)
		require.LessOrEqual(t, len(q.buf), limit, "the ring must never grow past the limit")
	}
	q.mu.Unlock()

	require.Equal(t, limit, q.Len())
	require.Len(t, evicted, 1000-limit)
	for i, e := range evicted {
		require.Equal(t, strconv.Itoa(i), string(e.Payload), "eviction must be oldest first")
	}

	require.Equal(t, []string{"993", "994", "995", "996", "997", "998", "999"}, payloads(q.takeAll()))
	require.Equal(t, 0, q.Len())
	require.Nil(t, q.buf, "an empty queue holds no buffer")
}

// The ring wraps and grows without reordering.
func TestPendingQueueOrderAcrossWrapAndGrowth(t *testing.T) {
	var q pendingQueue
	q.mu.Lock()
	for i := 0; i < 10; i++ {
		q.push(numbered(i), 100)
	}
	for i := 0; i < 6; i++ {
		require.Equal(t, strconv.Itoa(i), string(q.pop().Payload))
	}
	for i := 10; i < 40; i++ { // wraps the 16-slot ring, then grows it
		q.push(numbered(i), 100)
	}
	q.mu.Unlock()

	want := []string{}
	for i := 6; i < 40; i++ {
		want = append(want, strconv.Itoa(i))
	}
	require.Equal(t, want, payloads(q.takeAll()))
}

// droppedHook records the payloads of messages dropped for a client.
type droppedHook struct {
	HookBase
	mu      sync.Mutex
	dropped []string
}

func (h *droppedHook) ID() string           { return "dropped" }
func (h *droppedHook) Provides(b byte) bool { return b == OnPublishDropped }
func (h *droppedHook) OnPublishDropped(_ *Client, pk packets.Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dropped = append(h.dropped, string(pk.Payload))
}

// A client whose outbound channel is full queues messages; with the queue
// full, the oldest queued one is dropped (counted, reported through
// OnPublishDropped) and the newest kept.
func TestPublishToClientQueuesAndDropsOldestWhenFull(t *testing.T) {
	s := newServer()
	h := new(droppedHook)
	require.NoError(t, s.AddHook(h, nil))
	cl, _, _ := newTestClientNoWriteLoop() // nothing takes from outbound
	cl.ops.hooks = s.hooks
	cl.ops.info = s.Info
	cl.ops.options.Capabilities.MaximumClientPendingMessages = 3
	s.Clients.Add(cl)

	for i := 0; i < int(cl.ops.options.Capabilities.MaximumClientWritesPending); i++ {
		_, err := s.publishToClient(cl, packets.Subscription{Filter: "q"}, *numbered(i))
		require.NoError(t, err)
	}
	require.Equal(t, 0, cl.State.pending.Len(), "outbound still had room")

	for i := 3; i < 10; i++ {
		_, err := s.publishToClient(cl, packets.Subscription{Filter: "q"}, *numbered(i))
		require.NoError(t, err, "a message is queued, not refused")
	}

	require.Equal(t, 3, cl.State.pending.Len())
	require.Equal(t, []string{"3", "4", "5", "6"}, h.dropped)
	require.Equal(t, int64(4), atomic.LoadInt64(&s.Info.MessagesDropped))
	require.Equal(t, []string{"7", "8", "9"}, payloads(cl.State.pending.takeAll()))
}

// Once anything is queued, a message that could be handed out queues
// behind it: delivery order is arrival order.
func TestPublishToClientQueuesBehindQueued(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClientNoWriteLoop()
	cl.ops.info = s.Info
	s.Clients.Add(cl)
	for i := 0; i < int(cl.ops.options.Capabilities.MaximumClientWritesPending); i++ {
		_, _ = s.publishToClient(cl, packets.Subscription{Filter: "q"}, *numbered(i))
	}
	_, _ = s.publishToClient(cl, packets.Subscription{Filter: "q"}, *numbered(3)) // queued: outbound full
	<-cl.State.outbound                                                           // room for one again
	_, _ = s.publishToClient(cl, packets.Subscription{Filter: "q"}, *numbered(4))

	require.Equal(t, 2, cl.State.pending.Len(), "4 must queue behind 3, not overtake it")
	cl.drainPending()
	require.Equal(t, 1, cl.State.pending.Len())
	<-cl.State.outbound
	<-cl.State.outbound
	got := <-cl.State.outbound
	require.Equal(t, "3", string(got.Payload))
	cl.drainPending()
	got = <-cl.State.outbound
	require.Equal(t, "4", string(got.Payload))
}

// QoS 1 messages over the in-flight limit wait without a packet id or an
// in-flight entry, and get both when a PUBACK frees a slot.
func TestQueuedQoS1TakesItsPacketIDWhenDelivered(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClientNoWriteLoop()
	cl.ops.info = s.Info
	cl.ops.options.Capabilities.MaximumInflight = 2
	s.Options.Capabilities.MaximumInflight = 2
	cl.State.Inflight.ResetSendQuota(0) // no client Receive Maximum: the in-flight limit decides
	s.Clients.Add(cl)

	sub := packets.Subscription{Filter: "q", Qos: 1}
	for i := 0; i < 3; i++ {
		pk := *numbered(i)
		pk.FixedHeader.Qos = 1
		_, err := s.publishToClient(cl, sub, pk)
		require.NoError(t, err)
	}
	require.Equal(t, 2, cl.State.Inflight.Len())
	require.Equal(t, 1, cl.State.pending.Len())
	queued := cl.State.pending.takeAll()
	require.Equal(t, uint16(0), queued[0].PacketID, "a queued message holds no packet id")
	cl.State.pending.mu.Lock()
	cl.State.pending.push(queued[0], 10)
	cl.State.pending.mu.Unlock()

	first := <-cl.State.outbound
	require.NoError(t, s.processPacket(cl, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: first.PacketID}))

	require.Equal(t, 0, cl.State.pending.Len(), "the PUBACK's free slot is used at once")
	require.Equal(t, 2, cl.State.Inflight.Len())
	<-cl.State.outbound
	third := <-cl.State.outbound
	require.Equal(t, "2", string(third.Payload))
	require.NotZero(t, third.PacketID)
	_, ok := cl.State.Inflight.Get(third.PacketID)
	require.True(t, ok, "a delivered QoS 1 message is in flight until its PUBACK")
}

// A queued message whose expiry passed while it waited is deleted, not
// delivered [MQTT-3.3.2-5].
func TestDrainPendingDeletesExpired(t *testing.T) {
	cl, _, _ := newTestClientNoWriteLoop()
	stale := numbered(1)
	stale.Expiry = time.Now().Unix() - 10
	fresh := numbered(2)
	cl.State.pending.mu.Lock()
	cl.State.pending.push(stale, 10)
	cl.State.pending.push(fresh, 10)
	cl.State.pending.mu.Unlock()

	cl.drainPending()

	require.Equal(t, 0, cl.State.pending.Len())
	got := <-cl.State.outbound
	require.Equal(t, "2", string(got.Payload))
	require.Len(t, cl.State.outbound, 0)
}

// A topic alias is assigned when a message is handed out. A queued message
// dropped from the queue therefore never claims an alias, and the first
// message the client receives for a topic carries the topic name (upstream
// assigned it on arrival: a dropped first message left the client with an
// alias it was never told).
func TestTopicAliasAssignedOnlyToDeliveredMessages(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClientNoWriteLoop()
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.TopicAliasMaximum = 5
	cl.State.TopicAliases.Outbound = NewOutboundTopicAliases(5)
	cl.ops.options.Capabilities.MaximumClientPendingMessages = 1
	s.Clients.Add(cl)

	for i := 0; i < int(cl.ops.options.Capabilities.MaximumClientWritesPending); i++ {
		_, _ = s.publishToClient(cl, packets.Subscription{Filter: "#"}, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "filler"})
	}
	_, _ = s.publishToClient(cl, packets.Subscription{Filter: "#"}, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "a"}) // queued
	_, _ = s.publishToClient(cl, packets.Subscription{Filter: "#"}, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "b"}) // drops "a"
	for i := 0; i < int(cl.ops.options.Capabilities.MaximumClientWritesPending); i++ {
		<-cl.State.outbound
	}
	cl.drainPending()
	b := <-cl.State.outbound
	require.Equal(t, "b", b.TopicName)

	_, err := s.publishToClient(cl, packets.Subscription{Filter: "#"}, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "a"})
	require.NoError(t, err)
	a := <-cl.State.outbound
	require.Equal(t, "a", a.TopicName, "the first delivered message for a topic must name it, not only its alias")
	require.True(t, a.Properties.TopicAliasFlag)
}

// A closed client's queue is not drained into its in-flight store: a
// takeover clones that store and then takes the queue, so a QoS 1 message
// moved between the two would be lost to the resumed session.
func TestDrainPendingLeavesAClosedClientsQueueWhole(t *testing.T) {
	cl, _, _ := newTestClientNoWriteLoop()
	cl.ops.options.Capabilities.MaximumInflight = 10
	for i := 0; i < 3; i++ {
		pk := numbered(i)
		pk.FixedHeader.Qos = 1
		cl.State.pending.mu.Lock()
		cl.State.pending.push(pk, 10)
		cl.State.pending.mu.Unlock()
	}
	cl.Stop(errClientStop)

	cl.drainPending() // what its WriteLoop or the expiry sweep may do while a takeover runs

	require.Equal(t, 3, cl.State.pending.Len(), "the queue must stay whole, to move with the session")
	require.Equal(t, 0, cl.State.Inflight.Len(), "queued messages must not land in a store the takeover may already have cloned")
}

// A session's delivery state is discarded as a whole: its queue too.
func TestClearInflightsEmptiesThePendingQueue(t *testing.T) {
	cl, _, _ := newTestClientNoWriteLoop()
	cl.State.pending.mu.Lock()
	cl.State.pending.push(numbered(1), 10)
	cl.State.pending.mu.Unlock()
	cl.ClearInflights()
	require.Equal(t, 0, cl.State.pending.Len())
}

// Concurrent publishers, a draining writer: nothing lost while the queue
// has room, nothing duplicated, the queue never past its limit (-race).
func TestPendingQueueConcurrentPublishersAndWriter(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClientNoWriteLoop()
	cl.ops.info = s.Info
	cl.ops.options.Capabilities.MaximumClientPendingMessages = 1 << 20
	s.Clients.Add(cl)

	const publishers, each = 8, 500
	got := make(chan string, publishers*each)
	done := make(chan struct{})
	go func() { // a WriteLoop stand-in: take, then drain as WriteLoop does
		defer close(done)
		for n := 0; n < publishers*each; n++ {
			pk := <-cl.State.outbound
			got <- string(pk.Payload)
			cl.drainPending()
		}
	}()

	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				_, _ = s.publishToClient(cl, packets.Subscription{Filter: "q"}, *numbered(p*each + i))
			}
		}(p)
	}
	wg.Wait()
	cl.drainPending()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("writer stalled with %d queued", cl.State.pending.Len())
	}
	close(got)
	seen := map[string]bool{}
	for p := range got {
		require.False(t, seen[p], "message %s delivered twice", p)
		seen[p] = true
	}
	require.Len(t, seen, publishers*each)
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.MessagesDropped))
}

// A subscriber that does not read: WriteLoop is stuck writing, the outbound
// channel fills, and further messages queue. When it reads again, WriteLoop
// hands the queued messages out as it frees outbound slots: all arrive, in
// order.
func TestWriteLoopDrainsTheQueueAsOutboundFrees(t *testing.T) {
	s := newServer()
	cl, r, _ := newTestClient() // WriteLoop running; nobody reads r yet
	cl.ops.info = s.Info
	s.Clients.Add(cl)

	const sent = 20
	for i := 0; i < sent; i++ {
		_, err := s.publishToClient(cl, packets.Subscription{Filter: "q"}, *numbered(i))
		require.NoError(t, err)
	}
	require.Positive(t, cl.State.pending.Len(), "outbound (3) must have overflowed into the queue")

	br := bufio.NewReader(r)
	var got []string
	for i := 0; i < sent; i++ {
		require.NoError(t, r.SetReadDeadline(time.Now().Add(10*time.Second)))
		h, err := br.ReadByte()
		require.NoError(t, err, "message %d never arrived", i)
		n, _, err := packets.DecodeLength(br)
		require.NoError(t, err)
		body := make([]byte, n)
		_, err = io.ReadFull(br, body)
		require.NoError(t, err)
		pk := packets.Packet{ProtocolVersion: 4}
		require.NoError(t, pk.FixedHeader.Decode(h))
		pk.FixedHeader.Remaining = n
		require.NoError(t, pk.PublishDecode(body))
		got = append(got, string(pk.Payload))
	}
	want := []string{}
	for i := 0; i < sent; i++ {
		want = append(want, strconv.Itoa(i))
	}
	require.Equal(t, want, got)
}
