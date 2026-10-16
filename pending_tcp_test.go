// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 MaestroHub

package mqtt_test

// MAESTROHUB PATCH (pending.go): what a slow subscriber receives, over a real
// TCP listener. Every assertion is about the packets the client reads and
// the server's counters — never about elapsed time. Reads carry a generous
// deadline so a stalled delivery fails the test instead of hanging it.

import (
	"bufio"
	"bytes"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/maestrohub-labs/mochi-mqtt/v2"
	"github.com/maestrohub-labs/mochi-mqtt/v2/hooks/auth"
	"github.com/maestrohub-labs/mochi-mqtt/v2/listeners"
	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

const pendingReadTimeout = 10 * time.Second

// droppedPayloads records what the server dropped for a subscriber.
type droppedPayloads struct {
	mqtt.HookBase
	mu  sync.Mutex
	got []string
}

func (h *droppedPayloads) ID() string           { return "dropped-payloads" }
func (h *droppedPayloads) Provides(b byte) bool { return b == mqtt.OnPublishDropped }
func (h *droppedPayloads) OnPublishDropped(_ *mqtt.Client, pk packets.Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.got = append(h.got, string(pk.Payload))
}

func (h *droppedPayloads) all() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.got...)
}

// startQueueBroker starts a broker with an inline client on an ephemeral
// loopback port, the given in-flight and queue limits.
func startQueueBroker(t *testing.T, maxInflight uint16, maxPending int32) (*mqtt.Server, string, *droppedPayloads) {
	t.Helper()
	caps := mqtt.NewDefaultServerCapabilities()
	caps.MaximumInflight = maxInflight
	caps.MaximumClientPendingMessages = maxPending
	s := mqtt.New(&mqtt.Options{
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Capabilities: caps,
		InlineClient: true,
	})
	dropped := new(droppedPayloads)
	require.NoError(t, s.AddHook(new(auth.AllowHook), nil))
	require.NoError(t, s.AddHook(dropped, nil))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, s.AddListener(listeners.NewNet("queue", ln)))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })
	return s, ln.Addr().String(), dropped
}

// subscriber is a raw MQTT 5 client that acknowledges only when told to.
type subscriber struct {
	conn net.Conn
	r    *bufio.Reader
}

// connectSubscriber connects (receiveMaximum 0 = not sent; clean false
// keeps the session for an hour) and subscribes to "q" at QoS 1.
func connectSubscriber(t *testing.T, addr, id string, receiveMaximum uint16, clean bool) (*subscriber, bool) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c := &subscriber{conn: conn, r: bufio.NewReader(conn)}

	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: id,
			Keepalive:        60,
			Clean:            clean,
		},
		Properties: packets.Properties{ReceiveMaximum: receiveMaximum},
	}
	if !clean {
		pk.Properties.SessionExpiryInterval = 3600
		pk.Properties.SessionExpiryIntervalFlag = true
	}
	c.write(t, &pk)
	typ, body := c.read(t)
	require.Equal(t, packets.Connack, typ)
	require.Equal(t, byte(0), body[1], "connack reason")
	sessionPresent := body[0]&1 == 1
	return c, sessionPresent
}

func (c *subscriber) subscribe(t *testing.T) {
	t.Helper()
	c.write(t, &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: 5,
		PacketID:        1,
		Filters:         packets.Subscriptions{{Filter: "q", Qos: 1}},
	})
	typ, _ := c.read(t)
	require.Equal(t, packets.Suback, typ)
}

func (c *subscriber) write(t *testing.T, pk *packets.Packet) {
	t.Helper()
	var buf bytes.Buffer
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(&buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(&buf)
	case packets.Puback:
		err = pk.PubackEncode(&buf)
	}
	require.NoError(t, err)
	_, err = c.conn.Write(buf.Bytes())
	require.NoError(t, err)
}

func (c *subscriber) read(t *testing.T) (byte, []byte) {
	t.Helper()
	require.NoError(t, c.conn.SetReadDeadline(time.Now().Add(pendingReadTimeout)))
	h, err := c.r.ReadByte()
	require.NoError(t, err)
	n, _, err := packets.DecodeLength(c.r)
	require.NoError(t, err)
	body := make([]byte, n)
	_, err = io.ReadFull(c.r, body)
	require.NoError(t, err)
	return h >> 4, body
}

// nextPublish reads one PUBLISH and returns it decoded.
func (c *subscriber) nextPublish(t *testing.T) packets.Packet {
	t.Helper()
	require.NoError(t, c.conn.SetReadDeadline(time.Now().Add(pendingReadTimeout)))
	h, err := c.r.ReadByte()
	require.NoError(t, err)
	n, _, err := packets.DecodeLength(c.r)
	require.NoError(t, err)
	body := make([]byte, n)
	_, err = io.ReadFull(c.r, body)
	require.NoError(t, err)
	pk := packets.Packet{ProtocolVersion: 5}
	require.NoError(t, pk.FixedHeader.Decode(h))
	require.Equal(t, packets.Publish, pk.FixedHeader.Type)
	pk.FixedHeader.Remaining = n
	require.NoError(t, pk.PublishDecode(body))
	return pk
}

func (c *subscriber) ack(t *testing.T, id uint16) {
	t.Helper()
	c.write(t, &packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, ProtocolVersion: 5, PacketID: id})
}

// nothingMore asserts no packet arrives within a short window. Absence can
// only be sampled; it is the complement of the positive reads above.
func (c *subscriber) nothingMore(t *testing.T) {
	t.Helper()
	require.NoError(t, c.conn.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	_, err := c.r.ReadByte()
	require.Error(t, err, "no further message was expected")
	var ne net.Error
	require.ErrorAs(t, err, &ne)
	require.True(t, ne.Timeout())
}

func publishNumbered(t *testing.T, s *mqtt.Server, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		require.NoError(t, s.Publish("q", []byte(strconv.Itoa(i)), false, 1))
	}
}

func span(from, to int) []string {
	out := []string{}
	for i := from; i < to; i++ {
		out = append(out, strconv.Itoa(i))
	}
	return out
}

// A QoS 1 subscriber that stops acknowledging: its in-flight window (4) is
// sent, the next messages queue (10), and once the queue is full the OLDEST
// queued message is dropped for each new one. When the subscriber resumes
// acknowledging it receives the window and then the 10 newest messages, in
// publish order, each a QoS 1 message with its own packet id; the dropped
// ones are counted and reported oldest first.
func TestSlowSubscriberGetsNewestQueuedAndOldestAreDropped(t *testing.T) {
	const window, queue, sent = 4, 10, 100
	s, addr, dropped := startQueueBroker(t, window, queue)
	c, _ := connectSubscriber(t, addr, "slow", 0, true)
	c.subscribe(t)

	publishNumbered(t, s, 0, sent)

	require.Equal(t, span(window, sent-queue), dropped.all(), "dropped: the oldest queued, in order")
	require.Equal(t, int64(sent-window-queue), atomic.LoadInt64(&s.Info.MessagesDropped))

	var got []string
	ids := map[uint16]bool{}
	for i := 0; i < window+queue; i++ {
		pk := c.nextPublish(t)
		require.Equal(t, byte(1), pk.FixedHeader.Qos)
		require.NotZero(t, pk.PacketID)
		require.False(t, ids[pk.PacketID], "packet id %d in use twice", pk.PacketID)
		ids[pk.PacketID] = true
		got = append(got, string(pk.Payload))
		c.ack(t, pk.PacketID)
		delete(ids, pk.PacketID)
	}
	require.Equal(t, append(span(0, window), span(sent-queue, sent)...), got)
	c.nothingMore(t)
}

// A message the subscriber never acknowledges stays in flight: the queue
// does not release QoS 1 messages past the window just because time passes,
// and an unacknowledged message is resent with DUP when the session
// resumes. The queued messages move with the session and follow, in order,
// as acknowledgements free the window.
func TestQueuedMessagesFollowAResumedSession(t *testing.T) {
	const window, queue = 2, 100
	s, addr, _ := startQueueBroker(t, window, queue)
	c, _ := connectSubscriber(t, addr, "resume", 0, false)
	c.subscribe(t)

	publishNumbered(t, s, 0, 10)
	first := c.nextPublish(t)
	second := c.nextPublish(t)
	require.Equal(t, []string{"0", "1"}, []string{string(first.Payload), string(second.Payload)})
	c.nothingMore(t) // the window is full and unacknowledged
	_ = c.conn.Close()

	require.Eventually(t, func() bool {
		cl, ok := s.Clients.Get("resume")
		return ok && cl.Closed()
	}, pendingReadTimeout, time.Millisecond)

	c2, present := connectSubscriber(t, addr, "resume", 0, false)
	require.True(t, present)

	var got []string
	for i := 0; i < 10; i++ {
		pk := c2.nextPublish(t)
		if i < window {
			require.True(t, pk.FixedHeader.Dup, "an unacknowledged message is resent with DUP")
		}
		got = append(got, string(pk.Payload))
		c2.ack(t, pk.PacketID)
	}
	// The resent window's own order is upstream's (ResendInflightMessages
	// sorts by creation second); the queued messages follow it in order.
	require.ElementsMatch(t, span(0, window), got[:window])
	require.Equal(t, span(window, 10), got[window:])
	c2.nothingMore(t)
}

// A client's Receive Maximum is honoured, and messages over it are delivered
// in order once acknowledgements return quota. Upstream parked them in the
// in-flight store, sent them in creation-second order and deleted them once
// sent, so their PUBACKs returned no quota and the client stalled after
// Receive Maximum of them.
func TestReceiveMaximumQueuedMessagesAllArriveInOrder(t *testing.T) {
	const receiveMaximum, sent = 2, 50
	s, addr, dropped := startQueueBroker(t, 0, 0)
	c, _ := connectSubscriber(t, addr, "rm", receiveMaximum, true)
	c.subscribe(t)

	publishNumbered(t, s, 0, sent)

	var got []string
	for i := 0; i < sent; i++ {
		pk := c.nextPublish(t)
		got = append(got, string(pk.Payload))
		c.ack(t, pk.PacketID)
	}
	require.Equal(t, span(0, sent), got)
	require.Empty(t, dropped.all())
	c.nothingMore(t)
}

// Never more than Receive Maximum unacknowledged messages are sent
// [MQTT-4.9.0-2].
func TestReceiveMaximumIsNotExceeded(t *testing.T) {
	const receiveMaximum = 3
	s, addr, _ := startQueueBroker(t, 0, 0)
	c, _ := connectSubscriber(t, addr, "rm-cap", receiveMaximum, true)
	c.subscribe(t)

	publishNumbered(t, s, 0, 10)
	var held []packets.Packet
	for i := 0; i < receiveMaximum; i++ {
		held = append(held, c.nextPublish(t))
	}
	c.nothingMore(t)
	c.ack(t, held[0].PacketID)
	require.Equal(t, "3", string(c.nextPublish(t).Payload))
	c.nothingMore(t)
}
