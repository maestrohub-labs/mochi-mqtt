// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 MaestroHub

package mqtt_test

// MAESTROHUB PATCH 13 over a real TCP listener: the backlog a persistent
// session collected while offline arrives in publish order when it
// resumes, and messages an embedder removed before the resume are not
// delivered at all.

import (
	"strconv"
	"testing"
	"time"

	mqtt "github.com/maestrohub-labs/mochi-mqtt/v2"
	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

// goOffline closes the subscriber and waits until the server sees its
// session offline.
func goOffline(t *testing.T, s *mqtt.Server, c *subscriber, id string) *mqtt.Client {
	t.Helper()
	_ = c.conn.Close()
	var cl *mqtt.Client
	require.Eventually(t, func() bool {
		var ok bool
		cl, ok = s.Clients.Get(id)
		return ok && cl.Closed()
	}, pendingReadTimeout, time.Millisecond)
	return cl
}

func readAll(t *testing.T, c *subscriber, n int) []string {
	t.Helper()
	var got []string
	for i := 0; i < n; i++ {
		pk := c.nextPublish(t)
		got = append(got, string(pk.Payload))
		c.ack(t, pk.PacketID)
	}
	return got
}

// Every message published while the session was offline went into its
// in-flight store (the window is large); all of it is re-sent on resume, in
// the order published. Upstream re-sent it in random order within each
// second [MQTT-4.6.0-1].
func TestOfflineBacklogIsResentInPublishOrder(t *testing.T) {
	const sent = 200
	s, addr, _ := startQueueBroker(t, 0, 0)
	c, _ := connectSubscriber(t, addr, "offline", 0, false)
	c.subscribe(t)
	goOffline(t, s, c, "offline")

	publishNumbered(t, s, 0, sent)

	c2, present := connectSubscriber(t, addr, "offline", 0, false)
	require.True(t, present)
	require.Equal(t, span(0, sent), readAll(t, c2, sent))
	c2.nothingMore(t)
}

// A backlog larger than the in-flight window: the window is re-sent first,
// in order, then the queued messages, in order — the whole backlog in
// publish order.
func TestOfflineBacklogBeyondTheWindowArrivesInOrder(t *testing.T) {
	const window, sent = 8, 60
	s, addr, _ := startQueueBroker(t, window, 1000)
	c, _ := connectSubscriber(t, addr, "deep", 0, false)
	c.subscribe(t)
	goOffline(t, s, c, "deep")

	publishNumbered(t, s, 0, sent)

	c2, present := connectSubscriber(t, addr, "deep", 0, false)
	require.True(t, present)
	require.Equal(t, span(0, sent), readAll(t, c2, sent))
	c2.nothingMore(t)
}

// What an embedder removes from the offline session (here every multiple of
// three, from the in-flight window and from the queue behind it) is not
// delivered on resume; the rest arrives in order.
func TestRemoveQueuedBeforeResumeIsNotDelivered(t *testing.T) {
	const window, sent = 8, 40
	s, addr, _ := startQueueBroker(t, window, 1000)
	c, _ := connectSubscriber(t, addr, "filtered", 0, false)
	c.subscribe(t)
	cl := goOffline(t, s, c, "filtered")

	publishNumbered(t, s, 0, sent)

	var want []string
	removed := 0
	for i := 0; i < sent; i++ {
		if i%3 == 0 {
			removed++
			continue
		}
		want = append(want, strconv.Itoa(i))
	}
	n := cl.RemoveQueued(func(pk packets.Packet) bool {
		v, err := strconv.Atoi(string(pk.Payload))
		return err == nil && v%3 == 0
	})
	require.Equal(t, removed, n)

	c2, present := connectSubscriber(t, addr, "filtered", 0, false)
	require.True(t, present)
	require.Equal(t, want, readAll(t, c2, len(want)))
	c2.nothingMore(t)
}

// publishOnConnack publishes one message the moment the server has written
// a CONNACK: the instant a resumed session is registered and connected but
// its backlog has not been re-sent yet.
type publishOnConnack struct {
	mqtt.HookBase
	s       *mqtt.Server
	payload string
	armed   chan struct{}
}

func (h *publishOnConnack) ID() string           { return "publish-on-connack" }
func (h *publishOnConnack) Provides(b byte) bool { return b == mqtt.OnPacketSent }
func (h *publishOnConnack) OnPacketSent(_ *mqtt.Client, pk packets.Packet, _ []byte) {
	if pk.FixedHeader.Type != packets.Connack {
		return
	}
	select {
	case <-h.armed:
		_ = h.s.Publish("q", []byte(h.payload), false, 1)
	default:
	}
}

// A message published while a session is resuming — registered, its
// CONNACK written, its backlog not yet re-sent — arrives AFTER the backlog,
// never in front of it [MQTT-4.6.0-1]. Before, it went straight to the
// connection and overtook every message the session had been holding.
func TestPublishDuringResumeFollowsTheBacklog(t *testing.T) {
	const sent = 12 // all of it in flight: nothing queued for "live" to wait behind
	s, addr, _ := startQueueBroker(t, 0, 0)
	hook := &publishOnConnack{s: s, payload: "live", armed: make(chan struct{})}
	require.NoError(t, s.AddHook(hook, nil))

	c, _ := connectSubscriber(t, addr, "resuming", 0, false)
	c.subscribe(t)
	goOffline(t, s, c, "resuming")
	publishNumbered(t, s, 0, sent)

	close(hook.armed)
	c2, present := connectSubscriber(t, addr, "resuming", 0, false)
	require.True(t, present)
	require.Equal(t, append(span(0, sent), "live"), readAll(t, c2, sent+1))
	c2.nothingMore(t)
}

// EndSession discards an offline session at once: the next CONNECT under
// its client id finds no session, nothing it had queued is delivered, and
// its subscription is gone. A second call, or one on a session a new
// connection took over, does nothing.
func TestEndSessionDiscardsAnOfflineSession(t *testing.T) {
	s, addr, _ := startQueueBroker(t, 4, 100)
	c, _ := connectSubscriber(t, addr, "ended", 0, false)
	c.subscribe(t)
	cl := goOffline(t, s, c, "ended")
	publishNumbered(t, s, 0, 10)

	require.True(t, s.EndSession(cl))
	require.False(t, s.EndSession(cl), "ended once")
	_, ok := s.Clients.Get("ended")
	require.False(t, ok, "the client id is released")

	c2, present := connectSubscriber(t, addr, "ended", 0, false)
	require.False(t, present)
	publishNumbered(t, s, 10, 11)
	c2.nothingMore(t) // neither the backlog nor, without a subscription, the new one

	// A session taken over belongs to the new connection.
	c2.subscribe(t)
	old := goOffline(t, s, c2, "ended")
	c3, present := connectSubscriber(t, addr, "ended", 0, false)
	require.True(t, present)
	require.False(t, s.EndSession(old), "taken over: not the old client's to end")
	publishNumbered(t, s, 11, 12)
	require.Equal(t, []string{"11"}, readAll(t, c3, 1))
}
