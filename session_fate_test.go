// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

package mqtt

import (
	"testing"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

// A session's fate is decided once (claimTakeover / claimEnd): a takeover
// and an end — the connection's own tail, the expiry sweep — never both
// act on it.

func sessionWithState(t *testing.T, s *Server, id string) *Client {
	t.Helper()
	old := connClient(t, s)
	old.ID = id
	sub := packets.Subscription{Filter: "a/b", Qos: 1}
	old.State.Subscriptions.Add(sub.Filter, sub)
	s.Topics.Subscribe(old.ID, sub)
	require.NoError(t, s.processPacket(old, publishQos(7, 2))) // a held QoS 2 id
	s.Clients.Add(old)
	return old
}

// The session ended first (its end owns the cleanup): a takeover arriving
// then inherits nothing — no subscriptions, no inflights — and the old
// session's QoS 2 ids no longer answer the new one.
func TestSessionEndedBeforeTakeoverIsNotInherited(t *testing.T) {
	s, _ := newSettleServer(t)
	old := sessionWithState(t, s, "fate")
	require.True(t, old.claimEnd())

	cl, _, _ := newTestClient()
	cl.ID = "fate"
	cl.Properties.ProtocolVersion = 5
	require.False(t, s.inheritClientSession(packets.Packet{Connect: packets.ConnectParams{ClientIdentifier: "fate"}}, cl),
		"a session that already ended was inherited")
	require.Equal(t, 0, cl.State.Subscriptions.Len())
	require.Equal(t, 0, s.qos2.size(), "the ended session's QoS 2 ids would answer the new session's resends")
}

// The expiry sweep running while a takeover is under way (the old client
// claimed, the new one not registered yet) leaves the session alone: the
// new connection is inheriting its subscriptions and QoS 2 ids, which
// share the client id.
func TestSweepLeavesASessionBeingTakenOver(t *testing.T) {
	s, _ := newSettleServer(t)
	old := sessionWithState(t, s, "moving")
	old.Properties.ProtocolVersion = 5
	old.Properties.Props.SessionExpiryInterval, old.Properties.Props.SessionExpiryIntervalFlag = 1, true
	old.Stop(nil)
	old.State.disconnected = 1 // long expired
	require.True(t, old.claimTakeover())

	s.clearExpiredClients(1 << 40)
	require.Len(t, s.Topics.Subscribers("a/b").Subscriptions, 1, "the sweep unsubscribed a session being taken over")
	require.Equal(t, 1, s.qos2.size(), "the sweep ended the QoS 2 ids of a session being taken over")
}

// The takeover came first: the end is not the old session's to claim any
// more (its tail must not clean up what the new connection now holds).
func TestTakeoverBeforeEndOwnsTheSession(t *testing.T) {
	s, _ := newSettleServer(t)
	old := sessionWithState(t, s, "fate2")
	cl, _, _ := newTestClient()
	cl.ID = "fate2"
	cl.Properties.ProtocolVersion = 5
	require.True(t, s.inheritClientSession(packets.Packet{Connect: packets.ConnectParams{ClientIdentifier: "fate2"}}, cl))
	require.False(t, old.claimEnd(), "a session taken over could still be ended by its old connection")
	require.Equal(t, 1, cl.State.Subscriptions.Len())
}
