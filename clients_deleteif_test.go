// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

package mqtt

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

// The end of a connection whose session ends with it (clean session) must
// not delete a session another connection under the same client id holds
// by then: the tail deletes only while the id still maps to its own client
// (Clients.deleteIf) — a check of IsTakenOver alone was check-then-act.
func TestEndingConnectionKeepsTheSessionThatReplacedIt(t *testing.T) {
	s, _ := newSettleServer(t)
	s.Options.Capabilities.ReceiveMaximum = 10
	defer s.Close()
	r, w := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", r) }()
	go func() { _, _ = io.Copy(io.Discard, w) }()
	connect := packets.TPacketData[packets.Connect].Get(packets.TConnectClean)
	_, _ = w.Write(connect.RawBytes)
	id := connect.Packet.Connect.ClientIdentifier
	require.Eventually(t, func() bool { _, ok := s.Clients.Get(id); return ok }, time.Second, time.Millisecond)

	// Another connection takes the id (not through a takeover: the old
	// one is not marked), and holds a QoS 2 id in the registry.
	newer := connClient(t, s)
	newer.ID = id
	s.Clients.Add(newer)
	require.NoError(t, s.processPacket(newer, publishQos(9, 2)))
	require.Equal(t, 1, s.qos2.size())

	_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the old connection did not end")
	}
	got, ok := s.Clients.Get(id)
	require.True(t, ok, "the old connection's end deleted the session that replaced it")
	require.Same(t, newer, got)
	require.Equal(t, 1, s.qos2.size(), "the old connection's end wiped the new session's QoS 2 ids")
}

// replaceOnExpiry registers a new client under the expiring one's id at the
// moment the sweep decided to expire it — the race the sweep's
// compare-and-delete closes.
type replaceOnExpiry struct {
	HookBase
	s     *Server
	newer *Client
}

func (h *replaceOnExpiry) ID() string           { return "replace-on-expiry" }
func (h *replaceOnExpiry) Provides(b byte) bool { return b == OnClientExpired }
func (h *replaceOnExpiry) OnClientExpired(cl *Client) {
	h.newer.ID = cl.ID
	h.s.Clients.Add(h.newer)
}

func TestExpirySweepKeepsTheSessionThatReplacedIt(t *testing.T) {
	s, _ := newSettleServer(t)
	newer := connClient(t, s)
	require.NoError(t, s.AddHook(&replaceOnExpiry{s: s, newer: newer}, nil))
	old := connClient(t, s)
	old.ID = "sweep"
	s.Clients.Add(old)
	old.Properties.Props.SessionExpiryInterval, old.Properties.Props.SessionExpiryIntervalFlag = 1, true
	old.Stop(nil)
	old.State.disconnected = time.Now().Unix() - 10
	newer.ID = "sweep"
	require.NoError(t, s.processPacket(newer, publishQos(9, 2))) // the newer session's QoS 2 id

	s.clearExpiredClients(time.Now().Unix())
	got, ok := s.Clients.Get("sweep")
	require.True(t, ok, "the sweep deleted the session that replaced the expired one")
	require.Same(t, newer, got)
	require.Equal(t, 1, s.qos2.size(), "the sweep wiped the new session's QoS 2 ids")
}

func TestClientsDeleteIfOnlyItsOwn(t *testing.T) {
	cs := NewClients()
	a, _, _ := newTestClient()
	b, _, _ := newTestClient()
	a.ID, b.ID = "x", "x"
	cs.Add(a)
	cs.Add(b) // b replaced a
	require.False(t, cs.deleteIf("x", a))
	got, ok := cs.Get("x")
	require.True(t, ok)
	require.Same(t, b, got)
	require.True(t, cs.deleteIf("x", b))
	_, ok = cs.Get("x")
	require.False(t, ok)
}
