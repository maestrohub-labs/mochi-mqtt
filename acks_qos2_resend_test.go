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

// QoS 2 is exactly once: a PUBLISH resent while its first copy is still
// being handled (its PUBREC held) must not be handled again — neither on
// the same connection nor on the connection that resumes the session.
// Nothing is in the inflight store until the PUBREC is written, so the
// resend used to look new.

func qos2Dup(id uint16) packets.Packet {
	pk := publishQos(id, 2)
	pk.FixedHeader.Dup = true
	return pk
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
	expectAck(t, wire, packets.Pubrec, 7)
	expectNone(t, wire) // one PUBREC, not two
}

// takeover resumes old's session on a new connection, as a CONNECT with
// Clean Start 0 does.
func takeover(t *testing.T, s *Server, old *Client) (*Client, <-chan wirePacket) {
	t.Helper()
	old.Net.Conn = nil // the takeover's DISCONNECT goes nowhere (as in TestInheritClientSession)
	s.Clients.Add(old)
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	require.True(t, s.inheritClientSession(packets.Packet{Connect: packets.ConnectParams{ClientIdentifier: old.ID}}, cl))
	cl.ID = old.ID
	return cl, readWire(r)
}

func TestQos2ResendAfterTakeoverIsAnsweredByTheOriginal(t *testing.T) {
	s, h := newSettleServer(t)
	old, _, _ := newTestClient()
	old.Properties.ProtocolVersion = 5
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
	old, _, _ := newTestClient()
	old.Properties.ProtocolVersion = 5
	require.NoError(t, s.processPacket(old, publishQos(7, 2)))
	require.NoError(t, s.processPacket(old, publishQos(8, 2)))
	// 8 is settled, but its PUBREC waits behind 7 — still unwritten when
	// the session moves.
	h.release(t, 8, packets.CodeSuccess)

	cl, wire := takeover(t, s, old)
	require.NoError(t, s.processPacket(cl, qos2Dup(8)))
	expectAck(t, wire, packets.Pubrec, 8) // settled: answered at once
	require.Equal(t, int32(2), h.seen.Load(), "a resend of a settled publish was handled again")
}
