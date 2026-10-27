// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 MaestroHub
// SPDX-FileContributor: MaestroHub

package mqtt

// A server with a maximum packet size announces it in the CONNACK
// (FORK.md patch 14).

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
)

func connackFrom(t *testing.T, s *Server, version byte) []byte {
	t.Helper()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = version
	go func() {
		require.NoError(t, s.SendConnack(cl, packets.CodeSuccess, false, nil))
		_ = w.Close()
	}()
	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	return buf
}

// An MQTT 5 client is told the limit, so it never sends a packet the
// server would refuse with 0x95. Before the fix the limit was enforced
// but never announced.
func TestServerSendConnackAnnouncesMaximumPacketSize(t *testing.T) {
	s := newServer()
	s.Options.Capabilities.MaximumPacketSize = 1179648
	buf := connackFrom(t, s, 5)

	pk := packets.Packet{ProtocolVersion: 5, FixedHeader: packets.FixedHeader{Type: packets.Connack}}
	require.NoError(t, pk.ConnackDecode(buf[2:]))
	require.Equal(t, uint32(1179648), pk.Properties.MaximumPacketSize)
}

// No limit (0) writes no property: the CONNACK of every existing
// fixture is unchanged.
func TestServerSendConnackNoMaximumPacketSizeWritesNothing(t *testing.T) {
	buf := connackFrom(t, newServer(), 5)
	pk := packets.Packet{ProtocolVersion: 5, FixedHeader: packets.FixedHeader{Type: packets.Connack}}
	require.NoError(t, pk.ConnackDecode(buf[2:]))
	require.Equal(t, uint32(0), pk.Properties.MaximumPacketSize)
}

// MQTT 3.1.1 has no CONNACK properties: its CONNACK is the same with
// or without a limit.
func TestServerSendConnackMaximumPacketSizeLeavesV311Unchanged(t *testing.T) {
	withLimit := newServer()
	withLimit.Options.Capabilities.MaximumPacketSize = 4096
	require.Equal(t, connackFrom(t, newServer(), 4), connackFrom(t, withLimit, 4))
}
