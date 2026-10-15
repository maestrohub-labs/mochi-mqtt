// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

package mqtt

import (
	"bytes"
	"testing"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

// A PUBLISH too large for the client is discarded whole ([MQTT-3.1.2-25]),
// never sent with its User Properties stripped to make it fit
// ([MQTT-3.3.2-17]: they reach the receiver unaltered). v2.7.9-mh.13 sent
// it stripped; upstream did when the User Properties alone were too big.
func TestOversizePublishIsDiscardedNotStripped(t *testing.T) {
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire := readWire(r)

	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "a/b", Payload: []byte("hello"),
		Properties: packets.Properties{User: []packets.UserProperty{{Key: "k", Val: "v"}}}}
	full := pk
	full.ProtocolVersion = 5
	var buf bytes.Buffer
	require.NoError(t, full.PublishEncode(&buf))

	cl.Properties.Props.MaximumPacketSize = uint32(buf.Len() - 1) // the PUBLISH without its User Properties would fit
	require.ErrorIs(t, cl.WritePacket(pk), packets.ErrPacketTooLarge)
	expectNone(t, wire)

	cl.Properties.Props.MaximumPacketSize = uint32(buf.Len())
	require.NoError(t, cl.WritePacket(pk))
	select {
	case p := <-wire:
		require.Equal(t, packets.Publish, p.typ)
	case <-time.After(2 * time.Second):
		t.Fatal("the PUBLISH that fits was not written")
	}
}
