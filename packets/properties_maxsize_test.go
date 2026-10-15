// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

package packets

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// MAESTROHUB PATCH: the Reason String and User Properties are left out of a
// packet when they would make it larger than the client's Maximum Packet
// Size ([MQTT-3.4.2-2] [MQTT-3.4.2-3] and the same rule for every packet
// that carries them) — judged on the whole packet. The size check used to
// count only the bytes before the properties, not the fixed header, a
// PUBACK's reason code or the property length: a packet a few bytes over
// the limit kept the property, and the server then refused to send the
// packet at all (ErrPacketTooLarge).
func TestOptionalPropertiesRespectTheWholePacketSize(t *testing.T) {
	type build func(p Properties) Packet
	ack := func(typ byte) build {
		return func(p Properties) Packet {
			return Packet{FixedHeader: FixedHeader{Type: typ}, ProtocolVersion: 5, PacketID: 7, ReasonCode: ErrUnspecifiedError.Code, Properties: p}
		}
	}
	kinds := []struct {
		name  string
		build build
		// reasonString: the packet type may carry a Reason String
		reasonString bool
	}{
		{"puback", ack(Puback), true},
		{"pubrec", ack(Pubrec), true},
		{"pubrel", ack(Pubrel), true},
		{"pubcomp", ack(Pubcomp), true},
		{"suback", func(p Properties) Packet {
			return Packet{FixedHeader: FixedHeader{Type: Suback}, ProtocolVersion: 5, PacketID: 7, ReasonCodes: []byte{0, 1, 0x80}, Properties: p}
		}, true},
		{"unsuback", func(p Properties) Packet {
			return Packet{FixedHeader: FixedHeader{Type: Unsuback}, ProtocolVersion: 5, PacketID: 7, ReasonCodes: []byte{0, 0x11, 0x80}, Properties: p}
		}, true},
		{"connack", func(p Properties) Packet {
			return Packet{FixedHeader: FixedHeader{Type: Connack}, ProtocolVersion: 5, ReasonCode: ErrNotAuthorized.Code, Properties: p}
		}, true},
		{"disconnect", func(p Properties) Packet {
			return Packet{FixedHeader: FixedHeader{Type: Disconnect}, ProtocolVersion: 5, ReasonCode: ErrServerShuttingDown.Code, Properties: p}
		}, true},
		{"auth", func(p Properties) Packet {
			return Packet{FixedHeader: FixedHeader{Type: Auth}, ProtocolVersion: 5, ReasonCode: CodeContinueAuthentication.Code, Properties: p}
		}, true},
		{"publish", func(p Properties) Packet {
			return Packet{FixedHeader: FixedHeader{Type: Publish, Qos: 1}, ProtocolVersion: 5, PacketID: 7, TopicName: "a/b", Payload: []byte("hello"), Properties: p}
		}, false},
	}
	encode := func(t *testing.T, pk Packet, max uint32) []byte {
		t.Helper()
		pk.Mods.MaxSize = max
		var buf bytes.Buffer
		var err error
		switch pk.FixedHeader.Type {
		case Puback:
			err = pk.PubackEncode(&buf)
		case Pubrec:
			err = pk.PubrecEncode(&buf)
		case Pubrel:
			err = pk.PubrelEncode(&buf)
		case Pubcomp:
			err = pk.PubcompEncode(&buf)
		case Suback:
			err = pk.SubackEncode(&buf)
		case Unsuback:
			err = pk.UnsubackEncode(&buf)
		case Connack:
			err = pk.ConnackEncode(&buf)
		case Disconnect:
			err = pk.DisconnectEncode(&buf)
		case Auth:
			err = pk.AuthEncode(&buf)
		case Publish:
			err = pk.PublishEncode(&buf)
		}
		require.NoError(t, err)
		return buf.Bytes()
	}

	for _, k := range kinds {
		for _, opt := range []struct {
			name  string
			props Properties
			only  bool // only for packet types that carry a Reason String
		}{
			{"reason string", Properties{ReasonString: "abcdefghij"}, true},
			{"user property", Properties{User: []UserProperty{{Key: "k", Val: "abcdefgh"}}}, false},
		} {
			if opt.only && !k.reasonString {
				continue
			}
			t.Run(k.name+"/"+opt.name, func(t *testing.T) {
				bare := len(encode(t, k.build(Properties{}), 0))
				full := len(encode(t, k.build(opt.props), 0))
				require.Greater(t, full, bare)
				for max := bare; max <= full+1; max++ {
					got := encode(t, k.build(opt.props), uint32(max))
					require.LessOrEqual(t, len(got), max, "max %d: the packet exceeds the client's maximum", max)
					if max >= full {
						require.Len(t, got, full, fmt.Sprintf("max %d: the property fits and is sent", max))
					} else {
						require.Len(t, got, bare, fmt.Sprintf("max %d: the property does not fit and is left out", max))
					}
				}
			})
		}
	}
}

// The reviewer's case: an MQTT 5 refusal PUBACK with Reason String
// "abcdefghij" is 19 bytes, the bare refusal 5.
func TestRefusalPubackReasonStringBoundary(t *testing.T) {
	for max, want := range map[uint32]int{15: 5, 16: 5, 17: 5, 18: 5, 19: 19, 20: 19} {
		pk := Packet{FixedHeader: FixedHeader{Type: Puback}, ProtocolVersion: 5, PacketID: 7, ReasonCode: ErrUnspecifiedError.Code,
			Properties: Properties{ReasonString: "abcdefghij"}, Mods: Mods{MaxSize: max}}
		var buf bytes.Buffer
		require.NoError(t, pk.PubackEncode(&buf))
		require.Equal(t, want, buf.Len(), "max %d", max)
	}
}
