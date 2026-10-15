// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: MaestroHub

package packets

import (
	"bytes"
	"fmt"
	"strings"
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

	long := func(n int) string { return strings.Repeat("r", n) }
	for _, k := range kinds {
		for _, opt := range []struct {
			name  string
			props Properties
			only  bool // only for packet types that carry a Reason String
		}{
			{"reason string", Properties{ReasonString: "abcdefghij"}, true},
			{"user property", Properties{User: []UserProperty{{Key: "k", Val: "abcdefgh"}}}, false},
			// The remaining length (and the property length) crosses a
			// Variable Byte Integer boundary: 127 → 128 and 16383 → 16384,
			// so the fixed header grows by a byte.
			{"reason string 150", Properties{ReasonString: long(150)}, true},
			{"reason string 16400", Properties{ReasonString: long(16400)}, true},
			{"user property 150", Properties{User: []UserProperty{{Key: "k", Val: long(150)}}}, false},
			{"user property 16400", Properties{User: []UserProperty{{Key: "k", Val: long(16400)}}}, false},
		} {
			if opt.only && !k.reasonString {
				continue
			}
			t.Run(k.name+"/"+opt.name, func(t *testing.T) {
				bare := len(encode(t, k.build(Properties{}), 0))
				full := len(encode(t, k.build(opt.props), 0))
				require.Greater(t, full, bare)
				var maxes []int
				if full-bare <= 300 {
					for max := bare; max <= full+1; max++ {
						maxes = append(maxes, max)
					}
				} else {
					maxes = []int{bare, bare + 1, bare + 2, bare + 3, full - 3, full - 2, full - 1, full, full + 1}
				}
				for _, max := range maxes {
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

	// Both optional properties, room for one: the Reason String is kept
	// (the User Properties here are smaller, so they alone would fit too).
	for _, k := range kinds {
		if !k.reasonString {
			continue
		}
		t.Run(k.name+"/reason string kept before user properties", func(t *testing.T) {
			both := Properties{ReasonString: "a reason string", User: []UserProperty{{Key: "k", Val: "v"}}}
			reasonOnly := encode(t, k.build(Properties{ReasonString: both.ReasonString}), 0)
			userOnly := encode(t, k.build(Properties{User: both.User}), 0)
			require.Less(t, len(userOnly), len(reasonOnly))
			got := encode(t, k.build(both), uint32(len(reasonOnly)))
			require.Equal(t, reasonOnly, got)
		})
	}
}

// [MQTT-3.3.2-17] a PUBLISH's User Properties reach the receiver
// unaltered, and [MQTT-3.1.2-29] exempts PUBLISH from Request Problem
// Information: they are never left out. A PUBLISH too large for the client
// is discarded whole ([MQTT-3.1.2-25]; writePacket: ErrPacketTooLarge,
// fixture TPublishDropOversize), never sent stripped.
func TestPublishUserPropertiesAreNeverStripped(t *testing.T) {
	build := func(mods Mods) Packet {
		mods.AllowResponseInfo = true
		return Packet{FixedHeader: FixedHeader{Type: Publish, Qos: 1}, ProtocolVersion: 5, PacketID: 7, TopicName: "a/b", Payload: []byte("hello"),
			Properties: Properties{CorrelationData: []byte(strings.Repeat("c", 200)), User: []UserProperty{{Key: "k", Val: "v"}}}, Mods: mods}
	}
	enc := func(pk Packet) []byte {
		var buf bytes.Buffer
		require.NoError(t, pk.PublishEncode(&buf))
		return buf.Bytes()
	}
	full := enc(build(Mods{}))
	for _, max := range []int{10, 100, len(full) - 10, len(full) - 1, len(full), len(full) + 1} {
		require.Equal(t, full, enc(build(Mods{MaxSize: uint32(max)})), "max %d: the PUBLISH was altered", max)
	}
	require.Equal(t, full, enc(build(Mods{DisallowProblemInfo: true})), "Request Problem Information 0 stripped a PUBLISH's User Properties")
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
