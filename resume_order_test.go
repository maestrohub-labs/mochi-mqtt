// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 MaestroHub

package mqtt

// MAESTROHUB PATCH 13 (inflight.go, pending.go): a resumed session's
// in-flight messages are re-sent in the order they were sent
// [MQTT-4.6.0-1], and an embedder can remove queued messages (in flight
// or waiting) from a session before it resumes.

import (
	"sync/atomic"
	"testing"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
	"github.com/stretchr/testify/require"
)

func publishIDs(pks []packets.Packet) []uint16 {
	out := make([]uint16, 0, len(pks))
	for _, pk := range pks {
		out = append(out, pk.PacketID)
	}
	return out
}

// Packets made in the same second, with packet ids that wrap past 65535
// while they are made: GetAll returns them in the order they were stored.
// Upstream sorted on uint16(Created) — equal for all of these — with an
// unstable sort over a map, so the order was random; a packet-id
// tie-break would put the wrapped ids (1, 2) first. Repeated: a random
// order passes one trial out of many.
func TestInflightGetAllReturnsSendOrder(t *testing.T) {
	const second int64 = 1700000000
	sent := []uint16{65531, 65532, 65533, 65534, 65535, 1, 2, 3}
	for trial := 0; trial < 50; trial++ {
		in := NewInflights()
		for _, id := range sent {
			in.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: id, Created: second})
		}
		require.Equal(t, sent, publishIDs(in.GetAll(false)), "trial %d", trial)
	}
}

// A packet created in an earlier second never follows a later one, even
// across a 65536-second boundary (upstream truncated Created to uint16).
func TestInflightGetAllOrderAcrossSeconds(t *testing.T) {
	const boundary int64 = 1786970112 // a multiple of 65536
	in := NewInflights()
	in.Set(packets.Packet{PacketID: 9, Created: boundary - 1})
	in.Set(packets.Packet{PacketID: 2, Created: boundary + 1})
	require.Equal(t, []uint16{9, 2}, publishIDs(in.GetAll(false)))
}

// Replacing a packet under its id (a QoS 2 PUBLISH by its PUBREL) keeps its
// place; a deleted id that is reused goes to the end; a clone (a session
// taken over) keeps the order and continues it.
func TestInflightOrderSurvivesReplaceDeleteAndClone(t *testing.T) {
	in := NewInflights()
	for _, id := range []uint16{4, 5, 6} {
		in.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}, PacketID: id})
	}
	in.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrel}, PacketID: 4})
	require.Equal(t, []uint16{4, 5, 6}, publishIDs(in.GetAll(false)))

	in.Delete(5)
	in.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: 5})
	require.Equal(t, []uint16{4, 6, 5}, publishIDs(in.GetAll(false)))

	c := in.Clone()
	c.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: 1})
	require.Equal(t, []uint16{4, 6, 5, 1}, publishIDs(c.GetAll(false)))
	require.Equal(t, []uint16{4, 6, 5}, publishIDs(in.GetAll(false)), "the clone is independent")
}

// RemoveQueued takes the matching PUBLISH packets out of the in-flight store
// (returning their send quota and the server's in-flight count) and out of
// the pending queue (keeping the order of the rest), and leaves every
// other in-flight packet — a PUBREL the client is owed, an inbound QoS 2
// PUBREC — alone.
func TestRemoveQueuedFiltersInflightAndPending(t *testing.T) {
	cl, _, _ := newTestClientNoWriteLoop()
	cl.Stop(nil) // offline: everything stays queued
	cl.State.Inflight.ResetSendQuota(10)

	pub := func(id uint16, payload string) packets.Packet {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: id, TopicName: "t/" + payload, Payload: []byte(payload)}
	}
	for i, p := range []string{"a", "drop1", "b"} {
		pk := pub(uint16(i+1), p)
		cl.State.Inflight.Set(pk)
		cl.State.Inflight.DecreaseSendQuota()
		atomic.AddInt64(&cl.ops.info.Inflight, 1)
	}
	cl.State.Inflight.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrel}, PacketID: 4, TopicName: "t/drop-rel"})
	cl.State.Inflight.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: 5, TopicName: "t/drop-rec"})
	cl.State.pending.mu.Lock()
	for _, p := range []string{"c", "drop2", "d", "drop3"} {
		pk := pub(0, p)
		cl.State.pending.push(&pk, 100)
	}
	cl.State.pending.mu.Unlock()

	n := cl.RemoveQueued(func(pk packets.Packet) bool {
		return len(pk.TopicName) > 6 && pk.TopicName[:6] == "t/drop"
	})
	require.Equal(t, 3, n, "one in flight and two waiting")

	var left []string
	for _, pk := range cl.State.Inflight.GetAll(false) {
		left = append(left, pk.TopicName)
	}
	require.Equal(t, []string{"t/a", "t/b", "t/drop-rel", "t/drop-rec"}, left, "only PUBLISH packets are removed from the in-flight store")
	require.Equal(t, int32(8), atomic.LoadInt32(&cl.State.Inflight.sendQuota), "the removed message's send quota is returned")
	require.Equal(t, int64(2), atomic.LoadInt64(&cl.ops.info.Inflight))

	var waiting []string
	for _, pk := range cl.State.pending.takeAll() {
		waiting = append(waiting, string(pk.Payload))
	}
	require.Equal(t, []string{"c", "d"}, waiting, "the rest of the queue keeps its order")
	require.Equal(t, 2, cl.RemoveQueued(func(packets.Packet) bool { return true }), "the two PUBLISH packets still in flight")
	require.Equal(t, 2, cl.State.Inflight.Len(), "the PUBREL and PUBREC stay")
}
