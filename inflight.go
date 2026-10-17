// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
)

// Inflight is a map of InflightMessage keyed on packet id.
type Inflight struct {
	sync.RWMutex
	internal            map[uint16]packets.Packet // internal contains the inflight packets
	receiveQuota        int32                     // remaining inbound qos quota for flow control
	sendQuota           int32                     // remaining outbound qos quota for flow control
	maximumReceiveQuota int32                     // maximum allowed receive quota
	maximumSendQuota    int32                     // maximum allowed send quota

	// MAESTROHUB PATCH 13: order is when each packet id was stored, from
	// next — the order the packets were sent, which GetAll returns
	// [MQTT-4.6.0-1]. Created cannot give it: it is in seconds (a whole
	// window shares one), and a packet id wraps.
	order map[uint16]uint64
	next  uint64
}

// NewInflights returns a new instance of an Inflight packets map.
func NewInflights() *Inflight {
	return &Inflight{
		internal: map[uint16]packets.Packet{},
		order:    map[uint16]uint64{},
	}
}

// Set adds or updates an inflight packet by packet id.
func (i *Inflight) Set(m packets.Packet) bool {
	i.Lock()
	defer i.Unlock()

	_, ok := i.internal[m.PacketID]
	i.internal[m.PacketID] = m
	if !ok { // a packet replaced under its id (PUBLISH by PUBREL) keeps its place
		if i.order == nil {
			i.order = map[uint16]uint64{}
		}
		i.next++
		i.order[m.PacketID] = i.next
	}
	return !ok
}

// Get returns an inflight packet by packet id.
func (i *Inflight) Get(id uint16) (packets.Packet, bool) {
	i.RLock()
	defer i.RUnlock()

	if m, ok := i.internal[id]; ok {
		return m, true
	}

	return packets.Packet{}, false
}

// Len returns the size of the inflight messages map.
func (i *Inflight) Len() int {
	i.RLock()
	defer i.RUnlock()
	return len(i.internal)
}

// Clone returns a new instance of Inflight with the same message data.
// This is used when transferring inflights from a taken-over session.
func (i *Inflight) Clone() *Inflight {
	c := NewInflights()
	i.RLock()
	defer i.RUnlock()
	for k, v := range i.internal {
		c.internal[k] = v
	}
	for k, v := range i.order {
		c.order[k] = v
	}
	c.next = i.next
	return c
}

// GetAll returns all the inflight messages.
func (i *Inflight) GetAll(immediate bool) []packets.Packet {
	i.RLock()
	defer i.RUnlock()

	m := []packets.Packet{}
	for _, v := range i.internal {
		if !immediate || (immediate && v.Expiry < 0) {
			m = append(m, v)
		}
	}

	// MAESTROHUB PATCH 13: in the order stored, which is the order sent
	// [MQTT-4.6.0-1]. Upstream sorted on uint16(Created): every packet of
	// the same second compared equal (an unstable sort over a map, so a
	// random order), and the truncation wrapped every 18.2 hours.
	sort.Slice(m, func(a, b int) bool {
		return i.order[m[a].PacketID] < i.order[m[b].PacketID]
	})

	return m
}

// NextImmediate returns the next inflight packet which is indicated to be sent immediately.
// This typically occurs when the quota has been exhausted, and we need to wait until new quota
// is free to continue sending.
//
// MAESTROHUB PATCH: no read lock of its own — GetAll takes it. Go's RWMutex
// is not reentrant for readers: once a writer waits in Lock, the nested
// RLock blocks behind it and the outer one never returns. With acks written
// off the reader (acks.go), ackWritten's Set runs concurrently with the
// reader's NextImmediate, and the two waited on each other for good.
func (i *Inflight) NextImmediate() (packets.Packet, bool) {
	m := i.GetAll(true)
	if len(m) > 0 {
		return m[0], true
	}

	return packets.Packet{}, false
}

// Delete removes an in-flight message from the map. Returns true if the message existed.
func (i *Inflight) Delete(id uint16) bool {
	i.Lock()
	defer i.Unlock()

	_, ok := i.internal[id]
	delete(i.internal, id)
	delete(i.order, id)

	return ok
}

// TakeRecieveQuota reduces the receive quota by 1.
func (i *Inflight) DecreaseReceiveQuota() {
	if atomic.LoadInt32(&i.receiveQuota) > 0 {
		atomic.AddInt32(&i.receiveQuota, -1)
	}
}

// TakeRecieveQuota increases the receive quota by 1.
func (i *Inflight) IncreaseReceiveQuota() {
	if atomic.LoadInt32(&i.receiveQuota) < atomic.LoadInt32(&i.maximumReceiveQuota) {
		atomic.AddInt32(&i.receiveQuota, 1)
	}
}

// ResetReceiveQuota resets the receive quota to the maximum allowed value.
func (i *Inflight) ResetReceiveQuota(n int32) {
	atomic.StoreInt32(&i.receiveQuota, n)
	atomic.StoreInt32(&i.maximumReceiveQuota, n)
}

// DecreaseSendQuota reduces the send quota by 1.
func (i *Inflight) DecreaseSendQuota() {
	if atomic.LoadInt32(&i.sendQuota) > 0 {
		atomic.AddInt32(&i.sendQuota, -1)
	}
}

// IncreaseSendQuota increases the send quota by 1.
func (i *Inflight) IncreaseSendQuota() {
	if atomic.LoadInt32(&i.sendQuota) < atomic.LoadInt32(&i.maximumSendQuota) {
		atomic.AddInt32(&i.sendQuota, 1)
	}
}

// ResetSendQuota resets the send quota to the maximum allowed value.
func (i *Inflight) ResetSendQuota(n int32) {
	atomic.StoreInt32(&i.sendQuota, n)
	atomic.StoreInt32(&i.maximumSendQuota, n)
}
