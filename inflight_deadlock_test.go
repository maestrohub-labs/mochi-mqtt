package mqtt

import (
	"io"
	"os"
	"runtime/pprof"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestrohub-labs/mochi-mqtt/v2/packets"
)

// D1: a QoS 2 publisher whose PUBRECs are settled off the reader: the ack
// writer's Inflight.Set (ackWritten) races the reader's NextImmediate,
// which read-locks Inflight twice (recursive RLock) -> deadlock.
type asyncHook struct{ HookBase }

func (h *asyncHook) ID() string           { return "async" }
func (h *asyncHook) Provides(b byte) bool { return b == OnPublishedSettle }
func (h *asyncHook) OnPublishedSettle(cl *Client, pk packets.Packet, s *PublishSettlement) {
	rel := s.Hold()
	go rel(packets.CodeSuccess) // settled on a worker, as MaestroHub's pool does
}

func TestR3_NextImmediateDeadlock(t *testing.T) {
	s := newServer()
	if err := s.AddHook(&asyncHook{}, nil); err != nil {
		t.Fatal(err)
	}
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	go func() { _, _ = io.Copy(io.Discard, r) }()
	done := make(chan struct{})
	var prog atomic.Int64
	go func() {
		defer close(done)
		cl.State.Inflight.Set(s.buildAck(100, packets.Pubrec, 0, packets.Properties{}, packets.CodeSuccess)) // a flow awaiting PUBREL: Len > 0
		for n := 0; n < 20000; n++ {
			for id := uint16(1); id <= 8; id++ {
				_ = s.processPacket(cl, publishQos(id, 2))
			}
			for id := uint16(1); id <= 8; id++ {
				for i := 0; i < 10000; i++ {
					if _, ok := cl.State.Inflight.Get(id); ok {
						break
					}
					time.Sleep(time.Microsecond)
				}
				_ = s.processPacket(cl, pubrelPk(id))
				prog.Add(1)
			}
		}
	}()
	select {
	case <-done:
		t.Log("no deadlock in 160k exchanges")
	case <-time.After(60 * time.Second):
		a := prog.Load()
		time.Sleep(5 * time.Second)
		f, _ := os.Create("/tmp/claude-1000/-home-barisvelioglu-projects-maestrohub-3-maestrohub/07c13057-8182-4dbb-8da3-76e1a467f9ec/scratchpad/review-fork7c/dl-goroutines.txt")
		_ = pprof.Lookup("goroutine").WriteTo(f, 2)
		f.Close()
		// another client's reader delivering a QoS 1 message to this client
		other, _, _ := newTestClient()
		other.ID = "pub"
		s.Clients.Add(cl)
		sub := packets.Subscription{Filter: "a/b/c", Qos: 1}
		s.Topics.Subscribe(cl.ID, sub)
		cl.State.Subscriptions.Add(sub.Filter, sub)
		pd := make(chan struct{})
		go func() { pk := publishQos(9, 1); pk.TopicName = "a/b/c"; _ = s.processPacket(other, pk); close(pd) }()
		select {
		case <-pd:
			t.Log("other publisher not blocked")
		case <-time.After(3 * time.Second):
			t.Log("CASCADE: another client's publish to this subscriber is blocked too")
		}
		t.Fatalf("reader stuck: progress %d then %d after 5s", a, prog.Load())
	}
}
