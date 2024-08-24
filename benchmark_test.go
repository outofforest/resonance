package resonance

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/samber/lo"

	"github.com/outofforest/resonance/test"
)

// go test -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

var (
	msg = func() *test.Message {
		randBytes := make([]byte, 1024)
		lo.Must(rand.Read(randBytes))

		return &test.Message{
			Field1: "Hello world!",
			Field4: randBytes,
		}
	}()
)

func BenchmarkConnection(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	b.Cleanup(cancel)

	config := Config{
		MaxMessageSize: msg.Size(),
		MsgToIDFunc:    test.MsgToID,
		IDToMsgFunc:    test.IDToMsg(),
	}

	peer := NewPeerBuffer()
	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	var msg2 any

	_ = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("sender", parallel.Fail, func(ctx context.Context) error {
			b.StartTimer()
			for range b.N {
				_ = c1.Send(msg)
			}
			return nil
		})
		spawn("receiver", parallel.Fail, func(ctx context.Context) error {
			for range b.N {
				msg2, _ = c2.Receive()
			}
			b.StopTimer()
			return nil
		})
		return nil
	})

	_, _ = fmt.Fprint(io.Discard, msg2)
}
