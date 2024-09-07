package resonance

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance/pkg/sim"
	"github.com/outofforest/resonance/test"
)

func TestConnection(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config[test.Marshaller]{
		MaxMessageSize:    1024,
		MarshallerFactory: test.NewMarshaller,
	}

	peer := NewPeerBuffer()

	recvCh1 := make(chan any, 500)
	recvCh2 := make(chan any, 500)
	c1 := NewConnection(peer, config, recvCh1)
	c2 := NewConnection(peer.OtherPeer(), config, recvCh2)

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	requireT.True(
		c1.Send(&test.Message{
			Field: "Hello! I'm peer 1",
		}),
	)

	msg, ok := <-recvCh2
	requireT.True(ok)

	requireT.Equal("Hello! I'm peer 1", msg.(*test.Message).Field)

	requireT.True(
		c2.Send(&test.Message{
			Field: "Hello! I'm peer 2",
		}),
	)

	msg, ok = <-recvCh1
	requireT.True(ok)
	requireT.Equal("Hello! I'm peer 2", msg.(*test.Message).Field)

	requireT.True(
		c1.Send(&test.Message{
			Field: "Good to see you!",
		}),
	)

	msg, ok = <-recvCh2
	requireT.True(ok)
	requireT.Equal("Good to see you!", msg.(*test.Message).Field)
}
