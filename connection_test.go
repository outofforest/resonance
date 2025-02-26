package resonance

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance/pkg/sim"
	"github.com/outofforest/resonance/test"
)

func TestConnectionShortMessages(t *testing.T) {
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
			Field: "A",
		}),
	)

	msg, ok := <-recvCh2
	requireT.True(ok)

	requireT.Equal("A", msg.(*test.Message).Field)

	requireT.True(
		c2.Send(&test.Message{
			Field: "B",
		}),
	)

	msg, ok = <-recvCh1
	requireT.True(ok)
	requireT.Equal("B", msg.(*test.Message).Field)

	requireT.True(
		c1.Send(&test.Message{
			Field: "C",
		}),
	)

	msg, ok = <-recvCh2
	requireT.True(ok)
	requireT.Equal("C", msg.(*test.Message).Field)
}

func TestConnectionLongMessages(t *testing.T) {
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
			Field: longString + "A",
		}),
	)

	msg, ok := <-recvCh2
	requireT.True(ok)

	requireT.Equal(longString+"A", msg.(*test.Message).Field)

	requireT.True(
		c2.Send(&test.Message{
			Field: longString + "B",
		}),
	)

	msg, ok = <-recvCh1
	requireT.True(ok)
	requireT.Equal(longString+"B", msg.(*test.Message).Field)

	requireT.True(
		c1.Send(&test.Message{
			Field: longString + "C",
		}),
	)

	msg, ok = <-recvCh2
	requireT.True(ok)
	requireT.Equal(longString+"C", msg.(*test.Message).Field)
}

var longString = strings.Repeat("_", 300)
