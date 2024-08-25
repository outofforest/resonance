package resonance

import (
	"testing"

	"github.com/outofforest/parallel"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/resonance/pkg/sim"
	"github.com/outofforest/resonance/test"
)

func TestConnection(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 1024,
		MsgToIDFunc:    test.MsgToID,
		IDToMsgFunc:    test.IDToMsg(),
	}

	peer := NewPeerBuffer()
	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	requireT.True(
		c1.Send(&test.Message{
			Field: "Hello! I'm peer 1",
		}),
	)

	msg, ok := c2.Receive()
	requireT.True(ok)

	requireT.Equal("Hello! I'm peer 1", msg.(*test.Message).Field)

	requireT.True(
		c2.Send(&test.Message{
			Field: "Hello! I'm peer 2",
		}),
	)

	msg, ok = c1.Receive()
	requireT.True(ok)
	requireT.Equal("Hello! I'm peer 2", msg.(*test.Message).Field)

	requireT.True(
		c1.Send(&test.Message{
			Field: "Good to see you!",
		}),
	)

	msg, ok = c2.Receive()
	requireT.True(ok)
	requireT.Equal("Good to see you!", msg.(*test.Message).Field)
}
