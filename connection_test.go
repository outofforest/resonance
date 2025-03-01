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

	config := Config{
		MaxMessageSize: 1024,
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	requireT.True(
		c1.SendProton(&test.Message{
			Field: "A",
		}, m),
	)

	msg, err := c2.ReceiveProton(m)
	requireT.NoError(err)

	requireT.Equal("A", msg.(*test.Message).Field)

	requireT.True(
		c2.SendProton(&test.Message{
			Field: "B",
		}, m),
	)

	msg, err = c1.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal("B", msg.(*test.Message).Field)

	requireT.True(
		c1.SendProton(&test.Message{
			Field: "C",
		}, m),
	)

	msg, err = c2.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal("C", msg.(*test.Message).Field)
}

func TestConnectionLongMessages(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 1024,
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	requireT.True(
		c1.SendProton(&test.Message{
			Field: longString + "A",
		}, m),
	)

	msg, err := c2.ReceiveProton(m)
	requireT.NoError(err)

	requireT.Equal(longString+"A", msg.(*test.Message).Field)

	requireT.True(
		c2.SendProton(&test.Message{
			Field: longString + "B",
		}, m),
	)

	msg, err = c1.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal(longString+"B", msg.(*test.Message).Field)

	requireT.True(
		c1.SendProton(&test.Message{
			Field: longString + "C",
		}, m),
	)

	msg, err = c2.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal(longString+"C", msg.(*test.Message).Field)
}

var longString = strings.Repeat("_", 300)
