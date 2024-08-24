package resonance

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/resonance/test"
)

func TestConnection(t *testing.T) {
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 1024,
		MsgToIDFunc:    test.MsgToID,
		IDToMsgFunc:    test.IDToMsg(),
	}

	peer := NewPeerBuffer()
	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	requireT.NoError(c1.Send(&test.Message{
		Field1: "Hello! I'm peer 1",
	}))

	msg, err := c2.Receive()
	requireT.NoError(err)
	requireT.Equal("Hello! I'm peer 1", msg.(*test.Message).Field1)

	requireT.NoError(c2.Send(&test.Message{
		Field1: "Hello! I'm peer 2",
	}))

	msg, err = c1.Receive()
	requireT.NoError(err)
	requireT.Equal("Hello! I'm peer 2", msg.(*test.Message).Field1)

	requireT.NoError(c1.Ping())

	msg, err = c2.Receive()
	requireT.NoError(err)
	requireT.Nil(msg)

	requireT.NoError(c1.Send(&test.Message{
		Field1: "Good to see you!",
	}))

	msg, err = c2.Receive()
	requireT.NoError(err)
	requireT.Equal("Good to see you!", msg.(*test.Message).Field1)
}
