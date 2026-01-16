package resonance

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/parallel"
	"github.com/outofforest/qa"
	"github.com/outofforest/resonance/test"
	"github.com/outofforest/varuint64"
)

func TestConnectionProtonShort(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())

	_, err := c1.SendProton(&test.Message{
		Field: "A",
	}, m)
	requireT.NoError(err)

	msg, _, err := c2.ReceiveProton(m)
	requireT.NoError(err)

	requireT.Equal("A", msg.(*test.Message).Field)

	requireT.NoError(c2.sendPing())
	_, err = c2.SendProton(&test.Message{
		Field: "B",
	}, m)
	requireT.NoError(err)

	msg, _, err = c1.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal("B", msg.(*test.Message).Field)

	requireT.NoError(c1.sendPing())

	for range 1000 {
		_, err := c1.SendProton(&test.Message{
			Field: "C",
		}, m)
		requireT.NoError(err)
		_, err = c1.SendProton(&test.Message{
			Field: "D",
		}, m)
		requireT.NoError(err)

		msg, _, err = c2.ReceiveProton(m)
		requireT.NoError(err)
		requireT.Equal("C", msg.(*test.Message).Field)
		msg, _, err = c2.ReceiveProton(m)
		requireT.NoError(err)
		requireT.Equal("D", msg.(*test.Message).Field)
	}
}

func TestConnectionProtonLong(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + 3),
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())

	_, err := c1.SendProton(&test.Message{
		Field: longString + "A",
	}, m)
	requireT.NoError(err)

	msg, _, err := c2.ReceiveProton(m)
	requireT.NoError(err)

	requireT.Equal(longString+"A", msg.(*test.Message).Field)

	requireT.NoError(c2.sendPing())
	_, err = c2.SendProton(&test.Message{
		Field: longString + "B",
	}, m)
	requireT.NoError(err)

	msg, _, err = c1.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal(longString+"B", msg.(*test.Message).Field)

	requireT.NoError(c1.sendPing())

	for range 1000 {
		_, err := c1.SendProton(&test.Message{
			Field: longString + "C",
		}, m)
		requireT.NoError(err)
		_, err = c1.SendProton(&test.Message{
			Field: longString + "D",
		}, m)
		requireT.NoError(err)

		msg, _, err = c2.ReceiveProton(m)
		requireT.NoError(err)
		requireT.Equal(longString+"C", msg.(*test.Message).Field)
		msg, _, err = c2.ReceiveProton(m)
		requireT.NoError(err)
		requireT.Equal(longString+"D", msg.(*test.Message).Field)
	}
}

func TestConnectionProtonSendMaxMessage(t *testing.T) {
	requireT := require.New(t)

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c := NewConnection(peer, Config{
		MaxMessageSize: uint64(len(longString) + 2),
	})
	c.BufferReads()
	c.BufferWrites()

	_, err := c.SendProton(&test.Message{
		Field: longString,
	}, m)
	requireT.NoError(err)
}

func TestConnectionProtonSendTooBigMessage(t *testing.T) {
	requireT := require.New(t)

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c := NewConnection(peer, Config{
		MaxMessageSize: uint64(len(longString) + 1),
	})
	c.BufferReads()
	c.BufferWrites()

	_, err := c.SendProton(&test.Message{
		Field: longString,
	}, m)
	requireT.Error(err)
}

func TestConnectionProtonReceiveMaxMessage(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, Config{
		MaxMessageSize: uint64(len(longString) + 10),
	})
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), Config{
		MaxMessageSize: uint64(len(longString) + 2),
	})
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	_, err := c1.SendProton(&test.Message{
		Field: longString,
	}, m)
	requireT.NoError(err)

	msg, _, err := c2.ReceiveProton(m)
	requireT.NoError(err)
	requireT.NotNil(msg)
}

func TestConnectionProtonReceiveTooBigMessage(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, Config{
		MaxMessageSize: uint64(len(longString) + 10),
	})
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), Config{
		MaxMessageSize: uint64(len(longString) + 1),
	})
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	_, err := c1.SendProton(&test.Message{
		Field: longString,
	}, m)
	requireT.NoError(err)

	msg, _, err := c2.ReceiveProton(m)
	requireT.Error(err)
	requireT.Nil(msg)
}

func TestConnectionProtonReceiveInvalidMessage1(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + len(longString)/2),
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	buf := make([]byte, config.MaxMessageSize+varuint64.MaxSize)
	id, size, err := m.Marshal(&test.Message{
		Field: longString,
	}, buf[varuint64.MaxSize:])
	requireT.NoError(err)
	buf = buf[varuint64.MaxSize-varuint64.Size(id):]
	size += varuint64.Put(buf, id)
	buf[1]++ // Set invalid length of string inside message.

	_, err = c1.SendBytes(buf[:size])
	requireT.NoError(err)

	msg, _, err := c2.ReceiveProton(m)
	requireT.Error(err)
	requireT.Nil(msg)
}

func TestConnectionProtonReceiveInvalidMessage2(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + len(longString)/2),
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	buf := make([]byte, config.MaxMessageSize+varuint64.MaxSize)
	id, size, err := m.Marshal(&test.Message{
		Field: longString,
	}, buf[varuint64.MaxSize:])
	requireT.NoError(err)
	buf = buf[varuint64.MaxSize-varuint64.Size(id):]
	size += varuint64.Put(buf, id)
	buf[2] = 0x80 - 1 // Set invalid length of string inside message.

	_, err = c1.SendBytes(buf[:size])
	requireT.NoError(err)

	msg, _, err := c2.ReceiveProton(m)
	requireT.Error(err)
	requireT.Nil(msg)
}

func TestConnectionProtonMessageSize(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	msg := &test.Message{
		Field: "A",
	}
	expectedMsgSize, err := m.Size(msg)
	requireT.NoError(err)
	sentSize, err := c1.SendProton(msg, m)
	requireT.NoError(err)
	requireT.Equal(expectedMsgSize, sentSize)

	_, receivedSize, err := c2.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal(expectedMsgSize, receivedSize)
}

func TestConnectionBytesShort(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())

	_, err := c1.SendBytes([]byte{0x01})
	requireT.NoError(err)

	msg, _, err := c2.ReceiveBytes()
	requireT.NoError(err)

	requireT.Equal([]byte{0x01}, msg)

	requireT.NoError(c2.sendPing())
	_, err = c2.SendBytes([]byte{0x02})
	requireT.NoError(err)

	msg, _, err = c1.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal([]byte{0x02}, msg)

	requireT.NoError(c1.sendPing())

	for range 1000 {
		_, err := c1.SendBytes([]byte{0x03})
		requireT.NoError(err)
		_, err = c1.SendBytes([]byte{0x04})
		requireT.NoError(err)

		msg, _, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte{0x03}, msg)
		msg, _, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte{0x04}, msg)
	}
}

func TestConnectionBytesLong(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + 1),
	}

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())

	_, err := c1.SendBytes([]byte(longString + "A"))
	requireT.NoError(err)

	msg, _, err := c2.ReceiveBytes()
	requireT.NoError(err)

	requireT.Equal([]byte(longString+"A"), msg)

	requireT.NoError(c2.sendPing())
	_, err = c2.SendBytes([]byte(longString + "B"))
	requireT.NoError(err)

	msg, _, err = c1.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal([]byte(longString+"B"), msg)

	requireT.NoError(c1.sendPing())

	for range 1000 {
		_, err := c1.SendBytes([]byte(longString + "C"))
		requireT.NoError(err)
		_, err = c1.SendBytes([]byte(longString + "D"))
		requireT.NoError(err)

		msg, _, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte(longString+"C"), msg)
		msg, _, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte(longString+"D"), msg)
	}
}

func TestConnectionBytesSendMaxMessage(t *testing.T) {
	requireT := require.New(t)

	peer := NewPeerBuffer()

	c := NewConnection(peer, Config{
		MaxMessageSize: 1,
	})
	c.BufferReads()
	c.BufferWrites()

	_, err := c.SendBytes([]byte{0x00, 0x01})
	requireT.NoError(err)
}

func TestConnectionBytesSendTooBigMessage(t *testing.T) {
	requireT := require.New(t)

	peer := NewPeerBuffer()

	c := NewConnection(peer, Config{
		MaxMessageSize: 1,
	})
	c.BufferReads()
	c.BufferWrites()

	_, err := c.SendBytes([]byte{0x00, 0x01, 0x02})
	requireT.Error(err)
}

func TestConnectionBytesReceiveMaxMessage(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, Config{
		MaxMessageSize: uint64(len(longString) + 10),
	})
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), Config{
		MaxMessageSize: 1,
	})
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	_, err := c1.SendBytes([]byte{0x01, 0x02})
	requireT.NoError(err)

	msg, _, err := c2.ReceiveBytes()
	requireT.NoError(err)
	requireT.NotNil(msg)
}

func TestConnectionBytesReceiveTooBigMessage(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, Config{
		MaxMessageSize: uint64(len(longString) + 10),
	})
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), Config{
		MaxMessageSize: 1,
	})
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	_, err := c1.SendBytes([]byte{0x01, 0x02, 0x03})
	requireT.NoError(err)

	msg, _, err := c2.ReceiveBytes()
	requireT.Error(err)
	requireT.Nil(msg)
}

func TestConnectionBytesMessageSize(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	sizeSent, err := c1.SendBytes([]byte{0x01, 0x02})
	requireT.NoError(err)
	requireT.EqualValues(1, sizeSent)

	_, sizeReceived, err := c2.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal(sizeSent, sizeReceived)
}

func TestConnectionSendBytesReceiveProton(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + len(longString)/2),
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	buf := make([]byte, config.MaxMessageSize+varuint64.MaxSize)
	id, size, err := m.Marshal(&test.Message{
		Field: longString,
	}, buf[varuint64.MaxSize:])
	requireT.NoError(err)
	buf = buf[varuint64.MaxSize-varuint64.Size(id):]
	size += varuint64.Put(buf, id)

	_, err = c1.SendBytes(buf[:size])
	requireT.NoError(err)

	msg, _, err := c2.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal(&test.Message{
		Field: longString,
	}, msg)
}

func TestConnectionSendProtonReceiveBytes(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + len(longString)/2),
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	msg := &test.Message{
		Field: longString,
	}

	buf := make([]byte, config.MaxMessageSize+varuint64.MaxSize)
	id, size, err := m.Marshal(msg, buf[varuint64.MaxSize:])
	requireT.NoError(err)
	buf = buf[varuint64.MaxSize-varuint64.Size(id):]
	size += varuint64.Put(buf, id)
	buf = buf[:size]

	_, err = c1.SendProton(msg, m)
	requireT.NoError(err)

	msgBytes, _, err := c2.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal(buf, msgBytes)
}

func TestConnectionRawBytesShort(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())

	_, err := c1.SendRawBytes([]byte{0x01, 0x01})
	requireT.NoError(err)

	msg, _, err := c2.ReceiveRawBytes()
	requireT.NoError(err)

	requireT.Equal([]byte{0x01, 0x01}, msg)

	requireT.NoError(c2.sendPing())
	_, err = c2.SendRawBytes([]byte{0x01, 0x02})
	requireT.NoError(err)

	msg, _, err = c1.ReceiveRawBytes()
	requireT.NoError(err)
	requireT.Equal([]byte{0x01, 0x02}, msg)

	requireT.NoError(c1.sendPing())

	for range 1000 {
		_, err := c1.SendRawBytes([]byte{0x01, 0x03})
		requireT.NoError(err)
		_, err = c1.SendRawBytes([]byte{0x01, 0x04})
		requireT.NoError(err)

		msg, _, err = c2.ReceiveRawBytes()
		requireT.NoError(err)
		requireT.Equal([]byte{0x01, 0x03}, msg)
		msg, _, err = c2.ReceiveRawBytes()
		requireT.NoError(err)
		requireT.Equal([]byte{0x01, 0x04}, msg)
	}
}

func TestConnectionRawBytesLong(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + 3),
	}

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())

	_, err := c1.SendRawBytes(append([]byte{0xad, 0x02}, longString+"A"...))
	requireT.NoError(err)

	msg, _, err := c2.ReceiveRawBytes()
	requireT.NoError(err)

	requireT.Equal(append([]byte{0xad, 0x02}, longString+"A"...), msg)

	requireT.NoError(c2.sendPing())
	_, err = c2.SendRawBytes(append([]byte{0xad, 0x02}, longString+"B"...))
	requireT.NoError(err)

	msg, _, err = c1.ReceiveRawBytes()
	requireT.NoError(err)
	requireT.Equal(append([]byte{0xad, 0x02}, longString+"B"...), msg)

	requireT.NoError(c1.sendPing())

	for range 1000 {
		_, err := c1.SendRawBytes(append([]byte{0xad, 0x02}, longString+"C"...))
		requireT.NoError(err)
		_, err = c1.SendRawBytes(append([]byte{0xad, 0x02}, longString+"D"...))
		requireT.NoError(err)

		msg, _, err = c2.ReceiveRawBytes()
		requireT.NoError(err)
		requireT.Equal(append([]byte{0xad, 0x02}, longString+"C"...), msg)
		msg, _, err = c2.ReceiveRawBytes()
		requireT.NoError(err)
		requireT.Equal(append([]byte{0xad, 0x02}, longString+"D"...), msg)
	}
}

func TestConnectionRawBytesSendWrongBuffer(t *testing.T) {
	requireT := require.New(t)

	peer := NewPeerBuffer()

	c := NewConnection(peer, Config{
		MaxMessageSize: 1,
	})
	c.BufferReads()
	c.BufferWrites()

	_, err := c.SendRawBytes([]byte{0x03, 0x00, 0x01})
	requireT.Error(err)
}

func TestConnectionRawBytesSendMaxMessage(t *testing.T) {
	requireT := require.New(t)

	peer := NewPeerBuffer()

	c := NewConnection(peer, Config{
		MaxMessageSize: 1,
	})
	c.BufferReads()
	c.BufferWrites()

	_, err := c.SendRawBytes([]byte{0x02, 0x00, 0x01})
	requireT.NoError(err)
}

func TestConnectionRawBytesSendTooBigMessage(t *testing.T) {
	requireT := require.New(t)

	peer := NewPeerBuffer()

	c := NewConnection(peer, Config{
		MaxMessageSize: 1,
	})
	c.BufferReads()
	c.BufferWrites()

	_, err := c.SendRawBytes([]byte{0x03, 0x00, 0x01, 0x02})
	requireT.Error(err)
}

func TestConnectionRawBytesReceiveMaxMessage(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, Config{
		MaxMessageSize: 4,
	})
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), Config{
		MaxMessageSize: 1,
	})
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	_, err := c1.SendRawBytes([]byte{0x02, 0x01, 0x02})
	requireT.NoError(err)

	msg, _, err := c2.ReceiveRawBytes()
	requireT.NoError(err)
	requireT.NotNil(msg)
}

func TestConnectionRawBytesReceiveTooBigMessage(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, Config{
		MaxMessageSize: 4,
	})
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), Config{
		MaxMessageSize: 1,
	})
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	_, err := c1.SendRawBytes([]byte{0x03, 0x01, 0x02, 0x03})
	requireT.NoError(err)

	msg, _, err := c2.ReceiveRawBytes()
	requireT.Error(err)
	requireT.Nil(msg)
}

func TestConnectionRawBytesMessageSize(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	sizeSent, err := c1.SendRawBytes([]byte{0x02, 0x01, 0x02})
	requireT.NoError(err)
	requireT.EqualValues(1, sizeSent)

	_, sizeReceived, err := c2.ReceiveRawBytes()
	requireT.NoError(err)
	requireT.Equal(sizeSent, sizeReceived)
}

func TestConnectionStream(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c1.BufferReads()
	c1.BufferWrites()
	c2 := NewConnection(peer.OtherPeer(), config)
	c2.BufferReads()
	c2.BufferWrites()

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())

	requireT.NoError(c1.SendStream(bytes.NewBuffer([]byte{0x01, 0x01})))

	msg, _, err := c2.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal([]byte{0x01}, msg)

	requireT.NoError(c2.sendPing())
	requireT.NoError(c2.SendStream(bytes.NewBuffer([]byte{0x01, 0x02})))

	msg, _, err = c1.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal([]byte{0x02}, msg)

	requireT.NoError(c1.sendPing())

	for range 1000 {
		requireT.NoError(c1.SendStream(bytes.NewBuffer([]byte{0x01, 0x03})))
		requireT.NoError(c1.SendStream(bytes.NewBuffer([]byte{0x01, 0x04})))

		msg, _, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte{0x03}, msg)
		msg, _, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte{0x04}, msg)
	}
}

func TestConnectionUnbuffered(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.Run)
	group.Spawn("c2", parallel.Fail, c2.Run)

	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())
	requireT.NoError(c1.sendPing())

	_, err := c1.SendProton(&test.Message{
		Field: "A",
	}, m)
	requireT.NoError(err)

	msg, _, err := c2.ReceiveProton(m)
	requireT.NoError(err)

	requireT.Equal("A", msg.(*test.Message).Field)

	requireT.NoError(c2.sendPing())
	_, err = c2.SendProton(&test.Message{
		Field: "B",
	}, m)
	requireT.NoError(err)

	msg, _, err = c1.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal("B", msg.(*test.Message).Field)

	requireT.NoError(c1.sendPing())

	for range 1000 {
		_, err := c1.SendProton(&test.Message{
			Field: "C",
		}, m)
		requireT.NoError(err)
		_, err = c1.SendProton(&test.Message{
			Field: "D",
		}, m)
		requireT.NoError(err)

		msg, _, err = c2.ReceiveProton(m)
		requireT.NoError(err)
		requireT.Equal("C", msg.(*test.Message).Field)
		msg, _, err = c2.ReceiveProton(m)
		requireT.NoError(err)
		requireT.Equal("D", msg.(*test.Message).Field)
	}
}

func TestConnectionDead(t *testing.T) {
	ctx, cancel := context.WithCancel(qa.NewContext(t))
	cancel()
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	peer := NewPeerBuffer()

	c := NewConnection(peer, config)
	c.BufferReads()
	c.BufferWrites()
	_ = c.Run(ctx)

	_, err := c.SendProton(test.Message{}, test.NewMarshaller())
	requireT.Error(err)
	_, err = c.SendBytes([]byte{0x01, 0x02})
	requireT.Error(err)
	requireT.Error(c.SendStream(bytes.NewBuffer([]byte{0x01, 0x02})))
}

var longString = strings.Repeat("_", 300)
