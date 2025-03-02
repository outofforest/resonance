package resonance

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance/pkg/sim"
	"github.com/outofforest/resonance/test"
	"github.com/outofforest/varuint64"
)

func TestConnectionProtonShort(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	c1.sendPing()
	c1.sendPing()
	c1.sendPing()
	c1.sendPing()
	c1.sendPing()

	requireT.True(c1.SendProton(&test.Message{
		Field: "A",
	}, m))

	msg, err := c2.ReceiveProton(m)
	requireT.NoError(err)

	requireT.Equal("A", msg.(*test.Message).Field)

	c2.sendPing()
	requireT.True(c2.SendProton(&test.Message{
		Field: "B",
	}, m))

	msg, err = c1.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal("B", msg.(*test.Message).Field)

	c1.sendPing()

	for range 1000 {
		requireT.True(c1.SendProton(&test.Message{
			Field: "C",
		}, m))
		requireT.True(c1.SendProton(&test.Message{
			Field: "D",
		}, m))

		msg, err = c2.ReceiveProton(m)
		requireT.NoError(err)
		requireT.Equal("C", msg.(*test.Message).Field)
		msg, err = c2.ReceiveProton(m)
		requireT.NoError(err)
		requireT.Equal("D", msg.(*test.Message).Field)
	}
}

func TestConnectionProtonLong(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + 3),
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	c1.sendPing()
	c1.sendPing()
	c1.sendPing()
	c1.sendPing()
	c1.sendPing()

	requireT.True(c1.SendProton(&test.Message{
		Field: longString + "A",
	}, m))

	msg, err := c2.ReceiveProton(m)
	requireT.NoError(err)

	requireT.Equal(longString+"A", msg.(*test.Message).Field)

	c2.sendPing()
	requireT.True(c2.SendProton(&test.Message{
		Field: longString + "B",
	}, m))

	msg, err = c1.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal(longString+"B", msg.(*test.Message).Field)

	c1.sendPing()

	for range 1000 {
		requireT.True(c1.SendProton(&test.Message{
			Field: longString + "C",
		}, m))
		requireT.True(c1.SendProton(&test.Message{
			Field: longString + "D",
		}, m))

		msg, err = c2.ReceiveProton(m)
		requireT.NoError(err)
		requireT.Equal(longString+"C", msg.(*test.Message).Field)
		msg, err = c2.ReceiveProton(m)
		requireT.NoError(err)
		requireT.Equal(longString+"D", msg.(*test.Message).Field)
	}
}

func TestConnectionProtonReceiveTooBigMessage(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, Config{
		MaxMessageSize: uint64(len(longString) + 10),
	})
	c2 := NewConnection(peer.OtherPeer(), Config{
		MaxMessageSize: 2,
	})

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	requireT.True(c1.SendProton(&test.Message{
		Field: longString,
	}, m))

	msg, err := c2.ReceiveProton(m)
	requireT.Error(err)
	requireT.Nil(msg)
}

func TestConnectionProtonReceiveInvalidMessage1(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + len(longString)/2),
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	buf := make([]byte, config.MaxMessageSize+varuint64.MaxSize)
	id, size, err := m.Marshal(&test.Message{
		Field: longString,
	}, buf[varuint64.MaxSize:])
	requireT.NoError(err)
	buf = buf[varuint64.MaxSize-varuint64.Size(id):]
	size += varuint64.Put(buf, id)
	buf[1]++ // Set invalid length of string inside message.

	requireT.True(c1.SendBytes(buf[:size]))

	msg, err := c2.ReceiveProton(m)
	requireT.Error(err)
	requireT.Nil(msg)
}

func TestConnectionProtonReceiveInvalidMessage2(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + len(longString)/2),
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	buf := make([]byte, config.MaxMessageSize+varuint64.MaxSize)
	id, size, err := m.Marshal(&test.Message{
		Field: longString,
	}, buf[varuint64.MaxSize:])
	requireT.NoError(err)
	buf = buf[varuint64.MaxSize-varuint64.Size(id):]
	size += varuint64.Put(buf, id)
	buf[2] = 0x80 - 1 // Set invalid length of string inside message.

	requireT.True(c1.SendBytes(buf[:size]))

	msg, err := c2.ReceiveProton(m)
	requireT.Error(err)
	requireT.Nil(msg)
}

func TestConnectionBytesShort(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	c1.sendPing()
	c1.sendPing()
	c1.sendPing()
	c1.sendPing()
	c1.sendPing()

	requireT.True(c1.SendBytes([]byte{0x01}))

	msg, err := c2.ReceiveBytes()
	requireT.NoError(err)

	requireT.Equal([]byte{0x01}, msg)

	c2.sendPing()
	requireT.True(c2.SendBytes([]byte{0x02}))

	msg, err = c1.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal([]byte{0x02}, msg)

	c1.sendPing()

	for range 1000 {
		requireT.True(c1.SendBytes([]byte{0x03}))
		requireT.True(c1.SendBytes([]byte{0x04}))

		msg, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte{0x03}, msg)
		msg, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte{0x04}, msg)
	}
}

func TestConnectionBytesLong(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + 1),
	}

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	c1.sendPing()
	c1.sendPing()
	c1.sendPing()
	c1.sendPing()
	c1.sendPing()

	requireT.True(c1.SendBytes([]byte(longString + "A")))

	msg, err := c2.ReceiveBytes()
	requireT.NoError(err)

	requireT.Equal([]byte(longString+"A"), msg)

	c2.sendPing()
	requireT.True(c2.SendBytes([]byte(longString + "B")))

	msg, err = c1.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal([]byte(longString+"B"), msg)

	c1.sendPing()

	for range 1000 {
		requireT.True(c1.SendBytes([]byte(longString + "C")))
		requireT.True(c1.SendBytes([]byte(longString + "D")))

		msg, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte(longString+"C"), msg)
		msg, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte(longString+"D"), msg)
	}
}

func TestConnectionBytesReceiveTooBigMessage(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, Config{
		MaxMessageSize: uint64(len(longString) + 10),
	})
	c2 := NewConnection(peer.OtherPeer(), Config{
		MaxMessageSize: 2,
	})

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	requireT.True(c1.SendBytes([]byte{0x01, 0x02, 0x03}))

	msg, err := c2.ReceiveBytes()
	requireT.Error(err)
	requireT.Nil(msg)
}

func TestConnectionSendBytesReceiveProton(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + len(longString)/2),
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	buf := make([]byte, config.MaxMessageSize+varuint64.MaxSize)
	id, size, err := m.Marshal(&test.Message{
		Field: longString,
	}, buf[varuint64.MaxSize:])
	requireT.NoError(err)
	buf = buf[varuint64.MaxSize-varuint64.Size(id):]
	size += varuint64.Put(buf, id)

	requireT.True(c1.SendBytes(buf[:size]))

	msg, err := c2.ReceiveProton(m)
	requireT.NoError(err)
	requireT.Equal(&test.Message{
		Field: longString,
	}, msg)
}

func TestConnectionSendProtonReceiveBytes(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: uint64(len(longString) + len(longString)/2),
	}

	m := test.NewMarshaller()

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	msg := &test.Message{
		Field: longString,
	}

	buf := make([]byte, config.MaxMessageSize+varuint64.MaxSize)
	id, size, err := m.Marshal(msg, buf[varuint64.MaxSize:])
	requireT.NoError(err)
	buf = buf[varuint64.MaxSize-varuint64.Size(id):]
	size += varuint64.Put(buf, id)
	buf = buf[:size]

	requireT.True(c1.SendProton(msg, m))

	msgBytes, err := c2.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal(buf, msgBytes)
}

func TestConnectionStream(t *testing.T) {
	ctx := sim.NewContext(t)
	group := sim.NewParallel(ctx, t)
	requireT := require.New(t)

	config := Config{
		MaxMessageSize: 100,
	}

	peer := NewPeerBuffer()

	c1 := NewConnection(peer, config)
	c2 := NewConnection(peer.OtherPeer(), config)

	group.Spawn("c1", parallel.Fail, c1.run)
	group.Spawn("c2", parallel.Fail, c2.run)

	c1.sendPing()
	c1.sendPing()
	c1.sendPing()
	c1.sendPing()
	c1.sendPing()

	requireT.True(c1.SendStream(bytes.NewBuffer([]byte{0x01, 0x01})))

	msg, err := c2.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal([]byte{0x01}, msg)

	c2.sendPing()
	requireT.True(c2.SendStream(bytes.NewBuffer([]byte{0x01, 0x02})))

	msg, err = c1.ReceiveBytes()
	requireT.NoError(err)
	requireT.Equal([]byte{0x02}, msg)

	c1.sendPing()

	for range 1000 {
		requireT.True(c1.SendStream(bytes.NewBuffer([]byte{0x01, 0x03})))
		requireT.True(c1.SendStream(bytes.NewBuffer([]byte{0x01, 0x04})))

		msg, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte{0x03}, msg)
		msg, err = c2.ReceiveBytes()
		requireT.NoError(err)
		requireT.Equal([]byte{0x04}, msg)
	}
}

var longString = strings.Repeat("_", 300)
