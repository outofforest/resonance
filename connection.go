package resonance

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/parallel"
	"github.com/outofforest/varuint64"
)

const (
	pingInterval = 2 * time.Second
	missedPings  = 5
)

var pingBytes = []byte{0x00}

type sendProton struct {
	Msg        any
	Marshaller ProtonMarshaller
}

// ProtonMarshaller is the proton's interface marshalling messages.
type ProtonMarshaller interface {
	Marshal(msg any, buf []byte) (uint64, uint64, error)
}

// ProtonUnmarshaller is the proton's interface unmarshalling messages.
type ProtonUnmarshaller interface {
	Unmarshal(id uint64, buf []byte) (any, uint64, error)
}

// Config is the configuration of connection.
type Config struct {
	MaxMessageSize uint64
}

// NewConnection creates new connection.
func NewConnection(peer Peer, config Config) *Connection {
	bufferSize := config.MaxMessageSize + 2*varuint64.MaxSize
	return &Connection{
		peer:       peer,
		buf:        NewPeerBuffer(),
		sendCh:     make(chan any, 50),
		bufferSize: bufferSize,
		receiveBuf: make([]byte, bufferSize),
	}
}

// Connection allows to communicate with the peer.
type Connection struct {
	peer Peer

	buf                     PeerBuffer
	sendLatch, receiveLatch atomic.Bool
	sendCh                  chan any
	bufferSize              uint64
	receiveBuf              []byte
	readStart, readEnd      uint64
}

// SendProton sends proton message to the peer.
func (c *Connection) SendProton(msg any, m ProtonMarshaller) bool {
	defer func() {
		_ = recover()
	}()

	c.sendCh <- sendProton{
		Msg:        msg,
		Marshaller: m,
	}
	return true
}

// ReceiveProton receives proton message from the peer.
func (c *Connection) ReceiveProton(m ProtonUnmarshaller) (any, error) {
	for {
		if c.readEnd == c.readStart {
			c.readStart = 0
			c.readEnd = 0
		} else if c.readStart > c.bufferSize-varuint64.MaxSize {
			copy(c.receiveBuf, c.receiveBuf[c.readStart:c.readEnd])
			c.readEnd -= c.readStart
			c.readStart = 0
		}

		buf := c.receiveBuf[c.readStart:]
		sizeReceived := c.readEnd - c.readStart
		for {
			n, err := c.buf.Read(buf[sizeReceived:varuint64.MaxSize])
			if err != nil {
				return nil, err
			}
			sizeReceived += uint64(n)
			if varuint64.Contains(buf[:sizeReceived]) {
				break
			}
		}

		c.receiveLatch.Store(true)

		size, n := varuint64.Parse(buf[:sizeReceived])
		c.readEnd = c.readStart + sizeReceived
		c.readStart += n
		switch {
		case size == 0:
			// ping received
			continue
		case size > c.bufferSize-varuint64.MaxSize:
			return nil, errors.Errorf("message size %d exceeds allowed maximum %d",
				size, c.bufferSize)
		}
		if c.readStart == c.readEnd {
			c.readStart = 0
			c.readEnd = 0
		} else if c.readStart > c.bufferSize-size {
			copy(c.receiveBuf, c.receiveBuf[c.readStart:c.readEnd])
			c.readEnd -= c.readStart
			c.readStart = 0
		}

		buf = c.receiveBuf[c.readStart:]

		msgReceivedSize := c.readEnd - c.readStart
		for msgReceivedSize < size {
			n, err := c.buf.Read(buf[msgReceivedSize:size])
			if err != nil {
				return nil, err
			}
			msgReceivedSize += uint64(n)
		}
		c.readEnd = c.readStart + msgReceivedSize
		c.readStart += size

		msgID, n := varuint64.Parse(buf[:size])
		msg, msgSize, err := m.Unmarshal(msgID, buf[n:size])
		if err != nil {
			return nil, err
		}

		expectedSize := size - n
		if msgSize != expectedSize {
			return nil, errors.Errorf("expected message size %d, got %d", expectedSize, msgSize)
		}

		return msg, nil
	}
}

func (c *Connection) run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("send", parallel.Fail, c.runSendPipeline)
		spawn("ping", parallel.Fail, func(ctx context.Context) error {
			defer c.close()
			defer close(c.sendCh)

			pingTicker := time.NewTicker(pingInterval)
			defer pingTicker.Stop()

			missedTicker := time.NewTicker(missedPings * pingInterval)
			defer missedTicker.Stop()

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case <-pingTicker.C:
					if latch := c.sendLatch.Swap(false); !latch && len(c.sendCh) == 0 {
						c.sendCh <- nil
					}
				case <-missedTicker.C:
					if latch := c.receiveLatch.Swap(false); !latch {
						return errors.New("connection is dead")
					}
				}
			}
		})
		spawn("copy", parallel.Exit, func(ctx context.Context) error {
			return c.buf.Run(ctx, c.peer)
		})

		return nil
	})
}

func (c *Connection) close() {
	_ = c.peer.Close()
	_ = c.buf.Close()
}

func (c *Connection) runSendPipeline(ctx context.Context) error {
	sendBuf := make([]byte, c.bufferSize)

	for msg := range c.sendCh {
		c.sendLatch.Store(true)

		if msg == nil {
			// ping requested
			if _, err := c.buf.Write(pingBytes); err != nil {
				return err
			}
			continue
		}

		switch m := msg.(type) {
		case sendProton:
			msgID, msgSize, err := m.Marshaller.Marshal(m.Msg, sendBuf[2*varuint64.MaxSize:])
			if err != nil {
				return err
			}

			msgIDSize := varuint64.Size(msgID)
			varuint64.Put(sendBuf[2*varuint64.MaxSize-msgIDSize:], msgID)

			totalSize := msgIDSize + msgSize
			bufferStart := 2*varuint64.MaxSize - msgIDSize - varuint64.Size(totalSize)
			totalSize += varuint64.Put(sendBuf[bufferStart:], totalSize)

			if _, err := c.buf.Write(sendBuf[bufferStart : bufferStart+totalSize]); err != nil {
				return err
			}
		default:
			return errors.New("unknown send request")
		}
	}

	return errors.WithStack(ctx.Err())
}
