package resonance

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/outofforest/parallel"
	"github.com/pkg/errors"
)

const (
	pingInterval = 2 * time.Second
	missedPings  = 5
)

var pingBytes = bytes.Repeat([]byte{0x00}, maxVarUInt64Size)

// Marshalable is the interface for messages.
type Marshalable interface {
	Marshal(buf []byte) uint64
}

type unmarshalable interface {
	Unmarshal(buf []byte) uint64
}

// Config is the configuration of connection.
type Config struct {
	MaxMessageSize uint64
	MsgToIDFunc    func(m any) (uint64, error)
	IDToMsgFunc    func(id uint64) (any, error)
}

// NewConnection creates new connection.
func NewConnection(peer Peer, config Config) *Connection {
	return &Connection{
		peer:       peer,
		config:     config,
		buf:        NewPeerBuffer(),
		receiveCh:  make(chan any),
		sendCh:     make(chan Marshalable),
		sendBuf:    make([]byte, config.MaxMessageSize+2*maxVarUInt64Size),
		receiveBuf: make([]byte, config.MaxMessageSize+2*maxVarUInt64Size),
	}
}

// Connection allows to communicate with the peer.
type Connection struct {
	peer   Peer
	config Config

	buf                     PeerBuffer
	sendLatch, receiveLatch atomic.Bool
	receiveCh               chan any
	sendCh                  chan Marshalable
	sendBuf, receiveBuf     []byte
}

// Run runs the connection.
func (c *Connection) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receive", parallel.Fail, c.runReceivePipeline)
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
					if latch := c.sendLatch.Swap(false); !latch {
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

// Send sends message to the peer.
func (c *Connection) Send(msg Marshalable) bool {
	defer recover()

	c.sendCh <- msg
	return true
}

// Receive receives message from the peer.
func (c *Connection) Receive() (any, bool) {
	msg, ok := <-c.receiveCh
	return msg, ok
}

func (c *Connection) close() {
	_ = c.peer.Close()
	_ = c.buf.Close()
	for range c.receiveCh {
	}
}

func (c *Connection) runReceivePipeline(ctx context.Context) error {
	defer close(c.receiveCh)

	for {
		var sizeReceived uint64
		for sizeReceived < maxVarUInt64Size {
			n, err := c.buf.Read(c.receiveBuf[sizeReceived:maxVarUInt64Size])
			if errors.Is(err, io.EOF) {
				return errors.WithStack(ctx.Err())
			}
			if err != nil {
				return err
			}
			sizeReceived += uint64(n)
		}

		c.receiveLatch.Store(true)

		size, n := varUInt64(c.receiveBuf[:maxVarUInt64Size])
		receiveBuf := c.receiveBuf[n:]

		switch {
		case size == 0:
			// ping received
			continue
		case size > c.config.MaxMessageSize+maxVarUInt64Size:
			return errors.Errorf("message size %d exceeds allowed maximum %d",
				size, c.config.MaxMessageSize)
		}

		totalReceived := sizeReceived - n
		for totalReceived < size {
			n, err := c.buf.Read(receiveBuf[totalReceived:size])
			if errors.Is(err, io.EOF) {
				return errors.WithStack(ctx.Err())
			}
			if err != nil {
				return err
			}
			totalReceived += uint64(n)
		}

		msgID, n := varUInt64(receiveBuf[:totalReceived])
		msg, err := c.config.IDToMsgFunc(msgID)
		if err != nil {
			return err
		}

		msgUnmarshaled, msgSize, err := c.unmarshalMessage(msg, receiveBuf[n:size])
		if err != nil {
			return err
		}

		expectedSize := size - n
		if msgSize != expectedSize {
			return errors.Errorf("expected message size %d, got %d", expectedSize, msgSize)
		}

		c.receiveCh <- msgUnmarshaled
	}
}

func (c *Connection) runSendPipeline(ctx context.Context) error {
	for msg := range c.sendCh {
		c.sendLatch.Store(true)

		if msg == nil {
			// ping requested
			if _, err := c.buf.Write(pingBytes); err != nil {
				return err
			}
			continue
		}

		msgID, err := c.config.MsgToIDFunc(msg)
		if err != nil {
			return err
		}

		n := putVarUInt64(c.sendBuf[maxVarUInt64Size:], msgID)
		msgSize, err := c.marshalMessage(msg, c.sendBuf[maxVarUInt64Size+n:])
		if err != nil {
			return err
		}

		totalSize := n + msgSize
		bufferStart := maxVarUInt64Size - varUInt64Size(totalSize)
		n = putVarUInt64(c.sendBuf[bufferStart:], totalSize)
		totalSize += n

		if totalSize < maxVarUInt64Size {
			totalSize = maxVarUInt64Size
		}

		if _, err := c.buf.Write(c.sendBuf[bufferStart : bufferStart+totalSize]); err != nil {
			return err
		}
	}

	return errors.WithStack(ctx.Err())
}

func (c *Connection) marshalMessage(msg Marshalable, buf []byte) (size uint64, err error) {
	defer func() {
		if res := recover(); res != nil {
			size = 0
			err = errors.Errorf("marshaling message failed: %s", res)
		}
	}()
	return msg.Marshal(buf), nil
}

func (c *Connection) unmarshalMessage(msg any, buf []byte) (msgUnmarshaled any, size uint64, err error) {
	defer func() {
		if res := recover(); res != nil {
			msgUnmarshaled = nil
			size = 0
			err = errors.Errorf("marshaling message failed: %s", res)
		}
	}()

	size = msg.(unmarshalable).Unmarshal(buf)
	return msg, size, nil
}
