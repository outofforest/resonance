package resonance

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
	"github.com/pkg/errors"
)

const (
	pingInterval = 2 * time.Second
	missedPings  = 5
)

var pingBytes = bytes.Repeat([]byte{0x00}, maxVarUInt64Size)

// Config is the configuration of connection.
type Config struct {
	MaxMessageSize uint64
	MarshalFunc    func(m proton.Marshalable, buf []byte) (uint64, uint64, error)
	UnmarshalFunc  func(id uint64, buf []byte) (any, uint64, error)
}

// NewConnection creates new connection.
func NewConnection(peer Peer, config Config) *Connection {
	return &Connection{
		peer:       peer,
		config:     config,
		buf:        NewPeerBuffer(),
		receiveCh:  make(chan any, 1),
		sendCh:     make(chan proton.Marshalable, 1),
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
	sendCh                  chan proton.Marshalable
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
func (c *Connection) Send(msg proton.Marshalable) bool {
	defer recover() //nolint:errcheck // Error doesn't matter here

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
		msg, msgSize, err := c.config.UnmarshalFunc(msgID, receiveBuf[n:size])
		if err != nil {
			return err
		}

		expectedSize := size - n
		if msgSize != expectedSize {
			return errors.Errorf("expected message size %d, got %d", expectedSize, msgSize)
		}

		c.receiveCh <- msg
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

		msgID, msgSize, err := c.config.MarshalFunc(msg, c.sendBuf[2*maxVarUInt64Size:])
		if err != nil {
			return err
		}

		msgIDSize := varUInt64Size(msgID)
		putVarUInt64(c.sendBuf[2*maxVarUInt64Size-msgIDSize:], msgID)

		totalSize := msgIDSize + msgSize
		bufferStart := 2*maxVarUInt64Size - msgIDSize - varUInt64Size(totalSize)
		totalSize += putVarUInt64(c.sendBuf[bufferStart:], totalSize)

		if totalSize < maxVarUInt64Size {
			totalSize = maxVarUInt64Size
		}

		if _, err := c.buf.Write(c.sendBuf[bufferStart : bufferStart+totalSize]); err != nil {
			return err
		}
	}

	return errors.WithStack(ctx.Err())
}
