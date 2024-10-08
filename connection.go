package resonance

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
)

const (
	pingInterval             = 2 * time.Second
	missedPings              = 5
	receiveBufSizeMultiplier = 100
)

var pingBytes = bytes.Repeat([]byte{0x00}, maxVarUInt64Size)

// Config is the configuration of connection.
type Config[M proton.Marshaller] struct {
	MaxMessageSize    uint64
	MarshallerFactory func(capacity uint64) M
	ReceiveChannel    chan any
}

// NewConnection creates new connection.
func NewConnection[M proton.Marshaller](peer Peer, config Config[M], recvCh chan any) *Connection[M] {
	bufferSize := config.MaxMessageSize + 2*maxVarUInt64Size
	if recvCh == nil {
		recvCh = make(chan any, 500)
	}
	return &Connection[M]{
		peer:       peer,
		config:     config,
		marshaller: config.MarshallerFactory(1000),
		buf:        NewPeerBuffer(),
		recvCh:     recvCh,
		sendCh:     make(chan proton.Marshallable, 500),
		bufferSize: bufferSize,
		sendBuf:    make([]byte, bufferSize),
	}
}

// Connection allows to communicate with the peer.
type Connection[M proton.Marshaller] struct {
	peer       Peer
	config     Config[M]
	marshaller M

	buf                     PeerBuffer
	sendLatch, receiveLatch atomic.Bool
	recvCh                  chan any
	sendCh                  chan proton.Marshallable
	bufferSize              uint64
	sendBuf, receiveBuf     []byte
}

// Run runs the connection.
func (c *Connection[M]) Run(ctx context.Context) error {
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
func (c *Connection[M]) Send(msg proton.Marshallable) bool {
	defer recover() //nolint:errcheck // Error doesn't matter here

	c.sendCh <- msg
	return true
}

// SendIfPossible sends a message if there is space available in the queue.
func (c *Connection[M]) SendIfPossible(msg proton.Marshallable) (bool, bool) {
	defer recover() //nolint:errcheck // Error doesn't matter here

	select {
	case c.sendCh <- msg:
		return true, true
	default:
		return false, true
	}
}

func (c *Connection[M]) close() {
	_ = c.peer.Close()
	_ = c.buf.Close()

	if c.config.ReceiveChannel == nil {
		for range c.recvCh {
		}
	}
}

func (c *Connection[M]) runReceivePipeline(ctx context.Context) error {
	if c.config.ReceiveChannel == nil {
		defer close(c.recvCh)
	}

	for {
		if uint64(len(c.receiveBuf)) < c.bufferSize {
			c.receiveBuf = make([]byte, receiveBufSizeMultiplier*c.bufferSize)
		}

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

		msgReceivedSize := sizeReceived - n
		for msgReceivedSize < size {
			n, err := c.buf.Read(receiveBuf[msgReceivedSize:size])
			if errors.Is(err, io.EOF) {
				return errors.WithStack(ctx.Err())
			}
			if err != nil {
				return err
			}
			msgReceivedSize += uint64(n)
		}

		msgID, n := varUInt64(receiveBuf[:msgReceivedSize])
		msg, msgSize, err := c.marshaller.Unmarshal(msgID, receiveBuf[n:size])
		if err != nil {
			return err
		}

		expectedSize := size - n
		if msgSize != expectedSize {
			return errors.Errorf("expected message size %d, got %d", expectedSize, msgSize)
		}

		c.receiveBuf = receiveBuf[msgReceivedSize:]
		c.recvCh <- msg
	}
}

func (c *Connection[M]) runSendPipeline(ctx context.Context) error {
	for msg := range c.sendCh {
		c.sendLatch.Store(true)

		if msg == nil {
			// ping requested
			if _, err := c.buf.Write(pingBytes); err != nil {
				return err
			}
			continue
		}

		msgID, msgSize, err := c.marshaller.Marshal(msg, c.sendBuf[2*maxVarUInt64Size:])
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
