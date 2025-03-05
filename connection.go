package resonance

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/mass"
	"github.com/outofforest/parallel"
	"github.com/outofforest/varuint64"
)

const (
	pingInterval = 2 * time.Second
	missedPings  = 5
)

var pingBytes = []byte{0x00}

// ProtonMarshaller is the proton's interface marshalling messages.
type ProtonMarshaller interface {
	Size(msg any) (uint64, error)
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
	bufferSize := config.MaxMessageSize + 3*varuint64.MaxSize
	return &Connection{
		peer:           peer,
		buf:            NewPeerBuffer(),
		sendCh:         make(chan any, 50),
		bufferSize:     bufferSize,
		maxMessageSize: config.MaxMessageSize,
		receiveBuf:     make([]byte, bufferSize),
		massBytes:      mass.New[byte](10 * config.MaxMessageSize),
	}
}

// Connection allows to communicate with the peer.
type Connection struct {
	peer Peer

	buf                     PeerBuffer
	sendLatch, receiveLatch atomic.Bool
	sendCh                  chan any
	bufferSize              uint64
	maxMessageSize          uint64
	receiveBuf              []byte
	readStart, readEnd      uint64
	massBytes               *mass.Mass[byte]
}
type sendProton struct {
	Msg        any
	Marshaller ProtonMarshaller
}

// SendProton sends proton message to the peer.
func (c *Connection) SendProton(msg any, m ProtonMarshaller) (retErr error) {
	msgSize, err := m.Size(msg)
	if err != nil {
		return err
	}

	if msgSize > c.maxMessageSize {
		return errors.Errorf("message size %d exceeds maximum %d", msgSize, c.maxMessageSize)
	}

	defer sendRecover(&retErr)

	c.sendCh <- sendProton{
		Msg:        msg,
		Marshaller: m,
	}
	return nil
}

// ReceiveProton receives proton message from the peer.
func (c *Connection) ReceiveProton(m ProtonUnmarshaller) (any, error) {
	for {
		if c.readEnd == c.readStart {
			c.readStart = 0
			c.readEnd = 0
		}

		buf := c.receiveBuf[c.readStart:]
		sizeReceived := c.readEnd - c.readStart
		for !varuint64.Contains(buf[:sizeReceived]) {
			n, err := c.buf.Read(buf[sizeReceived:varuint64.MaxSize])
			if err != nil {
				return nil, err
			}
			sizeReceived += uint64(n)
		}

		c.receiveLatch.Store(true)

		size, n := varuint64.Parse(buf[:sizeReceived])
		c.readEnd = c.readStart + sizeReceived
		c.readStart += n
		switch {
		case size == 0:
			// ping received
			continue
		case size > c.maxMessageSize+varuint64.MaxSize:
			return nil, errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
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

// SendBytes sends bytes.
func (c *Connection) SendBytes(msg []byte) (retErr error) {
	if msgSize := uint64(len(msg)); msgSize > c.maxMessageSize {
		return errors.Errorf("message size %d exceeds maximum %d", msgSize, c.maxMessageSize)
	}

	defer sendRecover(&retErr)

	c.sendCh <- msg
	return nil
}

// ReceiveBytes receives bytes.
func (c *Connection) ReceiveBytes() ([]byte, error) {
	for {
		if c.readEnd == c.readStart {
			c.readStart = 0
			c.readEnd = 0
		}

		buf := c.receiveBuf[c.readStart:]
		sizeReceived := c.readEnd - c.readStart
		for !varuint64.Contains(buf[:sizeReceived]) {
			n, err := c.buf.Read(buf[sizeReceived:varuint64.MaxSize])
			if err != nil {
				return nil, err
			}
			sizeReceived += uint64(n)
		}

		c.receiveLatch.Store(true)

		size, n := varuint64.Parse(buf[:sizeReceived])
		c.readEnd = c.readStart + sizeReceived
		c.readStart += n
		switch {
		case size == 0:
			// ping received
			continue
		case size > c.maxMessageSize:
			return nil, errors.Errorf("message size %d exceeds allowed maximum %d",
				size, c.bufferSize)
		}

		msgBuf := c.massBytes.NewSlice(size)
		msgReceivedSize := c.readEnd - c.readStart
		if msgReceivedSize > size {
			msgReceivedSize = size
		}
		if msgReceivedSize > 0 {
			copy(msgBuf, c.receiveBuf[c.readStart:c.readStart+msgReceivedSize])
			c.readStart += msgReceivedSize
		}

		for msgReceivedSize < size {
			n, err := c.buf.Read(msgBuf[msgReceivedSize:size])
			if err != nil {
				return nil, err
			}
			msgReceivedSize += uint64(n)
		}

		return msgBuf, nil
	}
}

type rawBytes []byte

// SendRawBytes sends bytes with length prefix already included.
func (c *Connection) SendRawBytes(msg []byte) (retErr error) {
	if msgSize := uint64(len(msg)); msgSize > c.maxMessageSize {
		return errors.Errorf("message size %d exceeds maximum %d", msgSize, c.maxMessageSize)
	}

	defer sendRecover(&retErr)

	c.sendCh <- rawBytes(msg)
	return nil
}

// ReceiveRawBytes receives bytes and returns them with together with length prefix.
func (c *Connection) ReceiveRawBytes() ([]byte, error) {
	for {
		if c.readEnd == c.readStart {
			c.readStart = 0
			c.readEnd = 0
		}

		buf := c.receiveBuf[c.readStart:]
		sizeReceived := c.readEnd - c.readStart
		for !varuint64.Contains(buf[:sizeReceived]) {
			n, err := c.buf.Read(buf[sizeReceived:varuint64.MaxSize])
			if err != nil {
				return nil, err
			}
			sizeReceived += uint64(n)
		}

		c.receiveLatch.Store(true)

		size, n := varuint64.Parse(buf[:sizeReceived])
		c.readEnd = c.readStart + sizeReceived
		switch {
		case size == 0:
			// ping received
			c.readStart += n
			continue
		case size+n > c.maxMessageSize:
			return nil, errors.Errorf("message size %d exceeds allowed maximum %d",
				size, c.bufferSize)
		}

		size += n
		msgBuf := c.massBytes.NewSlice(size)
		msgReceivedSize := c.readEnd - c.readStart
		if msgReceivedSize > size {
			msgReceivedSize = size
		}
		if msgReceivedSize > 0 {
			copy(msgBuf, c.receiveBuf[c.readStart:c.readStart+msgReceivedSize])
			c.readStart += msgReceivedSize
		}

		for msgReceivedSize < size {
			n, err := c.buf.Read(msgBuf[msgReceivedSize:size])
			if err != nil {
				return nil, err
			}
			msgReceivedSize += uint64(n)
		}

		return msgBuf, nil
	}
}

// SendStream sends stream of data taken from the reader.
func (c *Connection) SendStream(r io.Reader) (retErr error) {
	defer sendRecover(&retErr)

	c.sendCh <- r
	return nil
}

// Close closes connection.
func (c *Connection) Close() {
	_ = c.peer.Close()
	_ = c.buf.Close()
}

func (c *Connection) run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("send", parallel.Continue, c.runSend)
		spawn("ping", parallel.Fail, func(ctx context.Context) error {
			defer c.Close()
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
						c.sendPing()
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

func (c *Connection) sendPing() {
	c.sendCh <- struct{}{}
}

func (c *Connection) runSend(ctx context.Context) error {
	sendBuf := make([]byte, c.bufferSize)

	for msg := range c.sendCh {
		c.sendLatch.Store(true)

		switch m := msg.(type) {
		case sendProton:
			msgID, msgSize, err := m.Marshaller.Marshal(m.Msg, sendBuf[2*varuint64.MaxSize:])
			if err != nil {
				return err
			}
			if msgSize > c.maxMessageSize {
				return errors.Errorf("message size %d exceeds maximum %d", msgSize, c.maxMessageSize)
			}

			msgIDSize := varuint64.Size(msgID)
			varuint64.Put(sendBuf[2*varuint64.MaxSize-msgIDSize:], msgID)

			totalSize := msgIDSize + msgSize
			bufferStart := 2*varuint64.MaxSize - msgIDSize - varuint64.Size(totalSize)
			totalSize += varuint64.Put(sendBuf[bufferStart:], totalSize)

			if _, err := c.buf.Write(sendBuf[bufferStart : bufferStart+totalSize]); err != nil {
				return err
			}
		case rawBytes:
			if uint64(len(m)) > c.maxMessageSize {
				return errors.Errorf("message size %d exceeds allowed maximum %d", len(m), c.bufferSize)
			}
			if _, err := c.buf.Write(m); err != nil {
				return err
			}
		case []byte:
			if uint64(len(m)) > c.maxMessageSize {
				return errors.Errorf("message size %d exceeds allowed maximum %d", len(m), c.bufferSize)
			}
			if _, err := c.buf.Write(sendBuf[:varuint64.Put(sendBuf, uint64(len(m)))]); err != nil {
				return err
			}
			if _, err := c.buf.Write(m); err != nil {
				return err
			}
		case io.Reader:
			if _, err := io.Copy(c.buf, m); err != nil {
				return errors.WithStack(err)
			}
		case struct{}:
			// ping requested
			if _, err := c.buf.Write(pingBytes); err != nil {
				return err
			}
		default:
			return errors.New("unknown send request")
		}
	}

	return errors.WithStack(ctx.Err())
}

func sendRecover(err *error) {
	if recover() != nil {
		*err = errors.New("connection is closed")
	}
}
