package resonance

import (
	"context"
	"io"
	"sync"
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

// Connection allows to communicate with the peer.
type Connection struct {
	peer Peer

	buf                     PeerBuffer
	reader                  io.Reader
	sendLatch, receiveLatch atomic.Bool
	bufferSize              uint64
	maxMessageSize          uint64
	receiveBuf              []byte
	readStart, readEnd      uint64
	massBytes               *mass.Mass[byte]
	bufferReadsCh           chan struct{}
	bufferWritesCh          chan struct{}

	mu      sync.Mutex
	writer  io.Writer
	sendBuf []byte
}

// NewConnection creates new connection.
func NewConnection(peer Peer, config Config) *Connection {
	// one varint for length, other one for message ID.
	bufferSize := config.MaxMessageSize + 2*varuint64.MaxSize
	buf := NewPeerBuffer()
	return &Connection{
		peer:           peer,
		buf:            buf,
		reader:         peer,
		writer:         peer,
		bufferSize:     bufferSize,
		maxMessageSize: config.MaxMessageSize,
		receiveBuf:     make([]byte, bufferSize),
		massBytes:      mass.New[byte](10 * config.MaxMessageSize),
		bufferReadsCh:  make(chan struct{}, 1),
		bufferWritesCh: make(chan struct{}, 1),
		sendBuf:        make([]byte, bufferSize),
	}
}

// BufferReads turns on read buffer.
func (c *Connection) BufferReads() {
	c.reader = c.buf

	select {
	case c.bufferReadsCh <- struct{}{}:
	default:
	}
}

// BufferWrites turns on write buffer.
func (c *Connection) BufferWrites() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writer = c.buf
	select {
	case c.bufferWritesCh <- struct{}{}:
	default:
	}
}

// SendProton sends proton message to the peer.
func (c *Connection) SendProton(msg any, m ProtonMarshaller) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sendLatch.Store(true)

	msgID, msgSize, err := m.Marshal(msg, c.sendBuf[2*varuint64.MaxSize:])
	if err != nil {
		return err
	}
	if msgSize > c.maxMessageSize {
		return errors.Errorf("message size %d exceeds maximum %d", msgSize, c.maxMessageSize)
	}

	msgIDSize := varuint64.Size(msgID)
	varuint64.Put(c.sendBuf[2*varuint64.MaxSize-msgIDSize:], msgID)

	totalSize := msgIDSize + msgSize
	bufferStart := 2*varuint64.MaxSize - msgIDSize - varuint64.Size(totalSize)
	totalSize += varuint64.Put(c.sendBuf[bufferStart:], totalSize)

	_, err = c.writer.Write(c.sendBuf[bufferStart : bufferStart+totalSize])
	return errors.WithStack(err)
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
			n, err := c.reader.Read(buf[sizeReceived:varuint64.MaxSize])
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
		// Varuint here is for message ID. For now, we assume that message ID took the max size of var int.
		// Later on there is another check, doing final verification of message size.
		case size > c.maxMessageSize+varuint64.MaxSize:
			return nil, errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
		}

		buf = c.receiveBuf[c.readStart:]

		msgReceivedSize := c.readEnd - c.readStart
		for msgReceivedSize < size {
			n, err := c.reader.Read(buf[msgReceivedSize:size])
			if err != nil {
				return nil, err
			}
			msgReceivedSize += uint64(n)
		}
		c.readEnd = c.readStart + msgReceivedSize
		c.readStart += size

		msgID, n := varuint64.Parse(buf[:size])

		// Here we do the final check of allowed message size.
		if size-n > c.maxMessageSize {
			return nil, errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
		}

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
func (c *Connection) SendBytes(msg []byte) error {
	msgLen := uint64(len(msg))
	// Varuint is for message ID.
	if msgLen > c.maxMessageSize+varuint64.MaxSize {
		return errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
	}

	// We find how many bytes there are for message ID to verify if the message itself fits into max size.
	_, n := varuint64.Parse(msg)
	if msgLen-n > c.maxMessageSize {
		return errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.sendLatch.Store(true)

	n = varuint64.Put(c.sendBuf, msgLen)
	if _, err := c.writer.Write(c.sendBuf[:n]); err != nil {
		return errors.WithStack(err)
	}
	_, err := c.writer.Write(msg)
	return errors.WithStack(err)
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
			n, err := c.reader.Read(buf[sizeReceived:varuint64.MaxSize])
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
		// Varuint here is for message ID. For now, we assume that message ID took the max size of var int.
		// Later on there is another check, doing final verification of message size.
		case size > c.maxMessageSize+varuint64.MaxSize:
			return nil, errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
		}

		msgBuf := c.massBytes.NewSlice(size)
		msgReceivedSize := min(c.readEnd-c.readStart, size)
		if msgReceivedSize > 0 {
			copy(msgBuf, c.receiveBuf[c.readStart:c.readStart+msgReceivedSize])
			c.readStart += msgReceivedSize
		}

		for msgReceivedSize < size {
			n, err := c.reader.Read(msgBuf[msgReceivedSize:size])
			if err != nil {
				return nil, err
			}
			msgReceivedSize += uint64(n)
		}

		// Here we do the final check of allowed message size.
		_, n = varuint64.Parse(msgBuf[:msgReceivedSize])
		if size-n > c.maxMessageSize {
			return nil, errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
		}

		return msgBuf, nil
	}
}

// SendRawBytes sends bytes with length prefix already included.
func (c *Connection) SendRawBytes(msg []byte) error {
	msgLen := uint64(len(msg))
	// Varuints are for length and message ID.
	if msgLen > c.maxMessageSize+2*varuint64.MaxSize {
		return errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
	}

	// We find how many bytes are taken by length.
	_, n1 := varuint64.Parse(msg)
	if msgLen-n1 > c.maxMessageSize+varuint64.MaxSize {
		return errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
	}

	// We find how many bytes are taken by message ID.
	_, n2 := varuint64.Parse(msg[n1:])
	if msgLen-n1-n2 > c.maxMessageSize {
		return errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.sendLatch.Store(true)

	_, err := c.writer.Write(msg)
	return errors.WithStack(err)
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
			n, err := c.reader.Read(buf[sizeReceived:varuint64.MaxSize])
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
		// Varuint here is for message ID. For now, we assume that message ID took the max size of var int.
		// Later on there is another check, doing final verification of message size.
		case size > c.maxMessageSize+varuint64.MaxSize:
			return nil, errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
		}

		size += n
		msgBuf := c.massBytes.NewSlice(size)
		msgReceivedSize := min(c.readEnd-c.readStart, size)
		if msgReceivedSize > 0 {
			copy(msgBuf, c.receiveBuf[c.readStart:c.readStart+msgReceivedSize])
			c.readStart += msgReceivedSize
		}

		for msgReceivedSize < size {
			n, err := c.reader.Read(msgBuf[msgReceivedSize:size])
			if err != nil {
				return nil, err
			}
			msgReceivedSize += uint64(n)
		}

		// Here we do the final check of allowed message size.
		_, n = varuint64.Parse(msgBuf[n:msgReceivedSize])
		if size-n > c.maxMessageSize {
			return nil, errors.Errorf("message size exceeds allowed maximum %d", c.maxMessageSize)
		}

		return msgBuf, nil
	}
}

// SendStream sends stream of data taken from the reader.
func (c *Connection) SendStream(r io.Reader) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sendLatch.Store(true)

	_, err := io.Copy(c.writer, r)
	return errors.WithStack(err)
}

// Close closes connection.
func (c *Connection) Close() {
	_ = c.peer.Close()
	_ = c.buf.Close()
}

// Run runs connection's goroutines.
func (c *Connection) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("ping", parallel.Fail, func(ctx context.Context) error {
			defer c.Close()

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
						if err := c.sendPing(); err != nil {
							return err
						}
					}
				case <-missedTicker.C:
					if latch := c.receiveLatch.Swap(false); !latch {
						return errors.New("connection is dead")
					}
				}
			}
		})
		spawn("readBuffer", parallel.Exit, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return errors.WithStack(ctx.Err())
			case <-c.bufferReadsCh:
			}
			return c.buf.RunReader(ctx, c.peer)
		})
		spawn("writeBuffer", parallel.Exit, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return errors.WithStack(ctx.Err())
			case <-c.bufferWritesCh:
			}
			return c.buf.RunWriter(ctx, c.peer)
		})

		return nil
	})
}

func (c *Connection) sendPing() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sendLatch.Store(true)

	_, err := c.writer.Write(pingBytes)
	return errors.WithStack(err)
}
