package resonance

import (
	"context"
	"crypto/tls"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/parallel"
)

const (
	pingInterval = 2 * time.Second
	missedPings  = 5
)

// ProtonMarshaller is the proton's interface marshalling messages.
type ProtonMarshaller interface {
	Size(msg any) (uint64, error)
	Marshal(msg any, buf []byte) (uint64, uint64, error)
}

// ProtonUnmarshaller is the proton's interface unmarshalling messages.
type ProtonUnmarshaller interface {
	Unmarshal(id uint64, buf []byte) (any, uint64, error)
}

// CASource represents CA TLS config generator.
type CASource interface {
	Generate() (*tls.Config, error)
}

// Config is the configuration of connection.
type Config struct {
	MaxMessageSize uint64
	CA             CASource
}

// Connection allows to communicate with the peer.
type Connection struct {
	maxMessageSize          uint64
	peer                    Peer
	buf                     PeerBuffer
	reader                  *Reader
	sendLatch, receiveLatch atomic.Bool

	bufferReadsCh  chan struct{}
	bufferWritesCh chan struct{}

	mu     sync.Mutex
	writer *Writer
}

// NewConnection creates new connection.
func NewConnection(peer Peer, config Config) *Connection {
	c := &Connection{
		peer:           peer,
		buf:            NewPeerBuffer(),
		maxMessageSize: config.MaxMessageSize,
		bufferReadsCh:  make(chan struct{}, 1),
		bufferWritesCh: make(chan struct{}, 1),
	}
	c.reader = NewReader(peer, config.MaxMessageSize)
	c.writer = NewWriter(peer, config.MaxMessageSize)
	return c
}

// BufferReads turns on read buffer.
func (c *Connection) BufferReads() {
	c.reader = NewReader(c.buf, c.maxMessageSize)

	select {
	case c.bufferReadsCh <- struct{}{}:
	default:
	}
}

// BufferWrites turns on write buffer.
func (c *Connection) BufferWrites() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writer = NewWriter(c.buf, c.maxMessageSize)
	select {
	case c.bufferWritesCh <- struct{}{}:
	default:
	}
}

// SendProton sends proton message to the peer.
func (c *Connection) SendProton(msg any, m ProtonMarshaller) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, err := c.writer.SendProton(msg, m)
	if err != nil {
		return 0, err
	}

	c.sendLatch.Store(true)

	return n, nil
}

// ReceiveProton receives proton message from the peer.
func (c *Connection) ReceiveProton(m ProtonUnmarshaller) (any, uint64, error) {
	msg, n, err := c.reader.ReceiveProton(m)
	if err != nil {
		return nil, 0, err
	}

	c.receiveLatch.Store(true)

	return msg, n, nil
}

// SendBytes sends bytes.
func (c *Connection) SendBytes(msg []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, err := c.writer.SendBytes(msg)
	if err != nil {
		return 0, err
	}

	c.sendLatch.Store(true)

	return n, nil
}

// ReceiveBytes receives bytes.
func (c *Connection) ReceiveBytes() ([]byte, uint64, error) {
	msg, n, err := c.reader.ReceiveBytes()
	if err != nil {
		return nil, 0, err
	}

	c.receiveLatch.Store(true)

	return msg, n, nil
}

// SendRawBytes sends bytes with length prefix already included.
func (c *Connection) SendRawBytes(msg []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, err := c.writer.SendRawBytes(msg)
	if err != nil {
		return 0, err
	}

	c.sendLatch.Store(true)

	return n, nil
}

// ReceiveRawBytes receives bytes and returns them with together with length prefix.
func (c *Connection) ReceiveRawBytes() ([]byte, uint64, error) {
	msg, n, err := c.reader.ReceiveRawBytes()
	if err != nil {
		return nil, 0, err
	}

	c.receiveLatch.Store(true)

	return msg, n, nil
}

// SendStream sends stream of data taken from the reader.
func (c *Connection) SendStream(r io.Reader) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.writer.SendStream(r); err != nil {
		return err
	}

	c.sendLatch.Store(true)

	return nil
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

	if err := c.writer.SendPing(); err != nil {
		return err
	}

	c.sendLatch.Store(true)

	return nil
}
