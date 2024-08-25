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
	pingInterval = time.Second
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
		doneCh:     make(chan struct{}),
	}
}

// Connection allows to communicate with the peer.
type Connection struct {
	peer   Peer
	config Config

	buf                 PeerBuffer
	pingTime            uint64
	receiveCh           chan any
	sendCh              chan Marshalable
	sendBuf, receiveBuf []byte

	doneCh chan struct{}
}

// Run runs the connection.
func (c *Connection) Run(ctx context.Context) error {
	defer close(c.doneCh)

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		c.pingTime = uint64(time.Now().Unix())

		spawn("receive", parallel.Fail, c.runReceivePipeline)
		spawn("send", parallel.Fail, c.runSendPipeline)
		spawn("copy", parallel.Exit, func(ctx context.Context) error {
			return c.buf.Run(ctx, c.peer)
		})

		return nil
	})
}

// Send sends message to the peer.
func (c *Connection) Send(msg Marshalable) bool {
	select {
	case <-c.doneCh:
		return false
	case c.sendCh <- msg:
		return true
	}
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
	defer c.close()
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

		atomic.StoreUint64(&c.pingTime, uint64(time.Now().Unix()))

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
	defer c.close()

	pingSendTicker := time.NewTicker(pingInterval)
	defer pingSendTicker.Stop()

	pingVerifyTicker := time.NewTicker(pingInterval)
	defer pingVerifyTicker.Stop()

	var msg Marshalable

	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-pingVerifyTicker.C:
			pingTime := atomic.LoadUint64(&c.pingTime)
			if uint64(time.Now().Unix())-pingTime > missedPings*uint64(pingInterval/time.Second) {
				return errors.New("connection is dead")
			}
			continue
		case <-pingSendTicker.C:
			if _, err := c.buf.Write(pingBytes); err != nil {
				return err
			}
			continue
		case msg = <-c.sendCh:
			pingSendTicker.Reset(pingInterval)
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
