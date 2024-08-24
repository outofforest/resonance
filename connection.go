package resonance

import (
	"bytes"
	"io"

	"github.com/pkg/errors"
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
func NewConnection(peer io.ReadWriter, config Config) *Connection {
	return &Connection{
		peer:       peer,
		config:     config,
		sendBuf:    make([]byte, config.MaxMessageSize+2*maxVarUInt64Size),
		receiveBuf: make([]byte, config.MaxMessageSize+2*maxVarUInt64Size),
	}
}

// Connection allows to communicate with the peer.
type Connection struct {
	peer   io.ReadWriter
	config Config

	sendBuf, receiveBuf []byte
}

// Send sends message to the peer.
func (c *Connection) Send(msg Marshalable) error {
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

	if _, err := c.peer.Write(c.sendBuf[bufferStart : bufferStart+totalSize]); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// Receive receives message from the peer.
func (c *Connection) Receive() (any, error) {
	var sizeReceived uint64
	for sizeReceived < maxVarUInt64Size {
		n, err := c.peer.Read(c.receiveBuf[sizeReceived:maxVarUInt64Size])
		if err != nil {
			return nil, errors.WithStack(err)
		}
		sizeReceived += uint64(n)
	}

	size, n := varUInt64(c.receiveBuf[:maxVarUInt64Size])
	receiveBuf := c.receiveBuf[n:]

	switch {
	case size == 0:
		// ping received
		return nil, nil
	case size > c.config.MaxMessageSize+maxVarUInt64Size:
		return nil, errors.Errorf("message size %d exceeds allowed maximum %d",
			size, c.config.MaxMessageSize)
	}

	totalReceived := sizeReceived - n
	for totalReceived < size {
		n, err := c.peer.Read(receiveBuf[totalReceived:size])
		if err != nil {
			return nil, errors.WithStack(err)
		}
		totalReceived += uint64(n)
	}

	msgID, n := varUInt64(receiveBuf[:totalReceived])
	msg, err := c.config.IDToMsgFunc(msgID)
	if err != nil {
		return nil, err
	}

	msgUnmarshaled, msgSize, err := c.unmarshalMessage(msg, receiveBuf[n:size])
	if err != nil {
		return nil, err
	}

	expectedSize := size - n
	if msgSize != expectedSize {
		return nil, errors.Errorf("expected message size %d, got %d", expectedSize, msgSize)
	}

	return msgUnmarshaled, nil
}

// Ping sends the 0x00 byte to ensure connection works.
func (c *Connection) Ping() error {
	_, err := c.peer.Write(pingBytes)
	return errors.WithStack(err)
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
