package resonance

import (
	"io"

	"github.com/pkg/errors"

	"github.com/outofforest/varuint64"
)

var pingBytes = []byte{0x00}

// Writer writes packets to stream.
type Writer struct {
	maxMessageSize uint64
	w              io.Writer
	sendBuf        []byte
}

// NewWriter creates new writer.
func NewWriter(w io.Writer, maxMessageSize uint64) *Writer {
	return &Writer{
		maxMessageSize: maxMessageSize,
		w:              w,
		sendBuf:        make([]byte, maxMessageSize+2*varuint64.MaxSize),
	}
}

// SendProton sends proton message to the peer.
func (w *Writer) SendProton(msg any, m ProtonMarshaller) (uint64, error) {
	msgID, msgSize, err := m.Marshal(msg, w.sendBuf[2*varuint64.MaxSize:])
	if err != nil {
		return 0, err
	}
	if msgSize > w.maxMessageSize {
		return 0, errors.Errorf("message size %d exceeds maximum %d", msgSize, w.maxMessageSize)
	}

	msgIDSize := varuint64.Size(msgID)
	varuint64.Put(w.sendBuf[2*varuint64.MaxSize-msgIDSize:], msgID)

	totalSize := msgIDSize + msgSize
	bufferStart := 2*varuint64.MaxSize - msgIDSize - varuint64.Size(totalSize)
	totalSize += varuint64.Put(w.sendBuf[bufferStart:], totalSize)

	if _, err := w.w.Write(w.sendBuf[bufferStart : bufferStart+totalSize]); err != nil {
		return 0, errors.WithStack(err)
	}
	return msgSize, nil
}

// SendBytes sends bytes.
func (w *Writer) SendBytes(msg []byte) (uint64, error) {
	msgLen := uint64(len(msg))
	// Varuint is for message ID.
	if msgLen > w.maxMessageSize+varuint64.MaxSize {
		return 0, errors.Errorf("message size exceeds allowed maximum %d", w.maxMessageSize)
	}

	// We find how many bytes there are for message ID to verify if the message itself fits into max size.
	_, n := varuint64.Parse(msg)
	msgSize := msgLen - n
	if msgSize > w.maxMessageSize {
		return 0, errors.Errorf("message size exceeds allowed maximum %d", w.maxMessageSize)
	}

	n = varuint64.Put(w.sendBuf, msgLen)
	if _, err := w.w.Write(w.sendBuf[:n]); err != nil {
		return 0, errors.WithStack(err)
	}
	if _, err := w.w.Write(msg); err != nil {
		return 0, errors.WithStack(err)
	}
	return msgSize, nil
}

// SendRawBytes sends bytes with length prefix already included.
func (w *Writer) SendRawBytes(msg []byte) (uint64, error) {
	msgLen := uint64(len(msg))
	// Varuints are for length and message ID.
	if msgLen > w.maxMessageSize+2*varuint64.MaxSize {
		return 0, errors.Errorf("message size exceeds allowed maximum %d", w.maxMessageSize)
	}

	// We find how many bytes are taken by length.
	size, n1 := varuint64.Parse(msg)
	if msgLen != size+n1 {
		return 0, errors.Errorf("buffer size mismatch, expected: %d, got: %d", size+n1, len(msg))
	}
	if msgLen-n1 > w.maxMessageSize+varuint64.MaxSize {
		return 0, errors.Errorf("message size exceeds allowed maximum %d", w.maxMessageSize)
	}

	// We find how many bytes are taken by message ID.
	_, n2 := varuint64.Parse(msg[n1:])
	msgSize := msgLen - n1 - n2
	if msgSize > w.maxMessageSize {
		return 0, errors.Errorf("message size exceeds allowed maximum %d", w.maxMessageSize)
	}

	if _, err := w.w.Write(msg); err != nil {
		return 0, errors.WithStack(err)
	}
	return msgSize, nil
}

// SendStream sends stream of data taken from the reader.
func (w *Writer) SendStream(r io.Reader) error {
	_, err := io.Copy(w.w, r)
	return errors.WithStack(err)
}

// SendPing sends ping.
func (w *Writer) SendPing() error {
	_, err := w.w.Write(pingBytes)
	return errors.WithStack(err)
}
