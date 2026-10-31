package resonance

import (
	"io"

	"github.com/pkg/errors"

	"github.com/outofforest/mass"
	"github.com/outofforest/varuint64"
)

// Reader reads packets from stream.
type Reader struct {
	r                  io.Reader
	maxMessageSize     uint64
	receiveBuf         []byte
	readStart, readEnd uint64
	massBytes          *mass.Mass[byte]
}

// NewReader creates new reader.
func NewReader(r io.Reader, maxMessageSize uint64) *Reader {
	// one varint for length, other one for message ID.
	bufferSize := maxMessageSize + 2*varuint64.MaxSize
	return &Reader{
		r:              r,
		maxMessageSize: maxMessageSize,
		receiveBuf:     make([]byte, bufferSize),
		massBytes:      mass.New[byte](10 * bufferSize),
	}
}

// ReceiveProton receives proton message from the peer.
func (r *Reader) ReceiveProton(m ProtonUnmarshaller) (any, uint64, error) {
	for {
		if r.readEnd == r.readStart {
			r.readStart = 0
			r.readEnd = 0
		}

		buf := r.receiveBuf[r.readStart:]
		sizeReceived := r.readEnd - r.readStart
		for !varuint64.Contains(buf[:sizeReceived]) {
			n, err := r.r.Read(buf[sizeReceived:varuint64.MaxSize])
			if err != nil {
				return nil, 0, err
			}
			sizeReceived += uint64(n)
		}

		size, n := varuint64.Parse(buf[:sizeReceived])
		r.readEnd = r.readStart + sizeReceived
		r.readStart += n
		switch {
		case size == 0:
			// ping received
			continue
		// Varuint here is for message ID. For now, we assume that message ID took the max size of var int.
		// Later on there is another check, doing final verification of message size.
		case size > r.maxMessageSize+varuint64.MaxSize:
			return nil, 0, errors.Errorf("message size exceeds allowed maximum %d", r.maxMessageSize)
		}

		buf = r.receiveBuf[r.readStart:]

		msgReceivedSize := r.readEnd - r.readStart
		for msgReceivedSize < size {
			n, err := r.r.Read(buf[msgReceivedSize:size])
			if err != nil {
				return nil, 0, err
			}
			msgReceivedSize += uint64(n)
		}
		r.readEnd = r.readStart + msgReceivedSize
		r.readStart += size

		msgID, n := varuint64.Parse(buf[:size])
		expectedSize := size - n

		// Here we do the final check of allowed message size.
		if expectedSize > r.maxMessageSize {
			return nil, 0, errors.Errorf("message size exceeds allowed maximum %d", r.maxMessageSize)
		}

		msg, msgSize, err := m.Unmarshal(msgID, buf[n:size])
		if err != nil {
			return nil, 0, err
		}

		if msgSize != expectedSize {
			return nil, 0, errors.Errorf("expected message size %d, got %d", expectedSize, msgSize)
		}

		return msg, msgSize, nil
	}
}

// ReceiveBytes receives bytes.
func (r *Reader) ReceiveBytes() ([]byte, uint64, error) {
	for {
		if r.readEnd == r.readStart {
			r.readStart = 0
			r.readEnd = 0
		}

		buf := r.receiveBuf[r.readStart:]
		sizeReceived := r.readEnd - r.readStart
		for !varuint64.Contains(buf[:sizeReceived]) {
			n, err := r.r.Read(buf[sizeReceived:varuint64.MaxSize])
			if err != nil {
				return nil, 0, err
			}
			sizeReceived += uint64(n)
		}

		size, n := varuint64.Parse(buf[:sizeReceived])
		r.readEnd = r.readStart + sizeReceived
		r.readStart += n
		switch {
		case size == 0:
			// ping received
			continue
		// Varuint here is for message ID. For now, we assume that message ID took the max size of var int.
		// Later on there is another check, doing final verification of message size.
		case size > r.maxMessageSize+varuint64.MaxSize:
			return nil, 0, errors.Errorf("message size exceeds allowed maximum %d", r.maxMessageSize)
		}

		msgBuf := r.massBytes.NewSlice(size)
		msgReceivedSize := min(r.readEnd-r.readStart, size)
		if msgReceivedSize > 0 {
			copy(msgBuf, r.receiveBuf[r.readStart:r.readStart+msgReceivedSize])
			r.readStart += msgReceivedSize
		}

		for msgReceivedSize < size {
			n, err := r.r.Read(msgBuf[msgReceivedSize:size])
			if err != nil {
				return nil, 0, err
			}
			msgReceivedSize += uint64(n)
		}

		// Here we do the final check of allowed message size.
		_, n = varuint64.Parse(msgBuf[:size])
		msgSize := size - n
		if msgSize > r.maxMessageSize {
			return nil, 0, errors.Errorf("message size exceeds allowed maximum %d", r.maxMessageSize)
		}

		return msgBuf, msgSize, nil
	}
}

// ReceiveRawBytes receives bytes and returns them with together with length prefix.
func (r *Reader) ReceiveRawBytes() ([]byte, uint64, error) {
	for {
		if r.readEnd == r.readStart {
			r.readStart = 0
			r.readEnd = 0
		}

		buf := r.receiveBuf[r.readStart:]
		sizeReceived := r.readEnd - r.readStart
		for !varuint64.Contains(buf[:sizeReceived]) {
			n, err := r.r.Read(buf[sizeReceived:varuint64.MaxSize])
			if err != nil {
				return nil, 0, err
			}
			sizeReceived += uint64(n)
		}

		size, n1 := varuint64.Parse(buf[:sizeReceived])
		r.readEnd = r.readStart + sizeReceived
		switch {
		case size == 0:
			// ping received
			r.readStart += n1
			continue
		// Varuint here is for message ID. For now, we assume that message ID took the max size of var int.
		// Later on there is another check, doing final verification of message size.
		case size > r.maxMessageSize+varuint64.MaxSize:
			return nil, 0, errors.Errorf("message size exceeds allowed maximum %d 1", r.maxMessageSize)
		}

		size += n1
		msgBuf := r.massBytes.NewSlice(size)
		msgReceivedSize := min(r.readEnd-r.readStart, size)
		if msgReceivedSize > 0 {
			copy(msgBuf, r.receiveBuf[r.readStart:r.readStart+msgReceivedSize])
			r.readStart += msgReceivedSize
		}

		for msgReceivedSize < size {
			n, err := r.r.Read(msgBuf[msgReceivedSize:size])
			if err != nil {
				return nil, 0, err
			}
			msgReceivedSize += uint64(n)
		}

		// Here we do the final check of allowed message size.
		_, n2 := varuint64.Parse(msgBuf[n1:size])
		msgSize := size - n1 - n2
		if msgSize > r.maxMessageSize {
			return nil, 0, errors.Errorf("message size exceeds allowed maximum %d 2", r.maxMessageSize)
		}

		return msgBuf, msgSize, nil
	}
}
