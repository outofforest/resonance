package proton

import (
	"reflect"
	"unsafe"

	"github.com/outofforest/proton"
	"github.com/outofforest/proton/helpers"
	"github.com/pkg/errors"
)

const (
	id2 uint64 = iota + 1
	id0
)

var _ proton.Marshaller = Marshaller{}

// NewMarshaller creates marshaller.
func NewMarshaller() Marshaller {
	return Marshaller{}
}

// Marshaller marshals and unmarshals messages.
type Marshaller struct {
}

// Messages returns list of the message types supported by marshaller.
func (m Marshaller) Messages() []any {
	return []any {
		Transaction{},
		TransactionResponse{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *Transaction:
		return id2, nil
	case *TransactionResponse:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *Transaction:
		return size2(msg2), nil
	case *TransactionResponse:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Transaction:
		return id2, marshal2(msg2, buf), nil
	case *TransactionResponse:
		return id0, marshal0(msg2, buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Unmarshal unmarshals message.
func (m Marshaller) Unmarshal(id uint64, buf []byte) (retMsg any, retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch id {
	case id2:
		msg := &Transaction{}
		return msg, unmarshal2(msg, buf), nil
	case id0:
		msg := &TransactionResponse{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *Transaction:
		return id2, makePatch2(msg2, msgSrc.(*Transaction), buf), nil
	case *TransactionResponse:
		return id0, makePatch0(msg2, msgSrc.(*TransactionResponse), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Transaction:
		return applyPatch2(msg2, buf), nil
	case *TransactionResponse:
		return applyPatch0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func size0(m *TransactionResponse) uint64 {
	var n uint64 = 18
	{
		// Message

		{
			l := uint64(len(m.Message))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	return n
}

func marshal0(m *TransactionResponse, b []byte) uint64 {
	var o uint64 = 1
	{
		// Hash

		copy(b[o:o+16], unsafe.Slice(&m.Hash[0], 16))
		o += 16
	}
	{
		// Success

		if m.Success {
			b[0] |= 0x01
		} else {
			b[0] &= 0xFE
		}
	}
	{
		// Message

		{
			l := uint64(len(m.Message))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.Message)
			o += l
		}
	}

	return o
}

func unmarshal0(m *TransactionResponse, b []byte) uint64 {
	var o uint64 = 1
	{
		// Hash

		copy(unsafe.Slice(&m.Hash[0], 16), b[o:o+16])
		o += 16
	}
	{
		// Success

		m.Success = b[0]&0x01 != 0
	}
	{
		// Message

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Message = string(b[o:o+l])
				o += l
			}
		}
	}

	return o
}

func makePatch0(m, mSrc *TransactionResponse, b []byte) uint64 {
	var o uint64 = 2
	{
		// Hash

		if reflect.DeepEqual(m.Hash, mSrc.Hash) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			copy(b[o:o+16], unsafe.Slice(&m.Hash[0], 16))
			o += 16
		}
	}
	{
		// Success

		if m.Success == mSrc.Success {
			b[1] &= 0xFE
		} else {
			b[1] |= 0x01
		}
	}
	{
		// Message

		if reflect.DeepEqual(m.Message, mSrc.Message) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			{
				l := uint64(len(m.Message))
				helpers.UInt64Marshal(l, b, &o)
				copy(b[o:o+l], m.Message)
				o += l
			}
		}
	}

	return o
}

func applyPatch0(m *TransactionResponse, b []byte) uint64 {
	var o uint64 = 2
	{
		// Hash

		if b[0]&0x01 != 0 {
			copy(unsafe.Slice(&m.Hash[0], 16), b[o:o+16])
			o += 16
		}
	}
	{
		// Success

		if b[1]&0x01 != 0 {
			m.Success = !m.Success
		}
	}
	{
		// Message

		if b[0]&0x02 != 0 {
			{
				var l uint64
				helpers.UInt64Unmarshal(&l, b, &o)
				if l > 0 {
					m.Message = string(b[o:o+l])
					o += l
				}
			}
		}
	}

	return o
}

func size2(m *Transaction) uint64 {
	var n uint64 = 18
	{
		// Payload

		l := uint64(len(m.Payload))
		helpers.UInt64Size(l, &n)
		n += l
	}
	{
		// GasUsed

		helpers.Int64Size(m.GasUsed, &n)
	}
	{
		// Header

		n += size1(&m.Header)
	}
	return n
}

func marshal2(m *Transaction, b []byte) uint64 {
	var o uint64
	{
		// Hash

		copy(b[o:o+16], unsafe.Slice(&m.Hash[0], 16))
		o += 16
	}
	{
		// Payload

		l := uint64(len(m.Payload))
		helpers.UInt64Marshal(l, b, &o)
		if l > 0 {
			copy(b[o:o+l], unsafe.Slice(&m.Payload[0], l))
			o += l
		}
	}
	{
		// GasUsed

		helpers.Int64Marshal(m.GasUsed, b, &o)
	}
	{
		// Header

		o += marshal1(&m.Header, b[o:])
	}

	return o
}

func unmarshal2(m *Transaction, b []byte) uint64 {
	var o uint64
	{
		// Hash

		copy(unsafe.Slice(&m.Hash[0], 16), b[o:o+16])
		o += 16
	}
	{
		// Payload

		var l uint64
		helpers.UInt64Unmarshal(&l, b, &o)
		if l > 0 {
			m.Payload = make([]uint8, l)
			copy(m.Payload, b[o:o+l])
			o += l
		}
	}
	{
		// GasUsed

		helpers.Int64Unmarshal(&m.GasUsed, b, &o)
	}
	{
		// Header

		o += unmarshal1(&m.Header, b[o:])
	}

	return o
}

func makePatch2(m, mSrc *Transaction, b []byte) uint64 {
	var o uint64 = 1
	{
		// Hash

		if reflect.DeepEqual(m.Hash, mSrc.Hash) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			copy(b[o:o+16], unsafe.Slice(&m.Hash[0], 16))
			o += 16
		}
	}
	{
		// Payload

		if reflect.DeepEqual(m.Payload, mSrc.Payload) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			l := uint64(len(m.Payload))
			helpers.UInt64Marshal(l, b, &o)
			if l > 0 {
				copy(b[o:o+l], unsafe.Slice(&m.Payload[0], l))
				o += l
			}
		}
	}
	{
		// GasUsed

		if reflect.DeepEqual(m.GasUsed, mSrc.GasUsed) {
			b[0] &= 0xFB
		} else {
			b[0] |= 0x04
			helpers.Int64Marshal(m.GasUsed, b, &o)
		}
	}
	{
		// Header

		if reflect.DeepEqual(m.Header, mSrc.Header) {
			b[0] &= 0xF7
		} else {
			b[0] |= 0x08
			o += marshal1(&m.Header, b[o:])
		}
	}

	return o
}

func applyPatch2(m *Transaction, b []byte) uint64 {
	var o uint64 = 1
	{
		// Hash

		if b[0]&0x01 != 0 {
			copy(unsafe.Slice(&m.Hash[0], 16), b[o:o+16])
			o += 16
		}
	}
	{
		// Payload

		if b[0]&0x02 != 0 {
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Payload = make([]uint8, l)
				copy(m.Payload, b[o:o+l])
				o += l
			}
		}
	}
	{
		// GasUsed

		if b[0]&0x04 != 0 {
			helpers.Int64Unmarshal(&m.GasUsed, b, &o)
		}
	}
	{
		// Header

		if b[0]&0x08 != 0 {
			o += unmarshal1(&m.Header, b[o:])
		}
	}

	return o
}

func size1(m *TransactionHeader) uint64 {
	var n uint64 = 2
	{
		// Properties

		l := uint64(len(m.Properties))
		helpers.UInt64Size(l, &n)
		for _, sv1 := range m.Properties {
			n += size3(&sv1)
		}
	}
	{
		// EdgeNode

		{
			l := uint64(len(m.EdgeNode))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	{
		// Signature

		n += size4(&m.Signature)
	}
	return n
}

func marshal1(m *TransactionHeader, b []byte) uint64 {
	var o uint64
	{
		// Properties

		helpers.UInt64Marshal(uint64(len(m.Properties)), b, &o)
		for _, sv1 := range m.Properties {
			o += marshal3(&sv1, b[o:])
		}
	}
	{
		// EdgeNode

		{
			l := uint64(len(m.EdgeNode))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.EdgeNode)
			o += l
		}
	}
	{
		// Signature

		o += marshal4(&m.Signature, b[o:])
	}

	return o
}

func unmarshal1(m *TransactionHeader, b []byte) uint64 {
	var o uint64
	{
		// Properties

		var l uint64
		helpers.UInt64Unmarshal(&l, b, &o)
		if l > 0 {
			m.Properties = make([]Property, l)
			for i1 := range l {
				o += unmarshal3(&m.Properties[i1], b[o:])
			}
		}
	}
	{
		// EdgeNode

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.EdgeNode = string(b[o:o+l])
				o += l
			}
		}
	}
	{
		// Signature

		o += unmarshal4(&m.Signature, b[o:])
	}

	return o
}

func size4(m *Signature) uint64 {
	var n uint64 = 65
	return n
}

func marshal4(m *Signature, b []byte) uint64 {
	var o uint64
	{
		// Algorithm

		b[o] = byte(m.Algorithm)
		o++
	}
	{
		// Signature

		copy(b[o:o+64], unsafe.Slice(&m.Signature[0], 64))
		o += 64
	}

	return o
}

func unmarshal4(m *Signature, b []byte) uint64 {
	var o uint64
	{
		// Algorithm

		m.Algorithm = SignatureAlgorithm(b[o])
		o++
	}
	{
		// Signature

		copy(unsafe.Slice(&m.Signature[0], 64), b[o:o+64])
		o += 64
	}

	return o
}

func size3(m *Property) uint64 {
	var n uint64 = 2
	{
		// Key

		{
			l := uint64(len(m.Key))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	{
		// Value

		{
			l := uint64(len(m.Value))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	return n
}

func marshal3(m *Property, b []byte) uint64 {
	var o uint64
	{
		// Key

		{
			l := uint64(len(m.Key))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.Key)
			o += l
		}
	}
	{
		// Value

		{
			l := uint64(len(m.Value))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.Value)
			o += l
		}
	}

	return o
}

func unmarshal3(m *Property, b []byte) uint64 {
	var o uint64
	{
		// Key

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Key = string(b[o:o+l])
				o += l
			}
		}
	}
	{
		// Value

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Value = string(b[o:o+l])
				o += l
			}
		}
	}

	return o
}
