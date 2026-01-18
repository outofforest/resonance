package test

import (
	"reflect"

	"github.com/outofforest/proton"
	"github.com/outofforest/proton/helpers"
	"github.com/pkg/errors"
)

const (
	id0 uint64 = iota + 1
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
		Message{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *Message:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *Message:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Message:
		return id0, marshal0(msg2, buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Unmarshal unmarshals message.
func (m Marshaller) Unmarshal(id uint64, buf []byte) (retMsg any, retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch id {
	case id0:
		msg := &Message{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

// IsPatchNeeded checks if non-empty patch exists.
func (m Marshaller) IsPatchNeeded(msgDst, msgSrc any) (bool, error) {
	switch msg2 := msgDst.(type) {
	case *Message:
		return isPatchNeeded0(msg2, msgSrc.(*Message)), nil
	default:
		return false, errors.Errorf("unknown message type %T", msgDst)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *Message:
		return id0, makePatch0(msg2, msgSrc.(*Message), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverApplyPatch(&retErr)

	switch msg2 := msg.(type) {
	case *Message:
		return applyPatch0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func size0(m *Message) uint64 {
	var n uint64 = 1
	{
		// Field

		{
			l := uint64(len(m.Field))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	return n
}

func marshal0(m *Message, b []byte) uint64 {
	var o uint64
	{
		// Field

		{
			l := uint64(len(m.Field))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.Field)
			o += l
		}
	}

	return o
}

func unmarshal0(m *Message, b []byte) uint64 {
	var o uint64
	{
		// Field

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Field = string(b[o:o+l])
				o += l
			}
		}
	}

	return o
}

func isPatchNeeded0(m, mSrc *Message) bool {
	{
		// Field

		if !reflect.DeepEqual(m.Field, mSrc.Field) {
			return true
		}

	}

	return false
}

func makePatch0(m, mSrc *Message, b []byte) uint64 {
	var o uint64 = 1
	{
		// Field

		if reflect.DeepEqual(m.Field, mSrc.Field) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			{
				l := uint64(len(m.Field))
				helpers.UInt64Marshal(l, b, &o)
				copy(b[o:o+l], m.Field)
				o += l
			}
		}
	}

	return o
}

func applyPatch0(m *Message, b []byte) uint64 {
	var o uint64 = 1
	{
		// Field

		if b[0]&0x01 != 0 {
			{
				var l uint64
				helpers.UInt64Unmarshal(&l, b, &o)
				if l > 0 {
					m.Field = string(b[o:o+l])
					o += l
				}
			}
		}
	}

	return o
}
