package resonance

const maxVarUInt64Size = 9

func varUInt64Size(v uint64) uint64 {
	// For last byte last bit is used for data, not for continuation flag.
	// That's why we may fit 64-bit number in 9 bytes, not 10.

	switch {
	case v <= 0x7F:
		return 1
	case v <= 0x3FFF:
		return 2
	case v <= 0x1FFFFF:
		return 3
	case v <= 0xFFFFFFF:
		return 4
	case v <= 0x7FFFFFFFF:
		return 5
	case v <= 0x3FFFFFFFFFF:
		return 6
	case v <= 0x1FFFFFFFFFFFF:
		return 7
	case v <= 0xFFFFFFFFFFFFFF:
		return 8
	default:
		return 9
	}
}

func putVarUInt64(b []byte, v uint64) uint64 {
	// For last byte last bit is used for data, not for continuation flag.
	// That's why we may fit 64-bit number in 9 bytes, not 10.

	switch {
	case v <= 0x7F:
		b[0] = byte(v)
		return 1
	case v <= 0x3FFF:
		b[0] = byte(v) | 0x80
		v >>= 7
		b[1] = byte(v)
		return 2
	case v <= 0x1FFFFF:
		b[0] = byte(v) | 0x80
		v >>= 7
		b[1] = byte(v) | 0x80
		v >>= 7
		b[2] = byte(v)
		return 3
	case v <= 0xFFFFFFF:
		b[0] = byte(v) | 0x80
		v >>= 7
		b[1] = byte(v) | 0x80
		v >>= 7
		b[2] = byte(v) | 0x80
		v >>= 7
		b[3] = byte(v)
		return 4
	case v <= 0x7FFFFFFFF:
		b[0] = byte(v) | 0x80
		v >>= 7
		b[1] = byte(v) | 0x80
		v >>= 7
		b[2] = byte(v) | 0x80
		v >>= 7
		b[3] = byte(v) | 0x80
		v >>= 7
		b[4] = byte(v)
		return 5
	case v <= 0x3FFFFFFFFFF:
		b[0] = byte(v) | 0x80
		v >>= 7
		b[1] = byte(v) | 0x80
		v >>= 7
		b[2] = byte(v) | 0x80
		v >>= 7
		b[3] = byte(v) | 0x80
		v >>= 7
		b[4] = byte(v) | 0x80
		v >>= 7
		b[5] = byte(v)
		return 6
	case v <= 0x1FFFFFFFFFFFF:
		b[0] = byte(v) | 0x80
		v >>= 7
		b[1] = byte(v) | 0x80
		v >>= 7
		b[2] = byte(v) | 0x80
		v >>= 7
		b[3] = byte(v) | 0x80
		v >>= 7
		b[4] = byte(v) | 0x80
		v >>= 7
		b[5] = byte(v) | 0x80
		v >>= 7
		b[6] = byte(v)
		return 7
	case v <= 0xFFFFFFFFFFFFFF:
		b[0] = byte(v) | 0x80
		v >>= 7
		b[1] = byte(v) | 0x80
		v >>= 7
		b[2] = byte(v) | 0x80
		v >>= 7
		b[3] = byte(v) | 0x80
		v >>= 7
		b[4] = byte(v) | 0x80
		v >>= 7
		b[5] = byte(v) | 0x80
		v >>= 7
		b[6] = byte(v) | 0x80
		v >>= 7
		b[7] = byte(v)
		return 8
	default:
		b[0] = byte(v) | 0x80
		v >>= 7
		b[1] = byte(v) | 0x80
		v >>= 7
		b[2] = byte(v) | 0x80
		v >>= 7
		b[3] = byte(v) | 0x80
		v >>= 7
		b[4] = byte(v) | 0x80
		v >>= 7
		b[5] = byte(v) | 0x80
		v >>= 7
		b[6] = byte(v) | 0x80
		v >>= 7
		b[7] = byte(v) | 0x80
		v >>= 7
		b[8] = byte(v)
		return 9
	}
}

func varUInt64(b []byte) (uint64, uint64) {
	v := uint64(b[0] & 0x7F)
	if b[0]&0x80 == 0 {
		return v, 1
	}

	v |= uint64(b[1]&0x7F) << 7
	if b[1]&0x80 == 0 {
		return v, 2
	}

	v |= uint64(b[2]&0x7F) << 14
	if b[2]&0x80 == 0 {
		return v, 3
	}

	v |= uint64(b[3]&0x7F) << 21
	if b[3]&0x80 == 0 {
		return v, 4
	}

	v |= uint64(b[4]&0x7F) << 28
	if b[4]&0x80 == 0 {
		return v, 5
	}

	v |= uint64(b[5]&0x7F) << 35
	if b[5]&0x80 == 0 {
		return v, 6
	}

	v |= uint64(b[6]&0x7F) << 42
	if b[6]&0x80 == 0 {
		return v, 7
	}

	v |= uint64(b[7]&0x7F) << 49
	if b[7]&0x80 == 0 {
		return v, 8
	}

	v |= uint64(b[8]) << 56
	return v, 9
}
