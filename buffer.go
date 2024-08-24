package resonance

import "github.com/outofforest/spin"

// NewPeerBuffer returns new peer buffer.
func NewPeerBuffer() PeerBuffer {
	return PeerBuffer{
		read:  spin.New(),
		write: spin.New(),
	}
}

// PeerBuffer simulates the network connection from the peer's view.
type PeerBuffer struct {
	read  *spin.Buffer
	write *spin.Buffer
}

// Read reads data from the read buffer.
func (b PeerBuffer) Read(buf []byte) (n int, err error) {
	return b.read.Read(buf)
}

// ReadByte reads byte from the read buffer.
func (b PeerBuffer) ReadByte() (byte, error) {
	return b.read.ReadByte()
}

// Write writes data to the write buffer.
func (b PeerBuffer) Write(buf []byte) (n int, err error) {
	return b.write.Write(buf)
}

// OtherPeer returns the corresponding buffer for the other peer in the connection.
func (b PeerBuffer) OtherPeer() PeerBuffer {
	return PeerBuffer{
		read:  b.write,
		write: b.read,
	}
}
