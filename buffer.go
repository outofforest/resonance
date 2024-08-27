package resonance

import (
	"context"
	"io"

	"github.com/pkg/errors"

	"github.com/outofforest/parallel"
	"github.com/outofforest/spin"
)

// Peer represents the connected peer.
type Peer io.ReadWriteCloser

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
func (b PeerBuffer) Read(buf []byte) (int, error) {
	return b.read.Read(buf)
}

// WriteTo writes data to the outgoing stream.
func (b PeerBuffer) WriteTo(w io.Writer) (int64, error) {
	return b.read.WriteTo(w)
}

// Write writes data to the write buffer.
func (b PeerBuffer) Write(buf []byte) (int, error) {
	return b.write.Write(buf)
}

// ReadFrom reads data from the incoming stream.
func (b PeerBuffer) ReadFrom(r io.Reader) (int64, error) {
	return b.write.ReadFrom(r)
}

// OtherPeer returns the corresponding buffer for the other peer in the connection.
func (b PeerBuffer) OtherPeer() PeerBuffer {
	return PeerBuffer{
		read:  b.write,
		write: b.read,
	}
}

// Close closes the streams.
func (b PeerBuffer) Close() error {
	err1 := b.read.Close()
	err2 := b.write.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// Run runs the goroutines responsible for copying data between this buffer and the external one.
func (b PeerBuffer) Run(ctx context.Context, peer Peer) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("peer", parallel.Exit, func(ctx context.Context) error {
			_, err := b.OtherPeer().ReadFrom(peer)
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		})
		spawn("me", parallel.Exit, func(ctx context.Context) error {
			_, err := b.OtherPeer().WriteTo(peer)
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		})
		return nil
	})
}
