package resonance

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/parallel"
	"github.com/outofforest/qa"
	"github.com/outofforest/resonance/test"
)

func TestHandlers(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	ca, err := NewCA(nil)
	requireT.NoError(err)

	config := Config{
		MaxMessageSize: 100,
		CA:             ca,
	}

	m := test.NewMarshaller()

	ls, err := net.Listen("tcp", "127.0.0.1:0")
	requireT.NoError(err)

	group.Spawn("server", parallel.Fail, func(ctx context.Context) error {
		return RunServer(ctx, ls, config, func(ctx context.Context, c *Connection) error {
			msg, _, err := c.ReceiveProton(m)
			if err != nil {
				return err
			}

			if msg.(*test.Message).Field != "A" {
				return errors.New("not A")
			}

			_, err = c.SendProton(&test.Message{
				Field: "B",
			}, m)
			if err != nil {
				return err
			}

			return ctx.Err()
		})
	})
	group.Spawn("client", parallel.Exit, func(ctx context.Context) error {
		return RunClient(ctx, ls.Addr().String(), config, func(ctx context.Context, c *Connection) error {
			_, err := c.SendProton(&test.Message{
				Field: "A",
			}, m)
			if err != nil {
				return err
			}

			msg, _, err := c.ReceiveProton(m)
			if err != nil {
				return err
			}

			if msg.(*test.Message).Field != "B" {
				return errors.New("not B")
			}

			return ctx.Err()
		})
	})

	requireT.NoError(group.Wait())
}

func TestHandlersWithInvalidServerCert(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	ca, err := NewCA(nil)
	requireT.NoError(err)

	config := Config{
		MaxMessageSize: 100,
		CA:             ca,
	}

	iCA, err := newCA(ca)
	requireT.NoError(err)
	invalidConfig := config
	invalidConfig.CA = iCA

	m := test.NewMarshaller()

	ls, err := net.Listen("tcp", "127.0.0.1:0")
	requireT.NoError(err)

	group.Spawn("server", parallel.Fail, func(ctx context.Context) error {
		return RunServer(ctx, ls, invalidConfig, func(ctx context.Context, c *Connection) error {
			msg, _, err := c.ReceiveProton(m)
			if err != nil {
				return err
			}

			if msg.(*test.Message).Field != "A" {
				return errors.New("not A")
			}

			_, err = c.SendProton(&test.Message{
				Field: "B",
			}, m)
			if err != nil {
				return err
			}

			return ctx.Err()
		})
	})
	err = RunClient(ctx, ls.Addr().String(), config, func(ctx context.Context, c *Connection) error {
		_, err := c.SendProton(&test.Message{
			Field: "A",
		}, m)
		if err != nil {
			return err
		}

		msg, _, err := c.ReceiveProton(m)
		if err != nil {
			return err
		}

		if msg.(*test.Message).Field != "B" {
			return errors.New("not B")
		}

		return ctx.Err()
	})

	requireT.Error(err)
}

func TestHandlersWithInvalidClientCert(t *testing.T) {
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)
	requireT := require.New(t)

	ca, err := NewCA(nil)
	requireT.NoError(err)

	config := Config{
		MaxMessageSize: 100,
		CA:             ca,
	}

	iCA, err := newCA(ca)
	requireT.NoError(err)
	invalidConfig := config
	invalidConfig.CA = iCA

	m := test.NewMarshaller()

	ls, err := net.Listen("tcp", "127.0.0.1:0")
	requireT.NoError(err)

	group.Spawn("server", parallel.Fail, func(ctx context.Context) error {
		return RunServer(ctx, ls, config, func(ctx context.Context, c *Connection) error {
			msg, _, err := c.ReceiveProton(m)
			if err != nil {
				return err
			}

			if msg.(*test.Message).Field != "A" {
				return errors.New("not A")
			}

			_, err = c.SendProton(&test.Message{
				Field: "B",
			}, m)
			if err != nil {
				return err
			}

			return ctx.Err()
		})
	})
	err = RunClient(ctx, ls.Addr().String(), invalidConfig, func(ctx context.Context, c *Connection) error {
		_, err := c.SendProton(&test.Message{
			Field: "A",
		}, m)
		if err != nil {
			return err
		}

		msg, _, err := c.ReceiveProton(m)
		if err != nil {
			return err
		}

		if msg.(*test.Message).Field != "B" {
			return errors.New("not B")
		}

		return ctx.Err()
	})

	requireT.Error(err)
}

type invalidCA struct {
	ca  CASource
	iCA CASource
}

func newCA(ca CASource) (*invalidCA, error) {
	iCA, err := NewCA(nil)
	if err != nil {
		return nil, err
	}
	return &invalidCA{
		ca:  ca,
		iCA: iCA,
	}, nil
}

func (ca invalidCA) Generate() (*tls.Config, error) {
	tlsConfig, err := ca.ca.Generate()
	if err != nil {
		return nil, err
	}
	iConfig, err := ca.iCA.Generate()
	if err != nil {
		return nil, err
	}

	tlsConfig.Certificates = iConfig.Certificates
	return tlsConfig, nil
}
