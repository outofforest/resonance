package resonance

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance/pkg/retry"
)

// RunServer runs server.
func RunServer(
	ctx context.Context,
	ls net.Listener,
	config Config,
	handler func(ctx context.Context, c *Connection) error,
) error {
	tlsConfig, err := config.CA.Generate()
	if err != nil {
		return err
	}
	tlsLS := tls.NewListener(ls, tlsConfig)
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("listener", parallel.Fail, func(ctx context.Context) error {
			for {
				conn, err := tlsLS.Accept()
				if err != nil {
					return errors.WithStack(err)
				}
				if ctx.Err() != nil {
					_ = conn.Close()
					return errors.WithStack(ctx.Err())
				}

				spawn("client", parallel.Continue, func(ctx context.Context) error {
					c := NewConnection(conn, config)

					if handler == nil {
						return c.Run(ctx)
					}

					err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
						spawn("connection", parallel.Fail, c.Run)
						spawn("handler", parallel.Exit, func(ctx context.Context) error {
							return handler(ctx, c)
						})
						return nil
					})
					if err != nil {
						logger.Get(ctx).Warn("Connection failed.", zap.Error(err))
					}
					return nil
				})
			}
		})
		spawn("watchdog", parallel.Fail, func(ctx context.Context) error {
			<-ctx.Done()
			conn, err := net.Dial("tcp", ls.Addr().String())
			if err == nil {
				_ = conn.Close()
			}
			return errors.WithStack(ctx.Err())
		})

		return nil
	})
}

// RunClient runs client.
func RunClient(
	ctx context.Context,
	addr string,
	config Config,
	handler func(ctx context.Context, c *Connection) error,
) error {
	tlsConfig, err := config.CA.Generate()
	if err != nil {
		return err
	}

	dialer := &net.Dialer{
		Timeout: time.Second,
	}

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		retryCtx, retryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer retryCancel()

		var tlsConn *tls.Conn
		err := retry.Do(retryCtx, time.Second, func() error {
			var err error
			tlsConn, err = tls.DialWithDialer(dialer, "tcp", addr, tlsConfig)
			if err != nil {
				return retry.Retryable(errors.WithStack(err))
			}
			return nil
		})
		if err != nil {
			return err
		}

		c := NewConnection(tlsConn, config)

		if handler == nil {
			return c.Run(ctx)
		}

		return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			spawn("connection", parallel.Fail, c.Run)
			spawn("handler", parallel.Exit, func(ctx context.Context) error {
				return handler(ctx, c)
			})
			return nil
		})
	})
}
