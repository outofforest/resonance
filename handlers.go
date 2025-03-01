package resonance

import (
	"context"
	"net"
	"time"

	"github.com/pkg/errors"

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
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("listener", parallel.Fail, func(ctx context.Context) error {
			for {
				conn, err := ls.Accept()
				if err != nil {
					return errors.WithStack(ctx.Err())
				}

				tcpConn := conn.(*net.TCPConn)
				spawn("client", parallel.Continue, func(ctx context.Context) error {
					c := NewConnection(tcpConn, config)

					if handler == nil {
						return c.run(ctx)
					}

					_ = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
						spawn("connection", parallel.Fail, c.run)
						spawn("handler", parallel.Exit, func(ctx context.Context) error {
							return handler(ctx, c)
						})
						return nil
					})
					return nil
				})
			}
		})
		spawn("watchdog", parallel.Fail, func(ctx context.Context) error {
			defer ls.Close()

			<-ctx.Done()
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
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		retryCtx, retryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer retryCancel()

		var tcpConn *net.TCPConn
		err := retry.Do(retryCtx, time.Second, func() error {
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				return retry.Retryable(errors.WithStack(err))
			}
			tcpConn = conn.(*net.TCPConn)
			return nil
		})
		if err != nil {
			return err
		}

		c := NewConnection(tcpConn, config)

		if handler == nil {
			return c.run(ctx)
		}

		return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			spawn("connection", parallel.Fail, c.run)
			spawn("handler", parallel.Exit, func(ctx context.Context) error {
				return handler(ctx, c)
			})
			return nil
		})
	})
}
