package resonance

import (
	"context"
	"net"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance/pkg/retry"
)

// RunServer runs server.
func RunServer[M proton.Marshaller](
	ctx context.Context,
	ls net.Listener,
	config Config[M],
	handler func(ctx context.Context, recvCh <-chan any, c *Connection[M]) error,
) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("listener", parallel.Fail, func(ctx context.Context) error {
			for {
				conn, err := ls.Accept()
				if err != nil {
					return errors.WithStack(ctx.Err())
				}

				recvCh := config.ReceiveChannel
				if recvCh == nil {
					recvCh = make(chan any, 500)
				}

				tcpConn := conn.(*net.TCPConn)
				spawn("client-"+tcpConn.RemoteAddr().String(), parallel.Continue, func(ctx context.Context) error {
					c := NewConnection(tcpConn, config, recvCh)

					if handler == nil {
						return c.Run(ctx)
					}

					_ = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
						spawn("connection", parallel.Fail, c.Run)
						spawn("handler", parallel.Exit, func(ctx context.Context) error {
							return handler(ctx, recvCh, c)
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
func RunClient[M proton.Marshaller](
	ctx context.Context,
	addr string,
	config Config[M],
	handler func(ctx context.Context, recvCh <-chan any, c *Connection[M]) error,
) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		retryCtx, retryCancel := context.WithTimeout(ctx, 20*time.Second)
		defer retryCancel()

		var tcpConn *net.TCPConn
		err := retry.Do(retryCtx, time.Second, func() error {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return retry.Retryable(err)
			}
			tcpConn = conn.(*net.TCPConn)
			return nil
		})
		if err != nil {
			return nil
		}

		recvCh := config.ReceiveChannel
		if recvCh == nil {
			recvCh = make(chan any, 500)
		}

		c := NewConnection(tcpConn, config, recvCh)

		if handler == nil {
			return c.Run(ctx)
		}

		return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			spawn("connection", parallel.Fail, c.Run)
			spawn("handler", parallel.Exit, func(ctx context.Context) error {
				return handler(ctx, recvCh, c)
			})
			return nil
		})
	})
}
