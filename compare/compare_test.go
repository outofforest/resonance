package compare

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
	"github.com/outofforest/resonance/compare/protobuf"
	"github.com/outofforest/resonance/compare/proton"
)

// To simulate real network conditions use command like this:
// modprobe sch_netem
// tc qdisc add dev lo root netem delay 20ms rate 500Mbit
// tc qdisc del dev lo root

// go test --benchtime=100000x -bench=BenchmarkStreamProton -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

// echo "4194304 4194304 4194304" > /proc/sys/net/ipv4/tcp_rmem
// echo "4194304 4194304 4194304" > /proc/sys/net/ipv4/tcp_wmem
// echo 1 > /proc/sys/net/ipv4/tcp_low_latency

func BenchmarkPingPongProton(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	b.Cleanup(cancel)

	config := resonance.Config[proton.Marshaller]{
		MaxMessageSize: protonTx.Size(),
		Marshaller:     proton.NewMarshaller(100),
	}

	ls, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err)
	defer ls.Close()

	var msg1 *proton.Transaction
	var msg2 *proton.TransactionResponse

	_ = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("server", parallel.Fail, func(ctx context.Context) error {
			c, err := ls.Accept()
			if err != nil {
				return errors.WithStack(err)
			}
			conn := resonance.NewConnection(c.(*net.TCPConn), config)

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("stream", parallel.Fail, func(ctx context.Context) error {
					for range b.N {
						msgAny, _ := conn.Receive()
						msg1 = msgAny.(*proton.Transaction)
						_ = conn.Send(protonResponse)
					}
					<-ctx.Done()
					return nil
				})
				spawn("connection", parallel.Fail, conn.Run)
				return nil
			})
		})
		spawn("client", parallel.Exit, func(ctx context.Context) error {
			var c net.Conn
			for {
				var err error
				c, err = net.Dial("tcp", ls.Addr().String())
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			conn := resonance.NewConnection(c.(*net.TCPConn), config)

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("stream", parallel.Exit, func(ctx context.Context) error {
					b.StartTimer()
					for range b.N {
						_ = conn.Send(protonTx)
						msgAny, _ := conn.Receive()
						msg2 = msgAny.(*proton.TransactionResponse)
					}
					b.StopTimer()
					return nil
				})
				spawn("connection", parallel.Fail, conn.Run)
				return nil
			})
		})
		return nil
	})
	_, _ = fmt.Fprint(io.Discard, msg1)
	_, _ = fmt.Fprint(io.Discard, msg2)
}

func BenchmarkPingPongProtobuf(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	b.Cleanup(cancel)

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err)
	defer l.Close()

	s := grpc.NewServer()
	protobuf.RegisterTransactionsServer(s, &transactionsServer{})

	client, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(b, err)
	txClient := protobuf.NewTransactionsClient(client)

	var resp *protobuf.TransactionResponse

	_ = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("server", parallel.Fail, func(_ context.Context) error {
			return errors.WithStack(s.Serve(l))
		})
		spawn("watchdog", parallel.Fail, func(ctx context.Context) error {
			<-ctx.Done()
			s.GracefulStop()
			return errors.WithStack(ctx.Err())
		})
		spawn("client", parallel.Exit, func(ctx context.Context) error {
			var txStream grpc.BidiStreamingClient[protobuf.Transaction, protobuf.TransactionResponse]
			for {
				var err error
				txStream, err = txClient.SendTransactions(ctx)
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			b.StartTimer()
			for range b.N {
				_ = txStream.Send(protobufMsg)
				resp, _ = txStream.Recv()
				txStream, _ = txClient.SendTransactions(ctx)
			}
			b.StopTimer()
			return nil
		})
		return nil
	})
	_, _ = fmt.Fprint(io.Discard, resp)
}

func BenchmarkStreamProton(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	b.Cleanup(cancel)

	config := resonance.Config[proton.Marshaller]{
		MaxMessageSize: protonTx.Size(),
		Marshaller:     proton.NewMarshaller(100),
	}

	ls, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err)
	defer ls.Close()

	var msg2 *proton.Transaction

	_ = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("server", parallel.Fail, func(ctx context.Context) error {
			c, err := ls.Accept()
			if err != nil {
				return errors.WithStack(err)
			}
			conn := resonance.NewConnection(c.(*net.TCPConn), config)

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("stream", parallel.Fail, func(ctx context.Context) error {
					for range b.N {
						_ = conn.Send(protonTx)
					}
					<-ctx.Done()
					return nil
				})
				spawn("connection", parallel.Fail, conn.Run)
				return nil
			})
		})
		spawn("client", parallel.Exit, func(ctx context.Context) error {
			var c net.Conn
			for {
				var err error
				c, err = net.Dial("tcp", ls.Addr().String())
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			conn := resonance.NewConnection(c.(*net.TCPConn), config)

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("stream", parallel.Exit, func(ctx context.Context) error {
					b.StartTimer()
					for range b.N {
						msgAny, _ := conn.Receive()
						msg2 = msgAny.(*proton.Transaction)
					}
					b.StopTimer()
					return nil
				})
				spawn("connection", parallel.Fail, conn.Run)
				return nil
			})
		})
		return nil
	})
	_, _ = fmt.Fprint(io.Discard, msg2)
}

func BenchmarkStreamProtobuf(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	b.Cleanup(cancel)

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err)
	defer l.Close()

	s := grpc.NewServer()
	protobuf.RegisterTransactionsServer(s, &transactionsServer{})

	client, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(b, err)
	txClient := protobuf.NewTransactionsClient(client)

	var resp *protobuf.Transaction

	_ = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("server", parallel.Fail, func(_ context.Context) error {
			return errors.WithStack(s.Serve(l))
		})
		spawn("watchdog", parallel.Fail, func(ctx context.Context) error {
			<-ctx.Done()
			s.GracefulStop()
			return errors.WithStack(ctx.Err())
		})
		spawn("client", parallel.Exit, func(ctx context.Context) error {
			var txStream grpc.ServerStreamingClient[protobuf.Transaction]
			for {
				var err error
				txStream, err = txClient.StreamTransactions(ctx, &protobuf.SubscribeTransactionsRequest{})
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			b.StartTimer()
			for range b.N {
				resp, _ = txStream.Recv()
			}
			b.StopTimer()
			return nil
		})
		return nil
	})
	_, _ = fmt.Fprint(io.Discard, resp)
}

type transactionsServer struct {
	protobuf.UnimplementedTransactionsServer
}

func (s *transactionsServer) SendTransactions(req protobuf.Transactions_SendTransactionsServer) error {
	return req.Send(protobufResponse)
}

func (s *transactionsServer) StreamTransactions(
	req *protobuf.SubscribeTransactionsRequest,
	stream grpc.ServerStreamingServer[protobuf.Transaction],
) error {
	for {
		if err := stream.Send(protobufMsg); err != nil {
			return err
		}
	}
}

var protonTx = func() *proton.Transaction {
	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)
	return &proton.Transaction{
		Hash:    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		GasUsed: 1024,
		Header: proton.TransactionHeader{
			EdgeNode: "my.edge.node.invalid",
			Signature: proton.Signature{
				Algorithm: "ed25519",
				//nolint:lll
				Signature: "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			},
			Properties: map[string]string{
				"property1": "ccccccccccccccccccccccccccccccccccccccccccccc",
				"property2": "ddddddddddddddddddddddddddddddddddddddddddddd",
			},
		},
		Payload: payload,
	}
}()

var protonResponse = &proton.TransactionResponse{
	Hash:    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	Success: true,
	Message: "Hello world!",
}

var protobufMsg = func() *protobuf.Transaction {
	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)
	return &protobuf.Transaction{
		Hash:    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		GasUsed: 1024,
		Header: &protobuf.TransactionHeader{
			EdgeNode: "my.edge.node.invalid",
			Signature: &protobuf.Signature{
				Algorithm: "ed25519",
				//nolint:lll
				Signature: "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			},
			Properties: map[string]string{
				"property1": "ccccccccccccccccccccccccccccccccccccccccccccc",
				"property2": "ddddddddddddddddddddddddddddddddddddddddddddd",
			},
		},
		Payload: payload,
	}
}()

var protobufResponse = &protobuf.TransactionResponse{
	Hash:    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	Success: true,
	Message: "Hello world!",
}
