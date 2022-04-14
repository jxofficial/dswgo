package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	api "github.com/jxofficial/proglog/api/v1"
	"github.com/jxofficial/proglog/internal/config"
	"github.com/jxofficial/proglog/internal/log"
)

func TestServer(t *testing.T) {
	tt := map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds":                                    testProduceConsume,
		"consume past boundary returns nil ConsumeResponse and error with expected status code": testConsumePastBoundary,
		"consume stream returns records in stream":                                              testProduceConsumeStream,
	}

	for scenario, fn := range tt {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{
		Value: []byte("hello world"),
		// the log will assign the correct offset to the record in segment.Append
	}
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	// to be pedantic, produce.Offset should be treated as the source of truth for the record's offset.

	// the `want` Record instance is not the same as the `consume.Record` instance.
	// because `Segment.Append` assigns the correct offset to `want.Offset`, and then
	// appends the marshalled bytes of `want` to the store.
	// the store bytes will be unmarshalled into a new instance in `Segment.Read`.

	// so here, we are essentially checking that `segment.Append` assigns the correct value of offset to `want`.
	require.Equal(t, want.Offset, consume.Record.Offset)
}
func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		}})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	records := []*api.Record{{
		Value: []byte("first message"),
	}, {
		Value: []byte("second message"),
	}}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for o, r := range records {
			err = stream.Send(&api.ProduceRequest{Record: r})
			require.NoError(t, err)
			resp, err := stream.Recv()
			require.NoError(t, err)
			if resp.Offset != uint64(o) {
				t.Fatalf("got offset: %d, want: %d", resp.Offset, o)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, r := range records {
			resp, err := stream.Recv()
			require.NoError(t, err)
			// Equal compares values, not memory address
			require.Equal(t, resp.Record, &api.Record{
				Value:  r.Value,
				Offset: uint64(i),
			})
		}
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	// automatically assign a free port
	listener, err := net.Listen("tcp", "127.0.0.1:")
	require.NoError(t, err)

	// set up TLS
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: listener.Addr().String(),
		IsServer:      true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	// commit log dependency
	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)
	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		// as the client, you only need access to the CA to verify the server's certificate
		CAFile: config.CAFile,
		// cert and key file are added to the CA that the server and authenticate the client
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
		IsServer: false, // specify this for clarity
	})
	require.NoError(t, err)
	clientCreds := credentials.NewTLS(clientTLSConfig)
	// cc is a client connection to the server's address
	cc, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	require.NoError(t, err)
	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		listener.Close()
		clog.Remove()
	}
}
