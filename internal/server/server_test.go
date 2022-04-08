package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	api "github.com/jxofficial/proglog/api/v1"
	"github.com/jxofficial/proglog/internal/log"
)

func TestServer(t *testing.T) {
	tt := map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
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

//
// func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
// 	ctx := context.Background()
// 	produce, err := client.Produce(ctx, &api.ProduceRequest{
// 		Record: &api.Record{
// 			Value: []byte("hello world"),
// 		}})
// }

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	// setting 0 as the port automatically assigns a free port
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

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
	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	// cc is a client connection to the server's address
	cc, err := grpc.Dial(listener.Addr().String(), clientOptions...)
	require.NoError(t, err)
	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		listener.Close()
		clog.Remove()
	}
}
