package server

import (
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

	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {

}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	// setting 0 as the port automatically assigns a free port
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	// cc is a client connection to the listener's address
	cc, err := grpc.Dial(listener.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	return nil, nil, nil
}
