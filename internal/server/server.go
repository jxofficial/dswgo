package server

import (
	"context"

	"google.golang.org/grpc"

	api "github.com/jxofficial/proglog/api/v1"
)

type Config struct {
	CommitLog
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type grpcServer struct {
	*Config
	api.UnimplementedLogServer
}

func NewGRPCServer(c *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(c)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse,
	error,
) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse,
	error,
) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream calls grpcServer.Produce() for every req in the stream.
// It sends the response back into the stream.
// It implements a bidirectional streaming RPC.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		resp, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		err = stream.Send(resp)
		if err != nil {
			return err
		}
	}
}

// ConsumeStream is implements a server side RPC stream, which serves every record following request offset,
// including records that are not in the log (yet).
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			resp, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			// if record currently does not exist, the stream will wait
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			err = stream.Send(resp)
			if err != nil {
				return err
			}
			req.Offset++
		}
	}
}

func newgrpcServer(c *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: c,
	}
	return srv, nil
}
