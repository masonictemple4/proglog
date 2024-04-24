package server

import (
	"context"
	"fmt"

	api "github.com/masonictemple4/proglog/api/v1"
	"google.golang.org/grpc"
)

type Config struct {
	CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(conf *Config) (*grpcServer, error) {
	srv := &grpcServer{
		Config: conf,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {

	println("Are we even hit????")
	fmt.Printf("The object from the request: %s\n", string(req.Record.Value))
	offset, err := s.CommitLog.Append(req.Record)

	if err != nil {
		fmt.Printf("Error writing: %v\n", err)
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		fmt.Printf("Error reading: %v\n", err)
		return nil, err
	}

	return &api.ConsumeResponse{Record: record}, nil

}

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}
