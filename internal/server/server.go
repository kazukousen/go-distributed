package server

import (
	"context"

	"google.golang.org/grpc"

	api "github.com/kazukousen/go-distributed/api/v1"
)

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	commitLog CommitLog
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

func NewGRPCServer(commitLog CommitLog) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newServer(commitLog)
	if err != nil {
		return nil, err
	}

	api.RegisterLogServer(gsrv, srv)

	return gsrv, nil
}

func newServer(commitLog CommitLog) (*grpcServer, error) {
	return &grpcServer{
		commitLog: commitLog,
	}, nil
}

func (s *grpcServer) Produce(_ context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	off, err := s.commitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: off}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	rec, err := s.commitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Record: rec}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
				// pass through
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			if err = stream.Send(res); err != nil {
				return err
			}

			req.Offset++
		}
	}
}
