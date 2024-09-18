package server

import (
	"context"

	logpkg "Proyecto/api/v1" // Actualiza esta ruta según sea necesario

	"google.golang.org/grpc"
)

type Parametros struct {
	Registro CommitLog
}

var _ logpkg.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	logpkg.UnimplementedLogServer
	Parametros *Parametros
}

func NewGRPCServer(param *Parametros) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(param)
	if err != nil {
		return nil, err
	}
	logpkg.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func newgrpcServer(param *Parametros) (*grpcServer, error) {
	srv := &grpcServer{
		Parametros: param,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *logpkg.ProduceRequest) (*logpkg.ProduceResponse, error) {
	offset, err := s.Parametros.Registro.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &logpkg.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *logpkg.ConsumeRequest) (*logpkg.ConsumeResponse, error) {
	record, err := s.Parametros.Registro.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &logpkg.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream logpkg.Log_ProduceStreamServer) error {
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

func (s *grpcServer) ConsumeStream(req *logpkg.ConsumeRequest, stream logpkg.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			if err != nil {
				return err
			}
			if err := stream.Send(res); err != nil {
				return err
			}
			req.Offset++ // Incrementa el offset para el siguiente registro
		}
	}
}

type CommitLog interface {
	Append(*logpkg.Record) (uint64, error)
	Read(uint64) (*logpkg.Record, error)
}
