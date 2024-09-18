package server

import (
	"context"
	"log"
	"net"

	api "lab3/api/v1"        // aqui esta lo del CommitLog
	logpkg "lab3/api/v1/log" //aqui esta el log_grpc.pb.go y el log.pg.go

	"google.golang.org/grpc"
)

type Config struct {
	CommitLog CommitLog
}

var _ logpkg.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	logpkg.UnimplementedLogServer
	*Config
}

func newgrpcServer(log *CommitLog) (*grpcServer, error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *logpkg.ProduceRequest) (*logpkg.ProduceResponse, error) {
	offset, err := s.Log.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &logpkg.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *logpkg.ConsumeRequest) (*logpkg.ConsumeResponse, error) {
	record, err := s.Log.Read(req.Offset)
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

func main() {
	logObj, err := api.NewLog("/path/to/your/log/dir", api.Config{}) // Configura el log según sea necesario
	if err != nil {
		log.Fatal("failed to create log: ", err)
	}

	srv, err := newgrpcServer(logObj)
	if err != nil {
		log.Fatal("failed to create server: ", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("failed to listen: ", err)
	}

	grpcServer := grpc.NewServer()
	logpkg.RegisterLogServer(grpcServer, srv)
	log.Println("Server is running on port :50051")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("failed to serve: ", err)
	}
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}
