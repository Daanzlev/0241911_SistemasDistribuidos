package server

import (
	"context"

	logpkg "Proyecto/api/v1"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Parametros struct {
	Registro   CommitLog
	Authorizer Authorizer
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

var _ logpkg.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	logpkg.UnimplementedLogServer
	Parametros *Parametros
}

func newgrpcServer(config *Parametros) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Parametros: config,
	}
	return srv, nil
}

func NewGRPCServer(config *Parametros, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			grpc_auth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		grpc_auth.UnaryServerInterceptor(authenticate),
	)))
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	logpkg.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}
func (s *grpcServer) Produce(ctx context.Context, req *logpkg.ProduceRequest) (*logpkg.ProduceResponse, error) {
	if err := s.Parametros.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}
	offset, err := s.Parametros.Registro.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &logpkg.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *logpkg.ConsumeRequest) (*logpkg.ConsumeResponse, error) {
	if err := s.Parametros.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}
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
		if err = stream.Send(res); err != nil {
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
			switch err.(type) {
			case nil:
			case logpkg.ErrOffsetOutOfRange:
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

type CommitLog interface {
	Append(*logpkg.Record) (uint64, error)
	Read(uint64) (*logpkg.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"Can't find peer information",
		).Err()
	}
	if peer.AuthInfo == nil {
		return ctx, status.New(
			codes.Unauthenticated,
			"No security on transport protocol",
		).Err()
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)
	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
