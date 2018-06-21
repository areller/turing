package turing

import (
	"github.com/areller/turing/proto"
	context "golang.org/x/net/context"
)

type protoServerImpl struct {

}

func (psi *protoServerImpl) Get(ctx context.Context, sr *proto.StringRequest) (*proto.StringResponse, error) {
	return nil, nil
}

type Server struct {

}

func (s *Server) Close() {

}

func (s *Server) Run() error {
	return nil
}

func NewServer(store KVStore) *Server {
	return &Server{

	}
}