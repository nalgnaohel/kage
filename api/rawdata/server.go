package rawdata

import (
	"net"

	"github.com/nalgnaohel/kage/broker"
)

type Server struct {
	registry *broker.Registry

	OnReplicaFetch func(topic string, partition int32, replicaID int32, fetchOffset uint64)
}

func NewServer(reg *broker.Registry) *Server {
	return &Server{registry: reg}
}

func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		hdr, body, err := ReadRequest(conn)
		if err != nil {
			return
		}

		switch hdr.APIKey {
		case ApiFetch:
			s.handleFetch(conn, hdr, body)
		case ApiProduce:
			s.handleProduce(conn, hdr, body)
		default:
			return
		}
	}
}
