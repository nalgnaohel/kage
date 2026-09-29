package rawdata

import (
	"net"
	"time"
)

const (
	produceBatchSize = 100
	produceLinger    = 10 * time.Millisecond
	acksAllTimeout   = 5 * time.Second
)

func (s *Server) handleProduce(conn net.Conn, hdr RequestHeader, body []byte) {
	req, err := DecodeProduceRequest(body)
	if err != nil {
		return
	}

	fl, ok := s.registry.GetFlusher(req.Topic, req.Partition, produceBatchSize, produceLinger)
	if !ok {
		EncodeProduceResponse(conn, hdr.CorrelationID, ErrUnknownTopicOrPartition, 0)
		return
	}

	res := <-fl.Push(req.Value)
	if res.Error != nil {
		EncodeProduceResponse(conn, hdr.CorrelationID, ErrInternal, 0)
		return
	}

	if req.RequiredAcks == RequiredAcksAll && s.WaitForHW != nil {
		if err := s.WaitForHW(req.Topic, req.Partition, uint64(res.Offset)+1, acksAllTimeout); err != nil {
			EncodeProduceResponse(conn, hdr.CorrelationID, ErrInternal, uint64(res.Offset))
			return
		}
	}

	EncodeProduceResponse(conn, hdr.CorrelationID, ErrNone, uint64(res.Offset))
}
