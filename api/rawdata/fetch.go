package rawdata

import (
	"io"
	"net"
)

func (s *Server) handleFetch(conn net.Conn, hdr RequestHeader, body []byte) {
	req, err := DecodeFetchRequest(body)
	if err != nil {
		return
	}

	l, ok := s.registry.GetLog(req.Topic, req.Partition)
	if !ok {
		EncodeFetchResponseHeader(conn, hdr.CorrelationID, ErrUnknownTopicOrPartition, 0, 0, 0)
		return
	}

	hw := l.HighWatermark()
	bound := hw
	if req.ReplicaID != 0 {
		bound = l.LogEndOffset()
	}

	if req.FetchOffset == bound {
		EncodeFetchResponseHeader(conn, hdr.CorrelationID, ErrNone, hw, bound, 0)
		return
	}
	if req.FetchOffset > bound {
		EncodeFetchResponseHeader(conn, hdr.CorrelationID, ErrOffsetOutOfRange, hw, bound, 0)
		return
	}

	seg, rng, err := l.LocateRange(req.FetchOffset, req.MaxBytes)
	if err != nil {
		EncodeFetchResponseHeader(conn, hdr.CorrelationID, ErrOffsetOutOfRange, hw, hw, 0)
		return
	}

	f, err := seg.OpenReader()
	if err != nil {
		EncodeFetchResponseHeader(conn, hdr.CorrelationID, ErrInternal, hw, hw, 0)
		return
	}
	defer f.Close()

	if _, err := f.Seek(rng.Pos, io.SeekStart); err != nil {
		EncodeFetchResponseHeader(conn, hdr.CorrelationID, ErrInternal, hw, hw, 0)
		return
	}

	if err := EncodeFetchResponseHeader(conn, hdr.CorrelationID, ErrNone, hw, rng.NextOffset, uint32(rng.Length)); err != nil {
		return
	}

	io.CopyN(conn, f, rng.Length)
}
