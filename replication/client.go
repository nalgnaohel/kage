package replication

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/nalgnaohel/kage/api/rawdata"
)

type FetchResult struct {
	Header  rawdata.FetchResponseHeader
	Records []Record
}

type Record struct {
	Offset uint64
	Value  []byte
}

const (
	recordOffsetWidth = 8
	recordLenWidth    = 4
)

func FetchFrom(conn net.Conn, correlationID uint32, req rawdata.FetchRequest) (FetchResult, error) {
	if err := rawdata.EncodeFetchRequest(conn, correlationID, req); err != nil {
		return FetchResult{}, err
	}

	hdr, err := rawdata.DecodeFetchResponseHeader(conn)
	if err != nil {
		return FetchResult{}, err
	}
	if hdr.Code != rawdata.ErrNone {
		return FetchResult{}, fmt.Errorf("replication: fetch %s-%d: error code %d", req.Topic, req.Partition, hdr.Code)
	}

	payload := make([]byte, hdr.PayloadLen)
	if hdr.PayloadLen > 0 {
		if _, err := io.ReadFull(conn, payload); err != nil {
			return FetchResult{}, err
		}
	}

	return FetchResult{Header: hdr, Records: parseRecords(payload)}, nil
}

func parseRecords(payload []byte) []Record {
	var recs []Record
	pos := 0
	for pos < len(payload) {
		offset := binary.BigEndian.Uint64(payload[pos : pos+recordOffsetWidth])
		pos += recordOffsetWidth
		length := binary.BigEndian.Uint32(payload[pos : pos+recordLenWidth])
		pos += recordLenWidth
		recs = append(recs, Record{Offset: offset, Value: payload[pos : pos+int(length)]})
		pos += int(length)
	}
	return recs
}
