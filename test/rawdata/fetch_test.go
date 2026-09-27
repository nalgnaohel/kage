package rawdata_test

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/nalgnaohel/kage/api/rawdata"
	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/storage"
)

type fetchResponse struct {
	CorrelationID uint32
	ErrorCode     rawdata.ErrorCode
	HighWatermark uint64
	NextOffset    uint64
	Payload       []byte
}

type wireRecord struct {
	Offset uint64
	Value  []byte
}

func recordSize(value []byte) int {
	return 8 + 4 + len(value)
}

func encodeFetchRequest(correlationID uint32, topic string, partition int32, fetchOffset uint64, maxBytes int32) []byte {
	body := make([]byte, 0, 2+len(topic)+4+8+4+4)

	topicLen := make([]byte, 2)
	binary.BigEndian.PutUint16(topicLen, uint16(len(topic)))
	body = append(body, topicLen...)
	body = append(body, []byte(topic)...)

	buf4 := make([]byte, 4)
	binary.BigEndian.PutUint32(buf4, uint32(partition))
	body = append(body, buf4...)

	buf8 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf8, fetchOffset)
	body = append(body, buf8...)

	binary.BigEndian.PutUint32(buf4, uint32(maxBytes))
	body = append(body, buf4...)

	replicaID := uint32(0)
	binary.BigEndian.PutUint32(buf4, replicaID)
	body = append(body, buf4...)

	header := make([]byte, 8)
	binary.BigEndian.PutUint16(header[0:2], rawdata.ApiFetch)
	binary.BigEndian.PutUint16(header[2:4], 0)
	binary.BigEndian.PutUint32(header[4:8], correlationID)

	frame := append(header, body...)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(frame)))
	return append(lenBuf, frame...)
}

func readFetchResponse(t *testing.T, conn net.Conn) fetchResponse {
	t.Helper()

	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		t.Fatalf("read response length: %v", err)
	}

	frame := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(conn, frame); err != nil {
		t.Fatalf("read response frame: %v", err)
	}

	payloadLen := binary.BigEndian.Uint32(frame[22:26])
	return fetchResponse{
		CorrelationID: binary.BigEndian.Uint32(frame[0:4]),
		ErrorCode:     rawdata.ErrorCode(binary.BigEndian.Uint16(frame[4:6])),
		HighWatermark: binary.BigEndian.Uint64(frame[6:14]),
		NextOffset:    binary.BigEndian.Uint64(frame[14:22]),
		Payload:       frame[26 : 26+payloadLen],
	}
}

func splitWireRecords(t *testing.T, payload []byte) []wireRecord {
	t.Helper()

	var recs []wireRecord
	pos := 0
	for pos < len(payload) {
		off := binary.BigEndian.Uint64(payload[pos : pos+8])
		length := binary.BigEndian.Uint32(payload[pos+8 : pos+12])
		pos += 12
		recs = append(recs, wireRecord{Offset: off, Value: payload[pos : pos+int(length)]})
		pos += int(length)
	}
	return recs
}

func newTestServer(t *testing.T) (*broker.Registry, net.Conn) {
	t.Helper()

	reg := broker.NewRegistry(t.TempDir(), storage.DefaultLogConfig())
	if err := reg.Startup(); err != nil {
		t.Fatalf("registry startup: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	srv := rawdata.NewServer(reg)
	go srv.Serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return reg, conn
}

func TestHandleFetch_HappyPath_SpansSeveralRecords(t *testing.T) {
	reg, conn := newTestServer(t)

	log, err := reg.CreateLog("topic-a", 0)
	if err != nil {
		t.Fatalf("CreateLog: %v", err)
	}

	values := [][]byte{[]byte("hello"), []byte("world"), []byte("foo"), []byte("bar"), []byte("baz")}
	for _, v := range values {
		if _, err := log.Append(v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	req := encodeFetchRequest(1, "topic-a", 0, 0, 1<<20)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readFetchResponse(t, conn)
	if resp.ErrorCode != rawdata.ErrNone {
		t.Fatalf("errorCode = %d, want ErrNone", resp.ErrorCode)
	}
	if resp.NextOffset != uint64(len(values)) {
		t.Fatalf("nextOffset = %d, want %d", resp.NextOffset, len(values))
	}

	recs := splitWireRecords(t, resp.Payload)
	if len(recs) != len(values) {
		t.Fatalf("got %d records, want %d", len(recs), len(values))
	}
	for i, rec := range recs {
		want, err := log.Read(rec.Offset)
		if err != nil {
			t.Fatalf("log.Read(%d): %v", rec.Offset, err)
		}
		if string(rec.Value) != string(want) {
			t.Errorf("record %d: got %q, want %q", i, rec.Value, want)
		}
	}
}

func TestHandleFetch_MaxBytesCutsOffMidway(t *testing.T) {
	reg, conn := newTestServer(t)

	log, err := reg.CreateLog("topic-b", 0)
	if err != nil {
		t.Fatalf("CreateLog: %v", err)
	}

	values := [][]byte{[]byte("aaaa"), []byte("bbbb"), []byte("cccc")}
	for _, v := range values {
		if _, err := log.Append(v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	maxBytes := recordSize(values[0]) + recordSize(values[1])

	req := encodeFetchRequest(2, "topic-b", 0, 0, int32(maxBytes))
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readFetchResponse(t, conn)
	if resp.ErrorCode != rawdata.ErrNone {
		t.Fatalf("errorCode = %d, want ErrNone", resp.ErrorCode)
	}
	if resp.NextOffset != 2 {
		t.Fatalf("nextOffset = %d, want 2", resp.NextOffset)
	}

	recs := splitWireRecords(t, resp.Payload)
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
}

func TestHandleFetch_OffsetEqualsHighWatermark_ReturnsEmpty(t *testing.T) {
	reg, conn := newTestServer(t)

	log, err := reg.CreateLog("topic-c", 0)
	if err != nil {
		t.Fatalf("CreateLog: %v", err)
	}
	if _, err := log.Append([]byte("only-record")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	req := encodeFetchRequest(3, "topic-c", 0, log.HighWatermark(), 1<<20)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readFetchResponse(t, conn)
	if resp.ErrorCode != rawdata.ErrNone {
		t.Fatalf("errorCode = %d, want ErrNone", resp.ErrorCode)
	}
	if len(resp.Payload) != 0 {
		t.Fatalf("payload len = %d, want 0", len(resp.Payload))
	}
	if resp.NextOffset != log.HighWatermark() {
		t.Fatalf("nextOffset = %d, want %d", resp.NextOffset, log.HighWatermark())
	}
}

func TestHandleFetch_OffsetPastHighWatermark_ReturnsError(t *testing.T) {
	reg, conn := newTestServer(t)

	log, err := reg.CreateLog("topic-d", 0)
	if err != nil {
		t.Fatalf("CreateLog: %v", err)
	}
	if _, err := log.Append([]byte("only-record")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	req := encodeFetchRequest(4, "topic-d", 0, 999, 1<<20)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readFetchResponse(t, conn)
	if resp.ErrorCode != rawdata.ErrOffsetOutOfRange {
		t.Fatalf("errorCode = %d, want ErrOffsetOutOfRange", resp.ErrorCode)
	}
}

func TestHandleFetch_UnknownTopicOrPartition_ReturnsError(t *testing.T) {
	_, conn := newTestServer(t)

	req := encodeFetchRequest(5, "no-such-topic", 0, 0, 1<<20)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readFetchResponse(t, conn)
	if resp.ErrorCode != rawdata.ErrUnknownTopicOrPartition {
		t.Fatalf("errorCode = %d, want ErrUnknownTopicOrPartition", resp.ErrorCode)
	}
}
