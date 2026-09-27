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

type goldenRecord struct {
	Offset uint64
	Value  string
}

type fetchGolden struct {
	ErrorCode     string
	HighWatermark uint64
	NextOffset    uint64
	Records       []goldenRecord
}

func toGoldenRecords(recs []wireRecord) []goldenRecord {
	out := make([]goldenRecord, len(recs))
	for i, r := range recs {
		out[i] = goldenRecord{Offset: r.Offset, Value: string(r.Value)}
	}
	return out
}

const (
	frameLengthWidth   = 4
	apiKeyWidth        = 2
	apiVersionWidth    = 2
	correlationIDWidth = 4
)

const requestHeaderSize = apiKeyWidth + apiVersionWidth + correlationIDWidth

const (
	topicLenWidth     = 2
	partitionWidth    = 4
	fetchOffsetWidth  = 8
	maxBytesWidth     = 4
	replicaIDWidth    = 4
	requiredAcksWidth = 1
	valueLenWidth     = 4
)

const (
	errorCodeWidth     = 2
	highWatermarkWidth = 8
	nextOffsetWidth    = 8
	payloadLenWidth    = 4
	baseOffsetWidth    = 8
)

const (
	recordOffsetWidth = 8
	recordLenWidth    = 4
)

func recordSize(value []byte) int {
	return recordOffsetWidth + recordLenWidth + len(value)
}

func encodeFetchRequest(correlationID uint32, topic string, partition int32, fetchOffset uint64, maxBytes int32) []byte {
	body := make([]byte, 0, topicLenWidth+len(topic)+partitionWidth+fetchOffsetWidth+maxBytesWidth+replicaIDWidth)

	topicLenBuf := make([]byte, topicLenWidth)
	binary.BigEndian.PutUint16(topicLenBuf, uint16(len(topic)))
	body = append(body, topicLenBuf...)
	body = append(body, []byte(topic)...)

	partitionBuf := make([]byte, partitionWidth)
	binary.BigEndian.PutUint32(partitionBuf, uint32(partition))
	body = append(body, partitionBuf...)

	fetchOffsetBuf := make([]byte, fetchOffsetWidth)
	binary.BigEndian.PutUint64(fetchOffsetBuf, fetchOffset)
	body = append(body, fetchOffsetBuf...)

	maxBytesBuf := make([]byte, maxBytesWidth)
	binary.BigEndian.PutUint32(maxBytesBuf, uint32(maxBytes))
	body = append(body, maxBytesBuf...)

	replicaIDBuf := make([]byte, replicaIDWidth)
	binary.BigEndian.PutUint32(replicaIDBuf, 0)
	body = append(body, replicaIDBuf...)

	header := make([]byte, requestHeaderSize)
	pos := 0
	binary.BigEndian.PutUint16(header[pos:pos+apiKeyWidth], rawdata.ApiFetch)
	pos += apiKeyWidth
	binary.BigEndian.PutUint16(header[pos:pos+apiVersionWidth], 0)
	pos += apiVersionWidth
	binary.BigEndian.PutUint32(header[pos:pos+correlationIDWidth], correlationID)

	frame := append(header, body...)
	lenBuf := make([]byte, frameLengthWidth)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(frame)))
	return append(lenBuf, frame...)
}

func readFetchResponse(t *testing.T, conn net.Conn) fetchResponse {
	t.Helper()

	var lenBuf [frameLengthWidth]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		t.Fatalf("read response length: %v", err)
	}

	frame := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(conn, frame); err != nil {
		t.Fatalf("read response frame: %v", err)
	}

	pos := 0
	correlationID := binary.BigEndian.Uint32(frame[pos : pos+correlationIDWidth])
	pos += correlationIDWidth
	errorCode := rawdata.ErrorCode(binary.BigEndian.Uint16(frame[pos : pos+errorCodeWidth]))
	pos += errorCodeWidth
	highWatermark := binary.BigEndian.Uint64(frame[pos : pos+highWatermarkWidth])
	pos += highWatermarkWidth
	nextOffset := binary.BigEndian.Uint64(frame[pos : pos+nextOffsetWidth])
	pos += nextOffsetWidth
	payloadLen := binary.BigEndian.Uint32(frame[pos : pos+payloadLenWidth])
	pos += payloadLenWidth

	return fetchResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
		HighWatermark: highWatermark,
		NextOffset:    nextOffset,
		Payload:       frame[pos : pos+int(payloadLen)],
	}
}

func splitWireRecords(t *testing.T, payload []byte) []wireRecord {
	t.Helper()

	var recs []wireRecord
	pos := 0
	for pos < len(payload) {
		off := binary.BigEndian.Uint64(payload[pos : pos+recordOffsetWidth])
		pos += recordOffsetWidth
		length := binary.BigEndian.Uint32(payload[pos : pos+recordLenWidth])
		pos += recordLenWidth
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
	log.SetHighWatermark(log.LogEndOffset())

	req := encodeFetchRequest(1, "topic-a", 0, 0, 1<<20)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readFetchResponse(t, conn)
	recs := splitWireRecords(t, resp.Payload)

	for _, rec := range recs {
		want, err := log.Read(rec.Offset)
		if err != nil {
			t.Fatalf("log.Read(%d): %v", rec.Offset, err)
		}
		if string(rec.Value) != string(want) {
			t.Errorf("record at offset %d: fetch payload %q does not match log.Read %q", rec.Offset, rec.Value, want)
		}
	}

	assertGolden(t, "fetch", fetchGolden{
		ErrorCode:     errorCodeString(resp.ErrorCode),
		HighWatermark: resp.HighWatermark,
		NextOffset:    resp.NextOffset,
		Records:       toGoldenRecords(recs),
	})
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
	log.SetHighWatermark(log.LogEndOffset())

	maxBytes := recordSize(values[0]) + recordSize(values[1])

	req := encodeFetchRequest(2, "topic-b", 0, 0, int32(maxBytes))
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readFetchResponse(t, conn)
	assertGolden(t, "fetch", fetchGolden{
		ErrorCode:     errorCodeString(resp.ErrorCode),
		HighWatermark: resp.HighWatermark,
		NextOffset:    resp.NextOffset,
		Records:       toGoldenRecords(splitWireRecords(t, resp.Payload)),
	})
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
	log.SetHighWatermark(log.LogEndOffset())

	req := encodeFetchRequest(3, "topic-c", 0, log.HighWatermark(), 1<<20)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readFetchResponse(t, conn)
	assertGolden(t, "fetch", fetchGolden{
		ErrorCode:     errorCodeString(resp.ErrorCode),
		HighWatermark: resp.HighWatermark,
		NextOffset:    resp.NextOffset,
		Records:       toGoldenRecords(splitWireRecords(t, resp.Payload)),
	})
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
	log.SetHighWatermark(log.LogEndOffset())

	req := encodeFetchRequest(4, "topic-d", 0, 999, 1<<20)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readFetchResponse(t, conn)
	assertGolden(t, "fetch", fetchGolden{
		ErrorCode:     errorCodeString(resp.ErrorCode),
		HighWatermark: resp.HighWatermark,
		NextOffset:    resp.NextOffset,
		Records:       toGoldenRecords(splitWireRecords(t, resp.Payload)),
	})
}

func TestHandleFetch_UnknownTopicOrPartition_ReturnsError(t *testing.T) {
	_, conn := newTestServer(t)

	req := encodeFetchRequest(5, "no-such-topic", 0, 0, 1<<20)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readFetchResponse(t, conn)
	assertGolden(t, "fetch", fetchGolden{
		ErrorCode:     errorCodeString(resp.ErrorCode),
		HighWatermark: resp.HighWatermark,
		NextOffset:    resp.NextOffset,
		Records:       toGoldenRecords(splitWireRecords(t, resp.Payload)),
	})
}
