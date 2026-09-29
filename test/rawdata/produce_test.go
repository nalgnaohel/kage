package rawdata_test

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nalgnaohel/kage/api/rawdata"
	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/storage"
)

var errWaitForHWTimeout = errors.New("wait for hw: timed out")

func newTestServerWithWaitForHW(t *testing.T, waitForHW func(topic string, partition int32, offset uint64, timeout time.Duration) error) (*broker.Registry, net.Conn) {
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
	srv.WaitForHW = waitForHW
	go srv.Serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return reg, conn
}

type produceResponse struct {
	CorrelationID uint32
	ErrorCode     rawdata.ErrorCode
	BaseOffset    uint64
}

func encodeProduceRequest(correlationID uint32, topic string, partition int32, requiredAcks uint8, value []byte) []byte {
	body := make([]byte, 0, topicLenWidth+len(topic)+partitionWidth+requiredAcksWidth+valueLenWidth+len(value))

	topicLenBuf := make([]byte, topicLenWidth)
	binary.BigEndian.PutUint16(topicLenBuf, uint16(len(topic)))
	body = append(body, topicLenBuf...)
	body = append(body, []byte(topic)...)

	partitionBuf := make([]byte, partitionWidth)
	binary.BigEndian.PutUint32(partitionBuf, uint32(partition))
	body = append(body, partitionBuf...)

	body = append(body, requiredAcks)

	valueLenBuf := make([]byte, valueLenWidth)
	binary.BigEndian.PutUint32(valueLenBuf, uint32(len(value)))
	body = append(body, valueLenBuf...)
	body = append(body, value...)

	header := make([]byte, requestHeaderSize)
	pos := 0
	binary.BigEndian.PutUint16(header[pos:pos+apiKeyWidth], rawdata.ApiProduce)
	pos += apiKeyWidth
	binary.BigEndian.PutUint16(header[pos:pos+apiVersionWidth], 0)
	pos += apiVersionWidth
	binary.BigEndian.PutUint32(header[pos:pos+correlationIDWidth], correlationID)

	frame := append(header, body...)
	lenBuf := make([]byte, frameLengthWidth)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(frame)))
	return append(lenBuf, frame...)
}

func readProduceResponse(t *testing.T, conn net.Conn) produceResponse {
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
	baseOffset := binary.BigEndian.Uint64(frame[pos : pos+baseOffsetWidth])

	return produceResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
		BaseOffset:    baseOffset,
	}
}

func TestHandleProduce_HappyPath_AppendsAndReturnsOffset(t *testing.T) {
	reg, conn := newTestServer(t)

	log, err := reg.CreateLog("produce-a", 0)
	if err != nil {
		t.Fatalf("CreateLog: %v", err)
	}

	req := encodeProduceRequest(1, "produce-a", 0, 1, []byte("hello"))
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readProduceResponse(t, conn)
	if resp.ErrorCode != rawdata.ErrNone {
		t.Fatalf("errorCode = %d, want ErrNone", resp.ErrorCode)
	}
	if resp.CorrelationID != 1 {
		t.Fatalf("correlationID = %d, want 1", resp.CorrelationID)
	}

	got, err := log.Read(resp.BaseOffset)
	if err != nil {
		t.Fatalf("log.Read(%d): %v", resp.BaseOffset, err)
	}
	if string(got) != "hello" {
		t.Fatalf("record value = %q, want %q", got, "hello")
	}
}

func TestHandleProduce_MultipleRequests_OffsetsIncrease(t *testing.T) {
	reg, conn := newTestServer(t)

	if _, err := reg.CreateLog("produce-b", 0); err != nil {
		t.Fatalf("CreateLog: %v", err)
	}

	values := []string{"one", "two", "three"}
	var offsets []uint64
	for i, v := range values {
		req := encodeProduceRequest(uint32(i), "produce-b", 0, 1, []byte(v))
		if _, err := conn.Write(req); err != nil {
			t.Fatalf("write request: %v", err)
		}
		resp := readProduceResponse(t, conn)
		if resp.ErrorCode != rawdata.ErrNone {
			t.Fatalf("errorCode = %d, want ErrNone", resp.ErrorCode)
		}
		offsets = append(offsets, resp.BaseOffset)
	}

	for i := 1; i < len(offsets); i++ {
		if offsets[i] <= offsets[i-1] {
			t.Fatalf("offsets not increasing: %v", offsets)
		}
	}
}

func TestHandleProduce_UnknownTopicOrPartition_ReturnsError(t *testing.T) {
	_, conn := newTestServer(t)

	req := encodeProduceRequest(2, "no-such-topic", 0, 1, []byte("x"))
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readProduceResponse(t, conn)
	if resp.ErrorCode != rawdata.ErrUnknownTopicOrPartition {
		t.Fatalf("errorCode = %d, want ErrUnknownTopicOrPartition", resp.ErrorCode)
	}
}

func TestHandleProduce_AcksAll_BlocksUntilWaitForHWReturns(t *testing.T) {
	release := make(chan struct{})
	var gotOffset uint64
	reg, conn := newTestServerWithWaitForHW(t, func(topic string, partition int32, offset uint64, timeout time.Duration) error {
		gotOffset = offset
		<-release
		return nil
	})

	if _, err := reg.CreateLog("produce-acks-all", 0); err != nil {
		t.Fatalf("CreateLog: %v", err)
	}

	req := encodeProduceRequest(1, "produce-acks-all", 0, rawdata.RequiredAcksAll, []byte("hello"))
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	done := make(chan produceResponse, 1)
	go func() { done <- readProduceResponse(t, conn) }()

	select {
	case <-done:
		t.Fatal("response arrived before WaitForHW returned")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)

	select {
	case resp := <-done:
		if resp.ErrorCode != rawdata.ErrNone {
			t.Fatalf("errorCode = %d, want ErrNone", resp.ErrorCode)
		}
		if gotOffset != resp.BaseOffset+1 {
			t.Fatalf("WaitForHW called with offset %d, want %d", gotOffset, resp.BaseOffset+1)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("response never arrived after WaitForHW returned")
	}
}

func TestHandleProduce_AcksAll_WaitForHWErrorReturnsInternal(t *testing.T) {
	reg, conn := newTestServerWithWaitForHW(t, func(topic string, partition int32, offset uint64, timeout time.Duration) error {
		return errWaitForHWTimeout
	})

	if _, err := reg.CreateLog("produce-acks-all-timeout", 0); err != nil {
		t.Fatalf("CreateLog: %v", err)
	}

	req := encodeProduceRequest(1, "produce-acks-all-timeout", 0, rawdata.RequiredAcksAll, []byte("hello"))
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp := readProduceResponse(t, conn)
	if resp.ErrorCode != rawdata.ErrInternal {
		t.Fatalf("errorCode = %d, want ErrInternal", resp.ErrorCode)
	}
}
