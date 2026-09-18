package rawdata_test

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/nalgnaohel/kage/api/rawdata"
)

type produceResponse struct {
	CorrelationID uint32
	ErrorCode     rawdata.ErrorCode
	BaseOffset    uint64
}

func encodeProduceRequest(correlationID uint32, topic string, partition int32, requiredAcks uint8, value []byte) []byte {
	body := make([]byte, 0, 2+len(topic)+4+1+4+len(value))

	topicLen := make([]byte, 2)
	binary.BigEndian.PutUint16(topicLen, uint16(len(topic)))
	body = append(body, topicLen...)
	body = append(body, []byte(topic)...)

	buf4 := make([]byte, 4)
	binary.BigEndian.PutUint32(buf4, uint32(partition))
	body = append(body, buf4...)

	body = append(body, requiredAcks)

	binary.BigEndian.PutUint32(buf4, uint32(len(value)))
	body = append(body, buf4...)
	body = append(body, value...)

	header := make([]byte, 8)
	binary.BigEndian.PutUint16(header[0:2], rawdata.ApiProduce)
	binary.BigEndian.PutUint16(header[2:4], 0)
	binary.BigEndian.PutUint32(header[4:8], correlationID)

	frame := append(header, body...)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(frame)))
	return append(lenBuf, frame...)
}

func readProduceResponse(t *testing.T, conn net.Conn) produceResponse {
	t.Helper()

	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		t.Fatalf("read response length: %v", err)
	}

	frame := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(conn, frame); err != nil {
		t.Fatalf("read response frame: %v", err)
	}

	return produceResponse{
		CorrelationID: binary.BigEndian.Uint32(frame[0:4]),
		ErrorCode:     rawdata.ErrorCode(binary.BigEndian.Uint16(frame[4:6])),
		BaseOffset:    binary.BigEndian.Uint64(frame[6:14]),
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
