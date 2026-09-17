package rawdata

import (
	"encoding/binary"
	"fmt"
	"io"
)

var enc = binary.BigEndian

const (
	ApiFetch   uint16 = 0
	ApiProduce uint16 = 1
)

type ErrorCode uint16

const (
	ErrNone ErrorCode = iota
	ErrUnknownTopicOrPartition
	ErrOffsetOutOfRange
	ErrInternal
)

const (
	topicLenWidth    = 2
	partitionWidth   = 4
	fetchOffsetWidth = 8
	maxBytesWidth    = 4
)

const requestHeaderSize = 2 + 2 + 4

const responseHeaderSize = 4 + 2 + 8 + 8 + 4

type RequestHeader struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
}

type FetchRequest struct {
	Topic       string
	Partition   int32
	FetchOffset uint64
	MaxBytes    int32
}

type ProduceRequest struct {
	Topic        string
	Partition    int32
	RequiredAcks uint8
	Value        []byte
}

func ReadRequest(r io.Reader) (RequestHeader, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return RequestHeader{}, nil, err
	}

	frame := make([]byte, enc.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(r, frame); err != nil {
		return RequestHeader{}, nil, err
	}
	if len(frame) < requestHeaderSize {
		return RequestHeader{}, nil, fmt.Errorf("rawdata: request frame too short (%d bytes)", len(frame))
	}

	hdr := RequestHeader{
		APIKey:        enc.Uint16(frame[0:2]),
		APIVersion:    enc.Uint16(frame[2:4]),
		CorrelationID: enc.Uint32(frame[4:8]),
	}
	return hdr, frame[requestHeaderSize:], nil
}

func DecodeFetchRequest(body []byte) (FetchRequest, error) {
	if len(body) < topicLenWidth {
		return FetchRequest{}, fmt.Errorf("rawdata: fetch request too short")
	}
	topicLen := int(enc.Uint16(body[0:topicLenWidth]))
	pos := topicLenWidth
	if len(body) < pos+topicLen+partitionWidth+fetchOffsetWidth+maxBytesWidth {
		return FetchRequest{}, fmt.Errorf("rawdata: fetch request truncated")
	}

	topic := string(body[pos : pos+topicLen])
	pos += topicLen
	partition := int32(enc.Uint32(body[pos : pos+partitionWidth]))
	pos += partitionWidth
	fetchOffset := enc.Uint64(body[pos : pos+fetchOffsetWidth])
	pos += fetchOffsetWidth
	maxBytes := int32(enc.Uint32(body[pos : pos+maxBytesWidth]))

	return FetchRequest{
		Topic:       topic,
		Partition:   partition,
		FetchOffset: fetchOffset,
		MaxBytes:    maxBytes,
	}, nil
}

func EncodeFetchResponseHeader(w io.Writer, correlationID uint32, code ErrorCode, hw, nextOffset uint64, payloadLen uint32) error {
	totalLength := uint32(responseHeaderSize) + payloadLen

	buf := make([]byte, 4+responseHeaderSize)
	enc.PutUint32(buf[0:4], totalLength)
	enc.PutUint32(buf[4:8], correlationID)
	enc.PutUint16(buf[8:10], uint16(code))
	enc.PutUint64(buf[10:18], hw)
	enc.PutUint64(buf[18:26], nextOffset)
	enc.PutUint32(buf[26:30], payloadLen)

	_, err := w.Write(buf)
	return err
}
