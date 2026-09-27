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
	frameLengthWidth   = 4
	apiKeyWidth        = 2
	apiVersionWidth    = 2
	correlationIDWidth = 4
)

const (
	topicLenWidth     = 2
	partitionWidth    = 4
	fetchOffsetWidth  = 8
	maxBytesWidth     = 4
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

const requestHeaderSize = apiKeyWidth + apiVersionWidth + correlationIDWidth

const responseHeaderSize = correlationIDWidth + errorCodeWidth + highWatermarkWidth + nextOffsetWidth + payloadLenWidth

const produceResponseHeaderSize = correlationIDWidth + errorCodeWidth + baseOffsetWidth

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
	var lenBuf [frameLengthWidth]byte
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

	pos := 0
	apiKey := enc.Uint16(frame[pos : pos+apiKeyWidth])
	pos += apiKeyWidth
	apiVersion := enc.Uint16(frame[pos : pos+apiVersionWidth])
	pos += apiVersionWidth
	correlationID := enc.Uint32(frame[pos : pos+correlationIDWidth])

	hdr := RequestHeader{APIKey: apiKey, APIVersion: apiVersion, CorrelationID: correlationID}
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

func DecodeProduceRequest(body []byte) (ProduceRequest, error) {
	if len(body) < topicLenWidth {
		return ProduceRequest{}, fmt.Errorf("rawdata: produce request too short")
	}
	topicLen := int(enc.Uint16(body[0:topicLenWidth]))
	pos := topicLenWidth
	if len(body) < pos+topicLen+partitionWidth+requiredAcksWidth+valueLenWidth {
		return ProduceRequest{}, fmt.Errorf("rawdata: produce request truncated")
	}

	topic := string(body[pos : pos+topicLen])
	pos += topicLen
	partition := int32(enc.Uint32(body[pos : pos+partitionWidth]))
	pos += partitionWidth
	requiredAcks := body[pos]
	pos += requiredAcksWidth
	valueLen := int(enc.Uint32(body[pos : pos+valueLenWidth]))
	pos += valueLenWidth
	if len(body) < pos+valueLen {
		return ProduceRequest{}, fmt.Errorf("rawdata: produce request truncated")
	}

	return ProduceRequest{
		Topic:        topic,
		Partition:    partition,
		RequiredAcks: requiredAcks,
		Value:        body[pos : pos+valueLen],
	}, nil
}

func EncodeFetchResponseHeader(w io.Writer, correlationID uint32, code ErrorCode, hw, nextOffset uint64, payloadLen uint32) error {
	totalLength := uint32(responseHeaderSize) + payloadLen

	buf := make([]byte, frameLengthWidth+responseHeaderSize)
	pos := 0
	enc.PutUint32(buf[pos:pos+frameLengthWidth], totalLength)
	pos += frameLengthWidth
	enc.PutUint32(buf[pos:pos+correlationIDWidth], correlationID)
	pos += correlationIDWidth
	enc.PutUint16(buf[pos:pos+errorCodeWidth], uint16(code))
	pos += errorCodeWidth
	enc.PutUint64(buf[pos:pos+highWatermarkWidth], hw)
	pos += highWatermarkWidth
	enc.PutUint64(buf[pos:pos+nextOffsetWidth], nextOffset)
	pos += nextOffsetWidth
	enc.PutUint32(buf[pos:pos+payloadLenWidth], payloadLen)

	_, err := w.Write(buf)
	return err
}

func EncodeProduceResponse(w io.Writer, correlationID uint32, code ErrorCode, baseOffset uint64) error {
	buf := make([]byte, frameLengthWidth+produceResponseHeaderSize)
	pos := 0
	enc.PutUint32(buf[pos:pos+frameLengthWidth], uint32(produceResponseHeaderSize))
	pos += frameLengthWidth
	enc.PutUint32(buf[pos:pos+correlationIDWidth], correlationID)
	pos += correlationIDWidth
	enc.PutUint16(buf[pos:pos+errorCodeWidth], uint16(code))
	pos += errorCodeWidth
	enc.PutUint64(buf[pos:pos+baseOffsetWidth], baseOffset)

	_, err := w.Write(buf)
	return err
}
