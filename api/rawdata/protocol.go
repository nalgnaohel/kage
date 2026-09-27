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
	ReplicaID   int32
}

type FetchResponseHeader struct {
	CorrelationID uint32
	Code          ErrorCode
	HighWatermark uint64
	NextOffset    uint64
	PayloadLen    uint32
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
	if len(body) < pos+topicLen+partitionWidth+fetchOffsetWidth+maxBytesWidth+replicaIDWidth {
		return FetchRequest{}, fmt.Errorf("rawdata: fetch request truncated")
	}

	topic := string(body[pos : pos+topicLen])
	pos += topicLen
	partition := int32(enc.Uint32(body[pos : pos+partitionWidth]))
	pos += partitionWidth
	fetchOffset := enc.Uint64(body[pos : pos+fetchOffsetWidth])
	pos += fetchOffsetWidth
	maxBytes := int32(enc.Uint32(body[pos : pos+maxBytesWidth]))
	pos += maxBytesWidth
	replicaID := int32(enc.Uint32(body[pos : pos+replicaIDWidth]))

	return FetchRequest{
		Topic:       topic,
		Partition:   partition,
		FetchOffset: fetchOffset,
		MaxBytes:    maxBytes,
		ReplicaID:   replicaID,
	}, nil
}

func EncodeFetchRequest(w io.Writer, correlationID uint32, req FetchRequest) error {
	body := make([]byte, 0, topicLenWidth+len(req.Topic)+partitionWidth+fetchOffsetWidth+maxBytesWidth+replicaIDWidth)

	topicLenBuf := make([]byte, topicLenWidth)
	enc.PutUint16(topicLenBuf, uint16(len(req.Topic)))
	body = append(body, topicLenBuf...)
	body = append(body, []byte(req.Topic)...)

	partitionBuf := make([]byte, partitionWidth)
	enc.PutUint32(partitionBuf, uint32(req.Partition))
	body = append(body, partitionBuf...)

	fetchOffsetBuf := make([]byte, fetchOffsetWidth)
	enc.PutUint64(fetchOffsetBuf, req.FetchOffset)
	body = append(body, fetchOffsetBuf...)

	maxBytesBuf := make([]byte, maxBytesWidth)
	enc.PutUint32(maxBytesBuf, uint32(req.MaxBytes))
	body = append(body, maxBytesBuf...)

	replicaIDBuf := make([]byte, replicaIDWidth)
	enc.PutUint32(replicaIDBuf, uint32(req.ReplicaID))
	body = append(body, replicaIDBuf...)

	frame := make([]byte, requestHeaderSize+len(body))
	pos := 0
	enc.PutUint16(frame[pos:pos+apiKeyWidth], ApiFetch)
	pos += apiKeyWidth
	enc.PutUint16(frame[pos:pos+apiVersionWidth], 0)
	pos += apiVersionWidth
	enc.PutUint32(frame[pos:pos+correlationIDWidth], correlationID)
	copy(frame[requestHeaderSize:], body)

	lenBuf := make([]byte, frameLengthWidth)
	enc.PutUint32(lenBuf, uint32(len(frame)))

	if _, err := w.Write(lenBuf); err != nil {
		return err
	}
	_, err := w.Write(frame)
	return err
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

func DecodeFetchResponseHeader(r io.Reader) (FetchResponseHeader, error) {
	var lenBuf [frameLengthWidth]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return FetchResponseHeader{}, err
	}
	totalLength := enc.Uint32(lenBuf[:])
	if totalLength < uint32(responseHeaderSize) {
		return FetchResponseHeader{}, fmt.Errorf("rawdata: fetch response frame too short (%d bytes)", totalLength)
	}

	buf := make([]byte, responseHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return FetchResponseHeader{}, err
	}

	pos := 0
	correlationID := enc.Uint32(buf[pos : pos+correlationIDWidth])
	pos += correlationIDWidth
	code := ErrorCode(enc.Uint16(buf[pos : pos+errorCodeWidth]))
	pos += errorCodeWidth
	hw := enc.Uint64(buf[pos : pos+highWatermarkWidth])
	pos += highWatermarkWidth
	nextOffset := enc.Uint64(buf[pos : pos+nextOffsetWidth])
	pos += nextOffsetWidth
	payloadLen := enc.Uint32(buf[pos : pos+payloadLenWidth])

	return FetchResponseHeader{
		CorrelationID: correlationID,
		Code:          code,
		HighWatermark: hw,
		NextOffset:    nextOffset,
		PayloadLen:    payloadLen,
	}, nil
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
