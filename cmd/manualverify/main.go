package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/nalgnaohel/kage/api/rpc/kadmin"
	"github.com/nalgnaohel/kage/api/rawdata"
)

const topic = "manual-verify"

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

func main() {
	conn, err := grpc.NewClient("127.0.0.1:9093", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	admin := pb.NewKafkaAdminClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createResp, err := admin.CreateTopic(ctx, &pb.CreateTopicRequest{
		TopicName:         topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("CreateTopic: success=%v message=%q\n", createResp.Success, createResp.Message)

	rawConn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		panic(err)
	}
	defer rawConn.Close()

	value := []byte("hello-zero-copy")
	baseOffset, err := produce(rawConn, topic, 0, value)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Produce: baseOffset=%d\n", baseOffset)

	got, hw, err := fetch(rawConn, topic, 0, baseOffset, 1<<16)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Fetch: highWatermark=%d value=%q match=%v\n", hw, got, string(got) == string(value))
}

func produce(conn net.Conn, topic string, partition int32, value []byte) (uint64, error) {
	body := make([]byte, 0, topicLenWidth+len(topic)+partitionWidth+requiredAcksWidth+valueLenWidth+len(value))
	topicLenBuf := make([]byte, topicLenWidth)
	binary.BigEndian.PutUint16(topicLenBuf, uint16(len(topic)))
	body = append(body, topicLenBuf...)
	body = append(body, []byte(topic)...)

	partitionBuf := make([]byte, partitionWidth)
	binary.BigEndian.PutUint32(partitionBuf, uint32(partition))
	body = append(body, partitionBuf...)
	body = append(body, 1)

	valueLenBuf := make([]byte, valueLenWidth)
	binary.BigEndian.PutUint32(valueLenBuf, uint32(len(value)))
	body = append(body, valueLenBuf...)
	body = append(body, value...)

	header := make([]byte, requestHeaderSize)
	pos := 0
	binary.BigEndian.PutUint16(header[pos:pos+apiKeyWidth], rawdata.ApiProduce)
	pos += apiKeyWidth + apiVersionWidth
	binary.BigEndian.PutUint32(header[pos:pos+correlationIDWidth], 1)
	frame := append(header, body...)

	lenBuf := make([]byte, frameLengthWidth)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(frame)))
	if _, err := conn.Write(append(lenBuf, frame...)); err != nil {
		return 0, err
	}

	var respLenBuf [frameLengthWidth]byte
	if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
		return 0, err
	}
	resp := make([]byte, binary.BigEndian.Uint32(respLenBuf[:]))
	if _, err := io.ReadFull(conn, resp); err != nil {
		return 0, err
	}
	pos = correlationIDWidth
	code := binary.BigEndian.Uint16(resp[pos : pos+errorCodeWidth])
	if code != uint16(rawdata.ErrNone) {
		return 0, fmt.Errorf("produce error code %d", code)
	}
	pos += errorCodeWidth
	return binary.BigEndian.Uint64(resp[pos : pos+baseOffsetWidth]), nil
}

func fetch(conn net.Conn, topic string, partition int32, offset uint64, maxBytes int32) ([]byte, uint64, error) {
	body := make([]byte, 0, topicLenWidth+len(topic)+partitionWidth+fetchOffsetWidth+maxBytesWidth+replicaIDWidth)
	topicLenBuf := make([]byte, topicLenWidth)
	binary.BigEndian.PutUint16(topicLenBuf, uint16(len(topic)))
	body = append(body, topicLenBuf...)
	body = append(body, []byte(topic)...)

	partitionBuf := make([]byte, partitionWidth)
	binary.BigEndian.PutUint32(partitionBuf, uint32(partition))
	body = append(body, partitionBuf...)

	fetchOffsetBuf := make([]byte, fetchOffsetWidth)
	binary.BigEndian.PutUint64(fetchOffsetBuf, offset)
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
	pos += apiKeyWidth + apiVersionWidth
	binary.BigEndian.PutUint32(header[pos:pos+correlationIDWidth], 2)
	frame := append(header, body...)

	lenBuf := make([]byte, frameLengthWidth)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(frame)))
	if _, err := conn.Write(append(lenBuf, frame...)); err != nil {
		return nil, 0, err
	}

	var respLenBuf [frameLengthWidth]byte
	if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
		return nil, 0, err
	}
	totalLen := binary.BigEndian.Uint32(respLenBuf[:])
	resp := make([]byte, totalLen)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, 0, err
	}

	pos = correlationIDWidth
	code := binary.BigEndian.Uint16(resp[pos : pos+errorCodeWidth])
	if code != uint16(rawdata.ErrNone) {
		return nil, 0, fmt.Errorf("fetch error code %d", code)
	}
	pos += errorCodeWidth
	hw := binary.BigEndian.Uint64(resp[pos : pos+highWatermarkWidth])
	pos += highWatermarkWidth + nextOffsetWidth
	payloadLen := binary.BigEndian.Uint32(resp[pos : pos+payloadLenWidth])
	pos += payloadLenWidth
	payload := resp[pos : pos+int(payloadLen)]

	valLen := binary.BigEndian.Uint32(payload[recordOffsetWidth : recordOffsetWidth+recordLenWidth])
	value := payload[recordOffsetWidth+recordLenWidth : recordOffsetWidth+recordLenWidth+int(valLen)]
	return value, hw, nil
}
