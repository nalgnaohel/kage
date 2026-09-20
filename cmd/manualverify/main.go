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
	body := make([]byte, 0, 2+len(topic)+4+1+4+len(value))
	topicLen := make([]byte, 2)
	binary.BigEndian.PutUint16(topicLen, uint16(len(topic)))
	body = append(body, topicLen...)
	body = append(body, []byte(topic)...)

	buf4 := make([]byte, 4)
	binary.BigEndian.PutUint32(buf4, uint32(partition))
	body = append(body, buf4...)
	body = append(body, 1)
	binary.BigEndian.PutUint32(buf4, uint32(len(value)))
	body = append(body, buf4...)
	body = append(body, value...)

	header := make([]byte, 8)
	binary.BigEndian.PutUint16(header[0:2], rawdata.ApiProduce)
	binary.BigEndian.PutUint32(header[4:8], 1)
	frame := append(header, body...)

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(frame)))
	if _, err := conn.Write(append(lenBuf, frame...)); err != nil {
		return 0, err
	}

	var respLenBuf [4]byte
	if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
		return 0, err
	}
	resp := make([]byte, binary.BigEndian.Uint32(respLenBuf[:]))
	if _, err := io.ReadFull(conn, resp); err != nil {
		return 0, err
	}
	code := binary.BigEndian.Uint16(resp[4:6])
	if code != uint16(rawdata.ErrNone) {
		return 0, fmt.Errorf("produce error code %d", code)
	}
	return binary.BigEndian.Uint64(resp[6:14]), nil
}

func fetch(conn net.Conn, topic string, partition int32, offset uint64, maxBytes int32) ([]byte, uint64, error) {
	body := make([]byte, 0, 2+len(topic)+4+8+4)
	topicLen := make([]byte, 2)
	binary.BigEndian.PutUint16(topicLen, uint16(len(topic)))
	body = append(body, topicLen...)
	body = append(body, []byte(topic)...)

	buf4 := make([]byte, 4)
	binary.BigEndian.PutUint32(buf4, uint32(partition))
	body = append(body, buf4...)

	buf8 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf8, offset)
	body = append(body, buf8...)

	binary.BigEndian.PutUint32(buf4, uint32(maxBytes))
	body = append(body, buf4...)

	header := make([]byte, 8)
	binary.BigEndian.PutUint16(header[0:2], rawdata.ApiFetch)
	binary.BigEndian.PutUint32(header[4:8], 2)
	frame := append(header, body...)

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(frame)))
	if _, err := conn.Write(append(lenBuf, frame...)); err != nil {
		return nil, 0, err
	}

	var respLenBuf [4]byte
	if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
		return nil, 0, err
	}
	totalLen := binary.BigEndian.Uint32(respLenBuf[:])
	resp := make([]byte, totalLen)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, 0, err
	}

	code := binary.BigEndian.Uint16(resp[4:6])
	if code != uint16(rawdata.ErrNone) {
		return nil, 0, fmt.Errorf("fetch error code %d", code)
	}
	hw := binary.BigEndian.Uint64(resp[6:14])
	payloadLen := binary.BigEndian.Uint32(resp[22:26])
	payload := resp[26 : 26+payloadLen]

	valLen := binary.BigEndian.Uint32(payload[8:12])
	value := payload[12 : 12+int(valLen)]
	return value, hw, nil
}
