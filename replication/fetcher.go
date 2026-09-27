package replication

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/nalgnaohel/kage/api/rawdata"
	"github.com/nalgnaohel/kage/storage"
)

const (
	fetchMaxBytes        = 1 << 20
	fetcherPollInterval  = 200 * time.Millisecond
	fetcherRetryInterval = time.Second
)

type Fetcher struct {
	topic      string
	partition  int32
	replicaID  int32
	leaderAddr string
	localLog   *storage.Log
}

func NewFetcher(topic string, partition int32, replicaID int32, leaderAddr string, localLog *storage.Log) *Fetcher {
	return &Fetcher{
		topic:      topic,
		partition:  partition,
		replicaID:  replicaID,
		leaderAddr: leaderAddr,
		localLog:   localLog,
	}
}

func (f *Fetcher) Run(ctx context.Context) {
	var conn net.Conn
	var correlationID uint32
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		if conn == nil {
			c, err := net.Dial("tcp", f.leaderAddr)
			if err != nil {
				log.Printf("replication: dial leader %s for %s-%d: %v", f.leaderAddr, f.topic, f.partition, err)
				if !sleepCtx(ctx, fetcherRetryInterval) {
					return
				}
				continue
			}
			conn = c
		}

		correlationID++
		result, err := FetchFrom(conn, correlationID, rawdata.FetchRequest{
			Topic:       f.topic,
			Partition:   f.partition,
			FetchOffset: f.localLog.LogEndOffset(),
			MaxBytes:    fetchMaxBytes,
			ReplicaID:   f.replicaID,
		})
		if err != nil {
			log.Printf("replication: fetch %s-%d from %s: %v", f.topic, f.partition, f.leaderAddr, err)
			conn.Close()
			conn = nil
			if !sleepCtx(ctx, fetcherRetryInterval) {
				return
			}
			continue
		}

		if len(result.Records) == 0 {
			if !sleepCtx(ctx, fetcherPollInterval) {
				return
			}
			continue
		}

		for _, rec := range result.Records {
			if _, err := f.localLog.Append(rec.Value); err != nil {
				log.Printf("replication: append to %s-%d: %v", f.topic, f.partition, err)
				break
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
