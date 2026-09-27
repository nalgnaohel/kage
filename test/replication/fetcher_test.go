package replication_test

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/nalgnaohel/kage/api/rawdata"
	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/replication"
	"github.com/nalgnaohel/kage/storage"
)

func TestFetcher_HappyPath_ConvergesWithLeader(t *testing.T) {
	reg, leaderLog := newRegistryLog(t, "topic-a", 0)
	leaderAddr, _ := startRawServer(t, reg, nil)

	values := [][]byte{[]byte("hello"), []byte("world"), []byte("foo")}
	for _, v := range values {
		if _, err := leaderLog.Append(v); err != nil {
			t.Fatalf("leader Append: %v", err)
		}
	}

	_, followerLog := newRegistryLog(t, "topic-a", 0)
	fetcher := replication.NewFetcher("topic-a", 0, 2, leaderAddr, followerLog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fetcher.Run(ctx)

	waitFor(t, 2*time.Second, func() bool { return followerLog.LogEndOffset() == uint64(len(values)) })

	assertGolden(t, "fetcher", logRecords(t, followerLog, len(values)))
}

func TestFetcher_CatchesUpOnNewlyProducedRecords(t *testing.T) {
	reg, leaderLog := newRegistryLog(t, "topic-b", 0)
	leaderAddr, _ := startRawServer(t, reg, nil)

	if _, err := leaderLog.Append([]byte("one")); err != nil {
		t.Fatalf("leader Append: %v", err)
	}

	_, followerLog := newRegistryLog(t, "topic-b", 0)
	fetcher := replication.NewFetcher("topic-b", 0, 2, leaderAddr, followerLog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fetcher.Run(ctx)

	waitFor(t, 2*time.Second, func() bool { return followerLog.LogEndOffset() == 1 })

	if _, err := leaderLog.Append([]byte("two")); err != nil {
		t.Fatalf("leader Append: %v", err)
	}
	if _, err := leaderLog.Append([]byte("three")); err != nil {
		t.Fatalf("leader Append: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool { return followerLog.LogEndOffset() == 3 })

	assertGolden(t, "fetcher", logRecords(t, followerLog, 3))
}

func TestFetcher_ResumesFromExistingLocalOffset(t *testing.T) {
	reg, leaderLog := newRegistryLog(t, "topic-c", 0)
	values := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	for _, v := range values {
		if _, err := leaderLog.Append(v); err != nil {
			t.Fatalf("leader Append: %v", err)
		}
	}
	leaderAddr, _ := startRawServer(t, reg, nil)

	_, followerLog := newRegistryLog(t, "topic-c", 0)
	if _, err := followerLog.Append(values[0]); err != nil {
		t.Fatalf("follower pre-seed Append: %v", err)
	}

	fetcher := replication.NewFetcher("topic-c", 0, 2, leaderAddr, followerLog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fetcher.Run(ctx)

	waitFor(t, 2*time.Second, func() bool { return followerLog.LogEndOffset() == uint64(len(values)) })

	assertGolden(t, "fetcher", logRecords(t, followerLog, len(values)))
}

func TestFetcher_StopsOnContextCancel(t *testing.T) {
	reg, _ := newRegistryLog(t, "topic-d", 0)
	leaderAddr, _ := startRawServer(t, reg, nil)

	_, followerLog := newRegistryLog(t, "topic-d", 0)
	fetcher := replication.NewFetcher("topic-d", 0, 2, leaderAddr, followerLog)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		fetcher.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Fetcher.Run did not stop after context cancellation")
	}
}

func TestFetcher_TriggersOnReplicaFetchHook(t *testing.T) {
	type call struct {
		Topic       string
		Partition   int32
		ReplicaID   int32
		FetchOffset uint64
	}
	calls := make(chan call, 10)

	reg, leaderLog := newRegistryLog(t, "topic-e", 0)
	if _, err := leaderLog.Append([]byte("x")); err != nil {
		t.Fatalf("leader Append: %v", err)
	}
	leaderAddr, _ := startRawServer(t, reg, func(topic string, partition int32, replicaID int32, fetchOffset uint64) {
		calls <- call{Topic: topic, Partition: partition, ReplicaID: replicaID, FetchOffset: fetchOffset}
	})

	_, followerLog := newRegistryLog(t, "topic-e", 0)
	fetcher := replication.NewFetcher("topic-e", 0, 7, leaderAddr, followerLog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fetcher.Run(ctx)

	select {
	case got := <-calls:
		assertGolden(t, "fetcher", got)
	case <-time.After(2 * time.Second):
		t.Fatal("OnReplicaFetch was never called")
	}
}

func TestFetcher_RetriesUntilLeaderComesUp(t *testing.T) {
	port := freePort(t)
	leaderAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

	_, followerLog := newRegistryLog(t, "topic-f", 0)
	fetcher := replication.NewFetcher("topic-f", 0, 2, leaderAddr, followerLog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fetcher.Run(ctx)

	time.Sleep(1200 * time.Millisecond)

	ln, err := net.Listen("tcp", leaderAddr)
	if err != nil {
		t.Fatalf("listen on %s: %v", leaderAddr, err)
	}
	t.Cleanup(func() { ln.Close() })

	reg := broker.NewRegistry(t.TempDir(), storage.DefaultLogConfig())
	if err := reg.Startup(); err != nil {
		t.Fatalf("registry startup: %v", err)
	}
	leaderLog, err := reg.CreateLog("topic-f", 0)
	if err != nil {
		t.Fatalf("CreateLog: %v", err)
	}
	go rawdata.NewServer(reg).Serve(ln)

	if _, err := leaderLog.Append([]byte("late-leader")); err != nil {
		t.Fatalf("leader Append: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool { return followerLog.LogEndOffset() == 1 })

	assertGolden(t, "fetcher", logRecords(t, followerLog, 1))
}

func TestFetcher_RetriesOnLeaderErrorCode(t *testing.T) {
	reg := newRegistry(t)
	leaderAddr, _ := startRawServer(t, reg, nil)

	_, followerLog := newRegistryLog(t, "topic-l", 0)
	fetcher := replication.NewFetcher("topic-l", 0, 2, leaderAddr, followerLog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fetcher.Run(ctx)

	time.Sleep(1200 * time.Millisecond)

	leaderLog, err := reg.CreateLog("topic-l", 0)
	if err != nil {
		t.Fatalf("CreateLog: %v", err)
	}
	if _, err := leaderLog.Append([]byte("caught-up")); err != nil {
		t.Fatalf("leader Append: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool { return followerLog.LogEndOffset() == 1 })

	assertGolden(t, "fetcher", logRecords(t, followerLog, 1))
}

func TestFetcher_TruncatedPayload_DoesNotCorruptLocalLog(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if _, _, err := rawdata.ReadRequest(c); err != nil {
					return
				}
				// Claim a 100-byte payload but only ever write 5 bytes of it.
				rawdata.EncodeFetchResponseHeader(c, 1, rawdata.ErrNone, 0, 0, 100)
				c.Write([]byte("short"))
			}(conn)
		}
	}()

	_, followerLog := newRegistryLog(t, "topic-m", 0)
	fetcher := replication.NewFetcher("topic-m", 0, 2, ln.Addr().String(), followerLog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fetcher.Run(ctx)

	time.Sleep(300 * time.Millisecond)

	assertGolden(t, "fetcher", logRecords(t, followerLog, int(followerLog.LogEndOffset())))
}

func TestFetcher_ReconnectsAfterLeaderConnectionDrops(t *testing.T) {
	reg, leaderLog := newRegistryLog(t, "topic-k", 0)
	if _, err := leaderLog.Append([]byte("one")); err != nil {
		t.Fatalf("leader Append: %v", err)
	}
	leaderAddr, drop := startRawServer(t, reg, nil)

	_, followerLog := newRegistryLog(t, "topic-k", 0)
	fetcher := replication.NewFetcher("topic-k", 0, 2, leaderAddr, followerLog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fetcher.Run(ctx)

	waitFor(t, 2*time.Second, func() bool { return followerLog.LogEndOffset() == 1 })

	drop()

	if _, err := leaderLog.Append([]byte("two")); err != nil {
		t.Fatalf("leader Append: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool { return followerLog.LogEndOffset() == 2 })

	assertGolden(t, "fetcher", logRecords(t, followerLog, 2))
}
