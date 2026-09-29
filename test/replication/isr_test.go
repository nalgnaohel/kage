package replication_test

import (
	"slices"
	"testing"
	"time"

	"github.com/nalgnaohel/kage/raft"
	"github.com/nalgnaohel/kage/replication"
)

type hwSnapshot struct {
	HighWatermark uint64
	LogEndOffset  uint64
}

type hwStage struct {
	Stage         string
	HighWatermark uint64
	LogEndOffset  uint64
}

func bootstrapLeaderNode(t *testing.T, brokerID int32) *raft.Node {
	t.Helper()

	node := newTestNode(t, brokerID)
	if err := node.Bootstrap(); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	return node
}

func TestLeaderTracker_HighWatermarkAdvancesAfterReplicaFetch(t *testing.T) {
	node := bootstrapLeaderNode(t, 1)
	registerBroker(t, node, 1)
	registerBroker(t, node, 2)
	createTopic(t, node, "topic-x", 1, 2)

	reg, log := newRegistryLog(t, "topic-x", 0)
	for _, v := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		if _, err := log.Append(v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	tracker := replication.NewLeaderTracker(node, reg)
	tracker.OnReplicaFetch("topic-x", 0, 2, 2)

	assertGolden(t, "isr", hwSnapshot{
		HighWatermark: log.HighWatermark(),
		LogEndOffset:  log.LogEndOffset(),
	})
}

func TestLeaderTracker_IgnoresNonLeaderBroker(t *testing.T) {
	node := bootstrapLeaderNode(t, 1)
	registerBroker(t, node, 0)
	registerBroker(t, node, 1)
	createTopic(t, node, "topic-y", 1, 2)

	reg, log := newRegistryLog(t, "topic-y", 0)
	if _, err := log.Append([]byte("a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := log.Append([]byte("b")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	tracker := replication.NewLeaderTracker(node, reg)
	tracker.OnReplicaFetch("topic-y", 0, 99, 2)

	assertGolden(t, "isr", hwSnapshot{
		HighWatermark: log.HighWatermark(),
		LogEndOffset:  log.LogEndOffset(),
	})
}

func TestLeaderTracker_StaleFollowerCapsHighWatermark(t *testing.T) {
	node := bootstrapLeaderNode(t, 1)
	registerBroker(t, node, 1)
	registerBroker(t, node, 2)
	registerBroker(t, node, 3)
	createTopic(t, node, "topic-z", 1, 3)

	reg, log := newRegistryLog(t, "topic-z", 0)
	for _, v := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")} {
		if _, err := log.Append(v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	tracker := replication.NewLeaderTracker(node, reg)
	tracker.OnReplicaFetch("topic-z", 0, 2, 5) // follower 2 fully caught up
	// follower 3 never calls OnReplicaFetch at all

	assertGolden(t, "isr", hwSnapshot{
		HighWatermark: log.HighWatermark(),
		LogEndOffset:  log.LogEndOffset(),
	})
}

func TestLeaderTracker_DeadFollowerFreezesHighWatermark(t *testing.T) {
	node := bootstrapLeaderNode(t, 1)
	registerBroker(t, node, 1)
	registerBroker(t, node, 2)
	registerBroker(t, node, 3)
	createTopic(t, node, "topic-w", 1, 3)

	reg, log := newRegistryLog(t, "topic-w", 0)
	tracker := replication.NewLeaderTracker(node, reg)

	var stages []hwStage

	for _, v := range [][]byte{[]byte("a"), []byte("b")} {
		if _, err := log.Append(v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	tracker.OnReplicaFetch("topic-w", 0, 2, 2)
	tracker.OnReplicaFetch("topic-w", 0, 3, 2)
	stages = append(stages, hwStage{Stage: "both-caught-up", HighWatermark: log.HighWatermark(), LogEndOffset: log.LogEndOffset()})

	for _, v := range [][]byte{[]byte("c"), []byte("d"), []byte("e")} {
		if _, err := log.Append(v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	tracker.OnReplicaFetch("topic-w", 0, 2, 5) // follower 2 keeps up; follower 3 goes silent from here on
	stages = append(stages, hwStage{Stage: "follower3-stale", HighWatermark: log.HighWatermark(), LogEndOffset: log.LogEndOffset()})

	for _, v := range [][]byte{[]byte("f"), []byte("g")} {
		if _, err := log.Append(v); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	tracker.OnReplicaFetch("topic-w", 0, 2, 7) // follower 2 still keeping up; follower 3 still silent
	stages = append(stages, hwStage{Stage: "follower3-still-stale", HighWatermark: log.HighWatermark(), LogEndOffset: log.LogEndOffset()})

	assertGolden(t, "isr", stages)
}

func TestLeaderTracker_Run_ShrinksThenExpandsISR(t *testing.T) {
	node := bootstrapLeaderNode(t, 1)
	registerBroker(t, node, 1)
	registerBroker(t, node, 2)
	createTopic(t, node, "topic-shrink", 1, 2)

	reg, _ := newRegistryLog(t, "topic-shrink", 0)

	tracker := replication.NewLeaderTracker(node, reg,
		replication.WithReplicaLagTimeout(80*time.Millisecond),
		replication.WithISRCheckInterval(20*time.Millisecond),
	)

	go tracker.Run(t.Context())

	currentISR := func() []int32 {
		return node.FSM().State().Topics["topic-shrink"].Partitions[0].Isr
	}

	waitFor(t, time.Second, func() bool { return slices.Equal(currentISR(), []int32{1}) })

	tracker.OnReplicaFetch("topic-shrink", 0, 2, 0)
	waitFor(t, time.Second, func() bool { return slices.Equal(currentISR(), []int32{1, 2}) })
}

func TestLeaderTracker_WaitForHW_UnblocksOnceStaleFollowerDropsFromISR(t *testing.T) {
	node := bootstrapLeaderNode(t, 1)
	registerBroker(t, node, 1)
	registerBroker(t, node, 2)
	registerBroker(t, node, 3)
	createTopic(t, node, "topic-wait", 1, 3)

	reg, log := newRegistryLog(t, "topic-wait", 0)
	if _, err := log.Append([]byte("a")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	tracker := replication.NewLeaderTracker(node, reg,
		replication.WithReplicaLagTimeout(80*time.Millisecond),
		replication.WithISRCheckInterval(20*time.Millisecond),
	)
	go tracker.Run(t.Context())

	stopFollower2 := make(chan struct{})
	defer close(stopFollower2)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopFollower2:
				return
			case <-ticker.C:
				tracker.OnReplicaFetch("topic-wait", 0, 2, 1)
			}
		}
	}()

	currentISR := func() []int32 {
		return node.FSM().State().Topics["topic-wait"].Partitions[0].Isr
	}
	waitFor(t, time.Second, func() bool { return slices.Equal(currentISR(), []int32{1, 2}) })

	if err := tracker.WaitForHW("topic-wait", 0, 1, time.Second); err != nil {
		t.Fatalf("WaitForHW: %v", err)
	}
}
