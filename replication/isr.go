package replication

import (
	"fmt"
	"sync"

	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/raft"
)

type LeaderTracker struct {
	node     *raft.Node
	registry *broker.Registry

	mu          sync.Mutex
	lastFetched map[string]map[int32]uint64
}

func NewLeaderTracker(node *raft.Node, registry *broker.Registry) *LeaderTracker {
	return &LeaderTracker{
		node:        node,
		registry:    registry,
		lastFetched: make(map[string]map[int32]uint64),
	}
}

func partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

func (t *LeaderTracker) OnReplicaFetch(topic string, partition int32, replicaID int32, fetchOffset uint64) {
	key := partitionKey(topic, partition)

	t.mu.Lock()
	if t.lastFetched[key] == nil {
		t.lastFetched[key] = make(map[int32]uint64)
	}
	t.lastFetched[key][replicaID] = fetchOffset
	t.mu.Unlock()

	t.recomputeHW(topic, partition)
}

func (t *LeaderTracker) recomputeHW(topic string, partition int32) {
	state := t.node.FSM().State()

	meta, ok := state.Topics[topic]
	if !ok {
		return
	}
	assignment, ok := meta.Partitions[partition]
	if !ok || len(assignment.Replicas) == 0 || assignment.Replicas[0] != t.node.BrokerID() {
		return
	}

	l, ok := t.registry.GetLog(topic, partition)
	if !ok {
		return
	}

	hw := l.LogEndOffset()

	t.mu.Lock()
	fetched := t.lastFetched[partitionKey(topic, partition)]
	for _, followerID := range assignment.Replicas[1:] {
		if fetched[followerID] < hw {
			hw = fetched[followerID]
		}
	}
	t.mu.Unlock()

	l.SetHighWatermark(hw)
}
