package replication

import (
	"context"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/raft"
)

const (
	replicaLagTimeout = 10 * time.Second
	isrCheckInterval  = 5 * time.Second
	proposeTimeout    = 5 * time.Second
)

type followerState struct {
	offset    uint64
	lastFetch time.Time
}

type LeaderTracker struct {
	node     *raft.Node
	registry *broker.Registry

	replicaLagTimeout time.Duration
	isrCheckInterval  time.Duration

	mu        sync.Mutex
	followers map[string]map[int32]followerState
}

type LeaderTrackerOption func(*LeaderTracker)

func WithReplicaLagTimeout(d time.Duration) LeaderTrackerOption {
	return func(t *LeaderTracker) { t.replicaLagTimeout = d }
}

func WithISRCheckInterval(d time.Duration) LeaderTrackerOption {
	return func(t *LeaderTracker) { t.isrCheckInterval = d }
}

func NewLeaderTracker(node *raft.Node, registry *broker.Registry, opts ...LeaderTrackerOption) *LeaderTracker {
	t := &LeaderTracker{
		node:              node,
		registry:          registry,
		replicaLagTimeout: replicaLagTimeout,
		isrCheckInterval:  isrCheckInterval,
		followers:         make(map[string]map[int32]followerState),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

func (t *LeaderTracker) OnReplicaFetch(topic string, partition int32, replicaID int32, fetchOffset uint64) {
	key := partitionKey(topic, partition)

	t.mu.Lock()
	if t.followers[key] == nil {
		t.followers[key] = make(map[int32]followerState)
	}
	t.followers[key][replicaID] = followerState{offset: fetchOffset, lastFetch: time.Now()}
	t.mu.Unlock()

	t.recomputeHW(topic, partition)
}

func (t *LeaderTracker) Run(ctx context.Context) {
	ticker := time.NewTicker(t.isrCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.reconcileISR()
		}
	}
}

func (t *LeaderTracker) reconcileISR() {
	state := t.node.FSM().State()
	brokerID := t.node.BrokerID()

	for topic, meta := range state.Topics {
		for partition, assignment := range meta.Partitions {
			if len(assignment.Replicas) == 0 || assignment.Replicas[0] != brokerID {
				continue
			}
			t.recomputeISR(topic, partition, assignment)
		}
	}
}

func (t *LeaderTracker) recomputeISR(topic string, partition int32, assignment raft.PartitionAssignment) {
	key := partitionKey(topic, partition)
	now := time.Now()
	brokerID := t.node.BrokerID()

	t.mu.Lock()
	if t.followers[key] == nil {
		t.followers[key] = make(map[int32]followerState)
	}
	followers := t.followers[key]
	newISR := []int32{brokerID}
	for _, id := range assignment.Replicas[1:] {
		fs, ok := followers[id]
		if !ok {
			fs = followerState{lastFetch: now}
			followers[id] = fs
		}
		if now.Sub(fs.lastFetch) <= t.replicaLagTimeout {
			newISR = append(newISR, id)
		}
	}
	t.mu.Unlock()

	if slices.Equal(newISR, assignment.Isr) {
		return
	}

	cmd, err := raft.NewUpdateISRCommand(raft.UpdateISRCommand{Topic: topic, Partition: partition, ISR: newISR})
	if err != nil {
		log.Printf("replication: encode UpdateISR for %s-%d: %v", topic, partition, err)
		return
	}
	if _, err := t.node.Propose(cmd, proposeTimeout); err != nil {
		log.Printf("replication: propose UpdateISR for %s-%d: %v", topic, partition, err)
	}
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
	followers := t.followers[partitionKey(topic, partition)]
	for _, id := range assignment.Isr {
		if id == t.node.BrokerID() {
			continue
		}
		if off := followers[id].offset; off < hw {
			hw = off
		}
	}
	t.mu.Unlock()

	l.SetHighWatermark(hw)
}
