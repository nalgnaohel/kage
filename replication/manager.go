package replication

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/raft"
	"github.com/nalgnaohel/kage/storage"
)

const fetcherManagerInterval = 5 * time.Second

// FetcherManager mirrors raft.Reconciler's diff-and-converge loop: it starts
// one Fetcher per partition this broker replicates but doesn't lead, and
// stops any Fetcher whose partition is no longer assigned here.
type FetcherManager struct {
	node     *raft.Node
	registry *broker.Registry

	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

func NewFetcherManager(node *raft.Node, registry *broker.Registry) *FetcherManager {
	return &FetcherManager{
		node:     node,
		registry: registry,
		cancels:  make(map[string]context.CancelFunc),
	}
}

func (m *FetcherManager) Run(ctx context.Context) {
	ticker := time.NewTicker(fetcherManagerInterval)
	defer ticker.Stop()

	m.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			return
		case <-m.node.FSM().VersionCh():
			m.reconcile(ctx)
		case <-ticker.C:
			m.reconcile(ctx)
		}
	}
}

func (m *FetcherManager) reconcile(ctx context.Context) {
	state := m.node.FSM().State()
	brokerID := m.node.BrokerID()
	wanted := make(map[string]bool)

	for topic, meta := range state.Topics {
		for partition, assignment := range meta.Partitions {
			if len(assignment.Replicas) == 0 || assignment.Replicas[0] == brokerID {
				continue
			}
			if !slices.Contains(assignment.Replicas, brokerID) {
				continue
			}

			localLog, ok := m.registry.GetLog(topic, partition)
			if !ok {
				continue
			}
			leader, ok := state.Brokers[assignment.Replicas[0]]
			if !ok || leader.RawAddr == "" {
				continue
			}

			key := partitionKey(topic, partition)
			wanted[key] = true
			m.startIfMissing(ctx, key, topic, partition, brokerID, leader.RawAddr, localLog)
		}
	}

	m.stopUnwanted(wanted)
}

func (m *FetcherManager) startIfMissing(ctx context.Context, key, topic string, partition, replicaID int32, leaderAddr string, localLog *storage.Log) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, running := m.cancels[key]; running {
		return
	}

	fctx, cancel := context.WithCancel(ctx)
	m.cancels[key] = cancel
	fetcher := NewFetcher(topic, partition, replicaID, leaderAddr, localLog)
	go fetcher.Run(fctx)
}

func (m *FetcherManager) stopUnwanted(wanted map[string]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, cancel := range m.cancels {
		if !wanted[key] {
			cancel()
			delete(m.cancels, key)
		}
	}
}

func (m *FetcherManager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, cancel := range m.cancels {
		cancel()
		delete(m.cancels, key)
	}
}

