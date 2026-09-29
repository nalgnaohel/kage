package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	hraft "github.com/hashicorp/raft"
)

type BrokerInfo struct {
	BrokerID int32
	Host     string
	Port     int32
	RaftAddr string
	RawAddr  string
}

type PartitionAssignment struct {
	Replicas []int32
	Isr      []int32
}

type TopicMeta struct {
	NumPartitions     int32
	ReplicationFactor int32
	Partitions        map[int32]PartitionAssignment
}

type State struct {
	Brokers map[int32]BrokerInfo
	Topics  map[string]TopicMeta
}

func newState() State {
	return State{
		Brokers: make(map[int32]BrokerInfo),
		Topics:  make(map[string]TopicMeta),
	}
}

func (s State) clone() State {
	out := newState()
	maps.Copy(out.Brokers, s.Brokers)
	for topic, meta := range s.Topics {
		partitions := make(map[int32]PartitionAssignment, len(meta.Partitions))
		for p, a := range meta.Partitions {
			partitions[p] = PartitionAssignment{Replicas: slices.Clone(a.Replicas), Isr: slices.Clone(a.Isr)}
		}
		meta.Partitions = partitions
		out.Topics[topic] = meta
	}
	return out
}

type FSM struct {
	mu        sync.RWMutex
	state     State
	version   uint64
	versionCh chan struct{}
}

func NewFSM() *FSM {
	return &FSM{
		state:     newState(),
		versionCh: make(chan struct{}, 1),
	}
}

func (f *FSM) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.clone()
}

func (f *FSM) VersionCh() <-chan struct{} {
	return f.versionCh
}

func (f *FSM) signal() {
	select {
	case f.versionCh <- struct{}{}:
	default:
	}
}

func (f *FSM) Apply(l *hraft.Log) any {
	cmd, err := Decode(l.Data)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var applyErr error
	switch cmd.Type {
	case CmdRegisterBroker:
		applyErr = f.applyRegisterBroker(cmd)
	case CmdCreateTopic:
		applyErr = f.applyCreateTopic(cmd)
	case CmdDeleteTopic:
		applyErr = f.applyDeleteTopic(cmd)
	case CmdUpdateISR:
		applyErr = f.applyUpdateISR(cmd)
	default:
		applyErr = fmt.Errorf("raft: unknown command type %q", cmd.Type)
	}

	if applyErr != nil {
		return applyErr
	}

	f.version++
	f.signal()
	return nil
}

func (f *FSM) applyRegisterBroker(cmd Command) error {
	payload, err := cmd.DecodeRegisterBroker()
	if err != nil {
		return err
	}
	f.state.Brokers[payload.BrokerID] = BrokerInfo{
		BrokerID: payload.BrokerID,
		Host:     payload.Host,
		Port:     payload.Port,
		RaftAddr: payload.RaftAddr,
		RawAddr:  payload.RawAddr,
	}
	return nil
}

func (f *FSM) applyCreateTopic(cmd Command) error {
	payload, err := cmd.DecodeCreateTopic()
	if err != nil {
		return err
	}
	if payload.NumPartitions <= 0 {
		return fmt.Errorf("raft: number of partitions must be greater than 0")
	}
	if payload.ReplicationFactor <= 0 {
		return fmt.Errorf("raft: replication factor must be greater than 0")
	}
	if _, exists := f.state.Topics[payload.Topic]; exists {
		return fmt.Errorf("raft: topic %q already exists", payload.Topic)
	}
	f.state.Topics[payload.Topic] = TopicMeta{
		NumPartitions:     payload.NumPartitions,
		ReplicationFactor: payload.ReplicationFactor,
		Partitions:        f.placePartitions(payload.NumPartitions, payload.ReplicationFactor),
	}
	return nil
}

func (f *FSM) placePartitions(numPartitions, replicationFactor int32) map[int32]PartitionAssignment {
	partitions := make(map[int32]PartitionAssignment, numPartitions)

	sorted := make([]int32, 0, len(f.state.Brokers))
	for id := range f.state.Brokers {
		sorted = append(sorted, id)
	}
	slices.Sort(sorted)

	replicaCount := min(int(replicationFactor), len(sorted))

	for p := range numPartitions {
		replicas := make([]int32, replicaCount)
		for i := range replicaCount {
			replicas[i] = sorted[(int(p)+i)%len(sorted)]
		}
		partitions[p] = PartitionAssignment{Replicas: replicas, Isr: slices.Clone(replicas)}
	}
	return partitions
}

func (f *FSM) applyDeleteTopic(cmd Command) error {
	payload, err := cmd.DecodeDeleteTopic()
	if err != nil {
		return err
	}
	if _, exists := f.state.Topics[payload.Topic]; !exists {
		return fmt.Errorf("raft: topic %q does not exist", payload.Topic)
	}
	delete(f.state.Topics, payload.Topic)
	return nil
}

func (f *FSM) applyUpdateISR(cmd Command) error {
	payload, err := cmd.DecodeUpdateISR()
	if err != nil {
		return err
	}
	meta, ok := f.state.Topics[payload.Topic]
	if !ok {
		return fmt.Errorf("raft: topic %q does not exist", payload.Topic)
	}
	assignment, ok := meta.Partitions[payload.Partition]
	if !ok {
		return fmt.Errorf("raft: topic %q has no partition %d", payload.Topic, payload.Partition)
	}
	for _, id := range payload.ISR {
		if !containsReplica(assignment.Replicas, id) {
			return fmt.Errorf("raft: broker %d is not a replica of %s/%d", id, payload.Topic, payload.Partition)
		}
	}
	assignment.Isr = slices.Clone(payload.ISR)
	meta.Partitions[payload.Partition] = assignment
	return nil
}

func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &fsmSnapshot{state: f.state.clone()}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var state State
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	if state.Brokers == nil {
		state.Brokers = make(map[int32]BrokerInfo)
	}
	if state.Topics == nil {
		state.Topics = make(map[string]TopicMeta)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = state
	return nil
}

type fsmSnapshot struct {
	state State
}

func (s *fsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
