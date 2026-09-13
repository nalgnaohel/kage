package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type TopicConfig struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int32
}

func (r *Registry) CreateTopic(topic string, numPartitions int32, replicationFactor int32) error {
	r.mu.Lock()
	if _, exists := r.topics[topic]; exists {
		return fmt.Errorf("topic %s already exists", topic)
	}
	r.mu.Unlock()

	if numPartitions <= 0 {
		return fmt.Errorf("number of partitions must be greater than 0")
	}

	if replicationFactor <= 0 {
		return fmt.Errorf("replication factor must be greater than 0")
	}

	for i := int32(0); i < numPartitions; i++ {
		_, err := r.CreateLog(topic, i)
		if err != nil {
			return fmt.Errorf("failed to create partition %d for topic %s: %w", i, topic, err)
		}
	}

	r.mu.Lock()
	r.topics[topic] = TopicConfig{
		Name:              topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}
	r.mu.Unlock()

	return nil
}

// Derived from r.logs, not r.topics: survives restart since Startup()
// rebuilds r.logs from disk.
func (r *Registry) ListTopics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	topics := make([]string, 0, len(r.logs))
	for topic := range r.logs {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

// r.topics is always populated for anything in r.logs (Startup()/CreateTopic
// both guarantee it), so no fallback construction is needed here.
func (r *Registry) DescribeTopic(topic string) (cfg TopicConfig, partitionIDs []int32, found bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pMap, ok := r.logs[topic]
	if !ok {
		return TopicConfig{}, nil, false
	}

	partitionIDs = make([]int32, 0, len(pMap))
	for p := range pMap {
		partitionIDs = append(partitionIDs, p)
	}
	sort.Slice(partitionIDs, func(i, j int) bool { return partitionIDs[i] < partitionIDs[j] })

	return r.topics[topic], partitionIDs, true
}

// Synchronous and irreversible — fine at single-broker scale; revisit if
// deletes ever need to be async (large topics, or fan-out once replicated).
func (r *Registry) DeleteTopic(topic string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	pMap, exists := r.logs[topic]
	if !exists {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	for partition, l := range pMap {
		if err := l.Close(); err != nil {
			return fmt.Errorf("failed to close partition %d for topic %s: %w", partition, topic, err)
		}

		dirName := fmt.Sprintf("%s-%d", topic, partition)
		partitionPath := filepath.Join(r.baseDir, dirName)
		if err := os.RemoveAll(partitionPath); err != nil {
			return fmt.Errorf("failed to remove partition %d for topic %s: %w", partition, topic, err)
		}
	}

	delete(r.logs, topic)
	delete(r.topics, topic)

	return nil
}
