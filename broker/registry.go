package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nalgnaohel/kage/storage"
)

type Registry struct {
	mu      sync.RWMutex
	baseDir string
	config  storage.LogConfig
	topics  map[string]TopicConfig

	// logs: topic -> partition -> Log
	logs map[string]map[int32]*storage.Log

	// flushers: topic -> partition -> Flusher
	flushers map[string]map[int32]*Flusher

	// Immutable after construction (no lock needed to read). Single-broker
	// only until Raft/KRaft cluster membership exists (Phase 2).
	clusterID string
	brokerID  int32
	host      string
	port      int32
}

func NewRegistry(baseDir string, cfg storage.LogConfig, clusterID string, brokerID int32, host string, port int32) *Registry {
	return &Registry{
		baseDir:   baseDir,
		config:    cfg,
		logs:      make(map[string]map[int32]*storage.Log),
		flushers:  make(map[string]map[int32]*Flusher),
		topics:    make(map[string]TopicConfig),
		clusterID: clusterID,
		brokerID:  brokerID,
		host:      host,
		port:      port,
	}
}

func (r *Registry) Startup() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := os.MkdirAll(r.baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	entries, err := os.ReadDir(r.baseDir)
	if err != nil {
		return fmt.Errorf("failed to read base directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		dirName := entry.Name()

		// split on the last dash: topic names may contain dashes
		lastDash := strings.LastIndex(dirName, "-")
		if lastDash == -1 {
			continue
		}

		topic := dirName[:lastDash]
		pStr := dirName[lastDash+1:]

		partition, err := strconv.Atoi(pStr)
		if err != nil {
			continue
		}

		partitionPath := filepath.Join(r.baseDir, dirName)
		l, err := storage.NewLog(partitionPath, r.config)
		if err != nil {
			return fmt.Errorf("failed to initialize log for %s: %w", dirName, err)
		}

		if _, ok := r.logs[topic]; !ok {
			r.logs[topic] = make(map[int32]*storage.Log)
		}
		r.logs[topic][int32(partition)] = l
	}

	// ReplicationFactor isn't persisted anywhere yet (no metadata store
	// until Raft lands in Phase 2), so it defaults to 1 here. A topic
	// created via CreateTopic in the same process lifetime already has a
	// real entry in r.topics and is left untouched.
	for topic, pMap := range r.logs {
		if _, exists := r.topics[topic]; exists {
			continue
		}
		r.topics[topic] = TopicConfig{
			Name:              topic,
			NumPartitions:     int32(len(pMap)),
			ReplicationFactor: 1,
		}
	}

	return nil
}

func (r *Registry) GetLog(topic string, partition int32) (*storage.Log, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pMap, ok := r.logs[topic]
	if !ok {
		return nil, false
	}

	l, ok := pMap[partition]
	return l, ok
}

func (r *Registry) CreateLog(topic string, partition int32) (*storage.Log, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if pMap, ok := r.logs[topic]; ok {
		if l, ok := pMap[partition]; ok {
			return l, fmt.Errorf("partition %d for topic %s already existed", partition, topic)
		}
	}

	dirName := fmt.Sprintf("%s-%d", topic, partition)
	partitionPath := filepath.Join(r.baseDir, dirName)
	if err := os.MkdirAll(partitionPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}

	newLog, err := storage.NewLog(partitionPath, r.config)
	if err != nil {
		return nil, err
	}

	if r.logs[topic] == nil {
		r.logs[topic] = make(map[int32]*storage.Log)
	}
	r.logs[topic][partition] = newLog
	return newLog, nil
}

func (r *Registry) GetFlusher(topic string, partition int32, batchSize int, linger time.Duration) (*Flusher, bool) {
	r.mu.RLock()
	if pMap, ok := r.flushers[topic]; ok {
		if fl, ok := pMap[partition]; ok {
			r.mu.RUnlock()
			return fl, true
		}
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if pMap, ok := r.flushers[topic]; ok {
		if fl, ok := pMap[partition]; ok {
			return fl, true
		}
	}

	pMap, ok := r.logs[topic]
	if !ok {
		return nil, false
	}
	l, ok := pMap[partition]
	if !ok {
		return nil, false
	}

	fl := NewFlusher(l, batchSize, linger)
	if r.flushers[topic] == nil {
		r.flushers[topic] = make(map[int32]*Flusher)
	}
	r.flushers[topic][partition] = fl
	return fl, true
}
