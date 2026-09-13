package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/nalgnaohel/kage/storage"
)

type Registry struct {
	mu      sync.RWMutex
	baseDir string
	config  storage.LogConfig
	topics  map[string]TopicConfig

	// logs maps: Topic Name -> Partition ID -> Log Object
	// Example: logs["orders"][0] -> *storage.Log
	logs map[string]map[int32]*storage.Log

	// This broker's own identity. Immutable after construction — no lock
	// needed to read these. Single-broker Phase 1 only; real multi-broker
	// cluster membership is Raft/KRaft controller work (Phase 2, not
	// started), so GetClusterInfo always reports exactly this one broker.
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
		topics:    make(map[string]TopicConfig),
		clusterID: clusterID,
		brokerID:  brokerID,
		host:      host,
		port:      port,
	}
}

// Startup scans the base directory and performs Eager Loading of all partitions
func (r *Registry) Startup() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Ensure the base data directory exists
	if err := os.MkdirAll(r.baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	// Read all entries in the data directory
	entries, err := os.ReadDir(r.baseDir)
	if err != nil {
		return fmt.Errorf("failed to read base directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue // Skip files, we only care about partition directories
		}

		dirName := entry.Name()

		// Logic: Last Index Suffix (e.g., "user-activity-0")
		lastDash := strings.LastIndex(dirName, "-")
		if lastDash == -1 {
			continue // Skip if directory name doesn't follow the topic-partition format
		}

		topic := dirName[:lastDash]
		pStr := dirName[lastDash+1:]

		partition, err := strconv.Atoi(pStr)
		if err != nil {
			continue // Skip if the suffix is not a valid integer
		}

		// Initialize the storage engine for this specific partition
		partitionPath := filepath.Join(r.baseDir, dirName)
		l, err := storage.NewLog(partitionPath, r.config)
		if err != nil {
			return fmt.Errorf("failed to initialize log for %s: %w", dirName, err)
		}

		// Register the log object in our thread-safe map
		if _, ok := r.logs[topic]; !ok {
			r.logs[topic] = make(map[int32]*storage.Log)
		}
		r.logs[topic][int32(partition)] = l
	}

	// Rebuild topic-level config from what was actually discovered on disk.
	// ReplicationFactor isn't persisted anywhere yet (no metadata store until
	// Raft lands in Phase 2), so it defaults to 1 here — a topic created via
	// CreateTopic in the same process lifetime keeps its real value since
	// this only fills in topics still missing from r.topics.
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

// GetLog retrieves the Log object for a given topic and partition (Thread-safe)
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

// CreateLog creates a new partition on disk and register into our Registry
// (should be thread-safe)
func (r *Registry) CreateLog(topic string, partition int32) (*storage.Log, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if pMap, ok := r.logs[topic]; ok {
		if l, ok := pMap[partition]; ok {
			return l, fmt.Errorf("partition %d for topic %s already existed", partition, topic)
		}
	}

	// Create the partition directory. The directory name should be
	// of the form data/topic-name-partitionID
	dirName := fmt.Sprintf("%s-%d", topic, partition)
	partitionPath := filepath.Join(r.baseDir, dirName)
	if err := os.MkdirAll(partitionPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}

	// Create a new Log
	newLog, err := storage.NewLog(partitionPath, r.config)
	if err != nil {
		return nil, err
	}

	// Register the new Log in the registry
	if r.logs[topic] == nil {
		r.logs[topic] = make(map[int32]*storage.Log)
	}
	r.logs[topic][partition] = newLog
	return newLog, nil
}
