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

	// logs maps: Topic Name -> Partition ID -> Log Object
	// Example: logs["orders"][0] -> *storage.Log
	logs map[string]map[int32]*storage.Log
}

func NewRegistry(baseDir string, cfg storage.LogConfig) *Registry {
	return &Registry{
		baseDir: baseDir,
		config:  cfg,
		logs:    make(map[string]map[int32]*storage.Log),
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

	// Check if the partition is already existed
	if partitionMap, existed := r.logs[topic]; existed {
		return partitionMap[partition], fmt.Errorf("partition %d for topic %s already existed", partition, topic)
	}

	// Create the partition directory. The directory name should be
	// of the form data/topic-name-partitionID
	dirName := fmt.Sprintf("%s-%d", topic, partition)
	partitionPath := filepath.Join(r.baseDir, dirName)

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
