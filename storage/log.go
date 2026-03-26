package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *Segment
	segments      []*Segment
}

type Config struct {
	MaxSegmentSize uint64
	MaxIndexSize   uint64
}

func NewLog(dir string, c Config) (*Log, error) {
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".log") {
			offStr := strings.TrimSuffix(file.Name(), ".log")
			off, _ := strconv.ParseUint(offStr, 10, 64)
			baseOffsets = append(baseOffsets, off)
		}
	}

	// Sort so segments[0] is the oldest data
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, offset := range baseOffsets {
		if err := l.newSegment(offset); err != nil {
			return err
		}
	}

	// Create initial segment if directory was empty
	if l.segments == nil {
		if err := l.newSegment(0); err != nil {
			return err
		}
	}

	return nil
}

// newSegment creates a new segment at the given base offset
func (l *Log) newSegment(baseOffset uint64) error {
	// 1. Construct file paths using 20-digit zero-padded names (Kafka style)
	logPath := path.Join(l.Dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := path.Join(l.Dir, fmt.Sprintf("%020d.index", baseOffset))

	// 2. Open or create the log file (Append mode)
	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	// 3. Open or create the index file
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open index file: %w", err)
	}

	// 4. Initialize the memory-mapped index
	idx, err := NewIndex(indexFile, l.Config.MaxIndexSize)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	// 5. Create the Segment instance
	s := &Segment{
		log:        logFile,
		index:      idx,
		baseOffset: baseOffset,
		maxLogSize: l.Config.MaxSegmentSize,
	}

	// 6. Recovery logic: Calculate the next offset and current size
	// This handles cases where the Broker restarts and reloads existing files
	fi, err := logFile.Stat()
	if err != nil {
		return err
	}
	s.currentSize = uint64(fi.Size())

	// Determine nextOffset based on the last index entry
	if lastOff, _, err := idx.Read(-1); err == nil {
		// nextOffset is the last recorded relative offset + 1 + baseOffset
		s.nextOffset = baseOffset + uint64(lastOff) + 1
	} else {
		// Index is empty, start from the base
		s.nextOffset = baseOffset
	}

	// 7. Update Log manager state
	l.segments = append(l.segments, s)
	l.activeSegment = s

	return nil
}

// Return the current offset of our log and error if any
func (l *Log) Append(data []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if the current segment is full.
	// If full, we append log data to new segment
	if l.activeSegment.currentSize+uint64(len(data)) > l.activeSegment.maxLogSize {
		if err := l.newSegment(l.activeSegment.nextOffset); err != nil {
			return 0, err
		}
	}
	return l.activeSegment
}

// Read logic at the Log level
func (l *Log) Read(off uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// 1. Find the segment that contains the offset
	// We use binary search to find the highest baseOffset <= off
	idx := sort.Search(len(l.segments), func(i int) bool {
		// This returns the first segment whose nextOffset is > off
		return l.segments[i].nextOffset > off
	})

	// 2. Validation
	if idx == len(l.segments) || l.segments[idx].baseOffset > off {
		return nil, fmt.Errorf("offset %d is out of range (current max: %d)", off, l.activeSegment.nextOffset-1)
	}

	// 3. Delegate to the specific segment
	return l.segments[idx].Read(off)
}
