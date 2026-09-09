package storage

import (
	"fmt"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *Segment
	segments      []*Segment
}

type Config struct {
	MaxSegmentSize     uint64
	MaxIndexSize       uint64
	IndexIntervalBytes uint64        // min bytes written between two index entries (sparse index)
	RetentionPeriod    time.Duration // how long segments are retained before cleanup
	FlushInterval      time.Duration // how often buffered writes are fsynced to disk
}

func DefaultConfig() Config {
	return Config{
		MaxSegmentSize:     1024 * 1024,        // 1 MB
		MaxIndexSize:       256 * 1024,         // 256 KB
		IndexIntervalBytes: 4096,               // 4 KB, matches Kafka's log.index.interval.bytes default
		RetentionPeriod:    7 * 24 * time.Hour, // 7 days
		FlushInterval:      500 * time.Millisecond,
	}
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
	s, err := NewSegment(l.Dir, baseOffset, SegmentConfig{
		MaxLogSize:         l.Config.MaxSegmentSize,
		MaxIndexSize:       l.Config.MaxIndexSize,
		IndexIntervalBytes: l.Config.IndexIntervalBytes,
		RetentionPeriod:    l.Config.RetentionPeriod,
		FlushInterval:      l.Config.FlushInterval,
	})
	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s

	return nil
}

// Append adds a record to the log and returns the offset and error if any
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
	off, err := l.activeSegment.Append(data)
	return int(off), err
}

// Close closes every segment's underlying files. The log must not be
// used after Close.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Read logic at the Log level
func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// 1. Find the segment that contains the offset
	// We use binary search to find the highest baseOffset <= offset
	idx := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].nextOffset > offset
	})

	// 2. Validation
	if idx == len(l.segments) || l.segments[idx].baseOffset > offset {
		return nil, fmt.Errorf("offset %d is out of range (current max: %d)", offset, l.activeSegment.nextOffset-1)
	}

	// 3. Delegate to the specific segment
	return l.segments[idx].Read(offset)
}
