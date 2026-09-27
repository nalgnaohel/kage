package storage

import (
	"fmt"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config LogConfig

	activeSegment *Segment
	segments      []*Segment

	highWatermark atomic.Uint64
}

type LogConfig struct {
	MaxSegmentSize     uint64
	MaxIndexSize       uint64
	IndexIntervalBytes uint64 // min bytes between two index entries (sparse index)
	RetentionPeriod    time.Duration
	FlushInterval      time.Duration // how often buffered writes are fsynced to disk
}

func DefaultLogConfig() LogConfig {
	return LogConfig{
		MaxSegmentSize:     1024 * 1024,        // 1 MB
		MaxIndexSize:       256 * 1024,         // 256 KB
		IndexIntervalBytes: 4096,               // 4 KB, matches Kafka's log.index.interval.bytes default
		RetentionPeriod:    7 * 24 * time.Hour, // 7 days
		FlushInterval:      500 * time.Millisecond,
	}
}

func NewLog(dir string, c LogConfig) (*Log, error) {
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

	// ascending, so segments[0] is the oldest data
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, offset := range baseOffsets {
		if err := l.newSegment(offset); err != nil {
			return err
		}
	}

	if l.segments == nil {
		if err := l.newSegment(0); err != nil {
			return err
		}
	}

	return nil
}

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

func (l *Log) Append(data []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.activeSegment.currentSize+uint64(len(data)) > l.activeSegment.maxLogSize {
		if err := l.newSegment(l.activeSegment.nextOffset); err != nil {
			return 0, err
		}
	}
	off, err := l.activeSegment.Append(data)
	return int(off), err
}

// Close must be the last call on a Log — it is not usable afterward.
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

func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// find the segment whose [baseOffset, nextOffset) range contains offset
	idx := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].nextOffset > offset
	})

	if idx == len(l.segments) || l.segments[idx].baseOffset > offset {
		return nil, fmt.Errorf("offset %d is out of range (current max: %d)", offset, l.activeSegment.nextOffset-1)
	}

	return l.segments[idx].Read(offset)
}

// LocateRange routes to the owning segment of startOffset with the given maxBytes.
func (l *Log) LocateRange(startOffset uint64, maxBytes int32) (seg *Segment, rng SegmentRange, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	idx := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].nextOffset > startOffset
	})

	if idx == len(l.segments) || l.segments[idx].baseOffset > startOffset {
		return nil, SegmentRange{}, fmt.Errorf("offset %d is out of range (current max: %d)", startOffset, l.activeSegment.nextOffset-1)
	}

	seg = l.segments[idx]
	rng, err = seg.LocateRange(startOffset, maxBytes)
	if err != nil {
		return nil, SegmentRange{}, err
	}
	return seg, rng, nil
}

func (l *Log) LogEndOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.activeSegment.nextOffset
}

func (l *Log) HighWatermark() uint64 {
	return l.highWatermark.Load()
}

func (l *Log) SetHighWatermark(offset uint64) {
	l.highWatermark.Store(offset)
}
