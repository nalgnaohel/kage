package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Segment's header's constants
const (
	lenWidth    = 4 // 4 bytes for Message Size
	offsetWidth = 8 // 8 bytes for Offset
)

// Use BigEndian to store the header
// BigEndian is used to make sure the data retrieved from the
// binary is the same regardless of the platform's endianness.
// This ensures that the log can be read correctly on any OS (macOS, Linux or Window).
var enc = binary.BigEndian

// Segment can be seen as an unit of log storage (log contains multiple segments)
type Segment struct {
	log         *os.File
	index       *Index // using mmap
	baseOffset  uint64 // first offset for this segment
	nextOffset  uint64 // next offset to be written
	maxLogSize  uint64
	currentSize uint64
	mu          sync.RWMutex // for write-safely when multiple producers send messages at the same time
}

// SegmentConfig holds tunable parameters for a single segment.
type SegmentConfig struct {
	MaxLogSize      uint64        // max bytes the .log file can grow before rolling
	MaxIndexSize    uint64        // max bytes for the memory-mapped index
	RetentionPeriod time.Duration // how long a segment is kept before eligible for deletion
	FlushInterval   time.Duration // how often buffered writes are fsynced to disk
}

// NewSegment creates and fully initialises a Segment from a config.
// It opens (or creates) the .log and .index files, recovers state from
// existing data on disk, and is ready to accept Append/Read calls.
func NewSegment(dir string, baseOffset uint64, c SegmentConfig) (*Segment, error) {
	logPath := fmt.Sprintf("%s/%020d.log", dir, baseOffset)
	indexPath := fmt.Sprintf("%s/%020d.index", dir, baseOffset)

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	idx, err := NewIndex(indexFile, c.MaxIndexSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	s := &Segment{
		log:        logFile,
		index:      idx,
		baseOffset: baseOffset,
		maxLogSize: c.MaxLogSize,
	}

	// Recovery: read current log size
	fi, err := logFile.Stat()
	if err != nil {
		return nil, err
	}
	s.currentSize = uint64(fi.Size())

	// Recovery: determine nextOffset from last index entry
	if lastOff, _, err := idx.Read(-1); err == nil {
		s.nextOffset = baseOffset + uint64(lastOff) + 1
	} else {
		s.nextOffset = baseOffset
	}

	return s, nil
}

func (s *Segment) Append(message []byte) (offset uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	curOffset := s.nextOffset
	header := make([]byte, offsetWidth+lenWidth)
	enc.PutUint64(header[:offsetWidth], curOffset)
	enc.PutUint32(header[offsetWidth:], uint32(len(message)))

	// Write header and message to the log file
	if _, err := s.log.Write(header); err != nil {
		return 0, err
	}
	if _, err := s.log.Write(message); err != nil {
		return 0, err
	}

	pos := s.currentSize
	if err := s.index.Write(uint32(curOffset-s.baseOffset), pos); err != nil {
		return 0, err
	}
	s.nextOffset++
	s.currentSize += uint64(len(header) + len(message))

	return curOffset, nil
}

func (s *Segment) Read(offset uint64) (message []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	relOffset := offset - s.baseOffset
	_, physicalPos, err := s.index.Read(int64(relOffset))
	if err != nil {
		return nil, err
	}

	currPos := int64(physicalPos)
	for {
		header := make([]byte, offsetWidth+lenWidth)
		if _, err := s.log.ReadAt(header, currPos); err != nil {
			if err == io.EOF {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}

		actualOff := enc.Uint64(header[:offsetWidth])
		msgSize := enc.Uint32(header[offsetWidth:])
		if actualOff == offset {
			data := make([]byte, msgSize)
			if _, err := s.log.ReadAt(data, currPos+int64(offsetWidth+lenWidth)); err != nil {
				return nil, err
			}
			return data, nil
		}

		if actualOff > offset {
			return nil, fmt.Errorf("offset %d not found", offset)
		}

		currPos += int64(offsetWidth+lenWidth) + int64(msgSize)
	}
}
