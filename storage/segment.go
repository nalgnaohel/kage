package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	lenWidth    = 4
	offsetWidth = 8
)

// BigEndian so the log reads back the same regardless of platform endianness.
var enc = binary.BigEndian

type Segment struct {
	log         *os.File
	logPath     string
	index       *Index // mmap-backed
	baseOffset  uint64
	nextOffset  uint64
	maxLogSize  uint64
	currentSize uint64

	indexIntervalBytes uint64 // min bytes between two index entries (sparse index)
	bytesSinceIndex    uint64

	mu sync.RWMutex
}

// SegmentConfig holds tunable parameters for a single segment.
type SegmentConfig struct {
	MaxLogSize         uint64 // max bytes the .log file can grow before rolling
	MaxIndexSize       uint64 // max bytes for the memory-mapped index
	IndexIntervalBytes uint64 // min bytes between two index entries (sparse index)
	RetentionPeriod    time.Duration
	FlushInterval      time.Duration // how often buffered writes are fsynced to disk
}

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
		log:                logFile,
		logPath:            logPath,
		index:              idx,
		baseOffset:         baseOffset,
		maxLogSize:         c.MaxLogSize,
		indexIntervalBytes: c.IndexIntervalBytes,
	}

	fi, err := logFile.Stat()
	if err != nil {
		return nil, err
	}
	s.currentSize = uint64(fi.Size())

	if err := s.recover(); err != nil {
		return nil, err
	}

	return s, nil
}

// recover determines nextOffset and bytesSinceIndex after (re)opening a
// segment. Because the index is sparse, its last entry does not necessarily
// point at the log's actual last record, so we jump to the last indexed
// position (or the start of the file if the index is empty) and scan
// forward record-by-record to the true end of the file.
func (s *Segment) recover() error {
	var startPos int64
	if _, lastPos, err := s.index.Read(-1); err == nil {
		startPos = int64(lastPos)
	}

	pos := startPos
	next := s.baseOffset
	for pos < int64(s.currentSize) {
		header := make([]byte, offsetWidth+lenWidth)
		if _, err := s.log.ReadAt(header, pos); err != nil {
			return fmt.Errorf("corrupt segment at pos %d: %w", pos, err)
		}

		off := enc.Uint64(header[:offsetWidth])
		size := enc.Uint32(header[offsetWidth:])

		next = off + 1
		pos += int64(offsetWidth+lenWidth) + int64(size)
	}

	s.nextOffset = next
	s.bytesSinceIndex = uint64(pos - startPos)
	return nil
}

func (s *Segment) Append(message []byte) (offset uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	curOffset := s.nextOffset
	header := make([]byte, offsetWidth+lenWidth)
	enc.PutUint64(header[:offsetWidth], curOffset)
	enc.PutUint32(header[offsetWidth:], uint32(len(message)))

	if _, err := s.log.Write(header); err != nil {
		return 0, err
	}
	if _, err := s.log.Write(message); err != nil {
		return 0, err
	}

	pos := s.currentSize
	// Always index the first record (so lookups at baseOffset never scan
	// from position 0); otherwise only once enough bytes have accumulated.
	if pos == 0 || s.bytesSinceIndex >= s.indexIntervalBytes {
		if err := s.index.Write(uint32(curOffset-s.baseOffset), pos); err != nil {
			return 0, err
		}
		s.bytesSinceIndex = 0
	}

	written := uint64(len(header) + len(message))
	s.nextOffset++
	s.currentSize += written
	s.bytesSinceIndex += written

	return curOffset, nil
}

// Close must be the last call on a Segment — it is not usable afterward.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.log.Sync(); err != nil {
		return err
	}
	if err := s.index.Close(); err != nil {
		return err
	}
	return s.log.Close()
}

// walkRecords scans sequentially from physical position startPos, a shared logic helper.
func (s *Segment) walkRecords(startPos int64, visit func(off uint64, recPos, recLen int64) (keepGoing bool)) error {
	pos := startPos
	for pos < int64(s.currentSize) {
		header := make([]byte, offsetWidth+lenWidth)
		if _, err := s.log.ReadAt(header, pos); err != nil {
			return err
		}

		off := enc.Uint64(header[:offsetWidth])
		size := enc.Uint32(header[offsetWidth:])
		recLen := int64(offsetWidth+lenWidth) + int64(size)

		if !visit(off, pos, recLen) {
			return nil
		}

		pos += recLen
	}
	return nil
}

func (s *Segment) Read(offset uint64) (message []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	relOffset := offset - s.baseOffset
	_, physicalPos, err := s.index.Read(int64(relOffset))
	if err != nil {
		return nil, err
	}

	var (
		data     []byte
		found    bool
		notFound bool
		readErr  error
	)
	walkErr := s.walkRecords(int64(physicalPos), func(off uint64, recPos, recLen int64) bool {
		if off == offset {
			buf := make([]byte, recLen-int64(offsetWidth+lenWidth))
			if _, e := s.log.ReadAt(buf, recPos+int64(offsetWidth+lenWidth)); e != nil {
				readErr = e
				return false
			}
			data = buf
			found = true
			return false
		}

		if off > offset {
			notFound = true
			return false
		}

		return true
	})

	if readErr != nil {
		return nil, readErr
	}
	if walkErr != nil {
		if walkErr == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, walkErr
	}
	if notFound {
		return nil, fmt.Errorf("offset %d not found", offset)
	}
	if !found {
		// Ran off the end of the segment's committed data without ever seeing
		// offset — same "corrupt/short read" signal the old unbounded scan
		// produced by hitting ReadAt's io.EOF past the real file size.
		return nil, io.ErrUnexpectedEOF
	}

	return data, nil
}

type SegmentRange struct {
	Pos        int64  // byte offset of the first record's header in the .log file
	Length     int64  // total bytes spanned (whole records only)
	NextOffset uint64 // offset to resume fetching from
}

func (s *Segment) LocateRange(startOffset uint64, maxBytes int32) (SegmentRange, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	relOffset := startOffset - s.baseOffset
	_, physicalPos, err := s.index.Read(int64(relOffset))
	if err != nil {
		return SegmentRange{}, err
	}

	var (
		segmentRange SegmentRange
		started      bool
		total        int64
		notFound     bool
	)
	walkErr := s.walkRecords(int64(physicalPos), func(off uint64, recPos, recLen int64) bool {
		if !started {
			if off < startOffset {
				return true
			}
			if off > startOffset {
				notFound = true
				return false
			}
			segmentRange.Pos = recPos
			started = true
		}

		total += recLen
		segmentRange.NextOffset = off + 1

		return total < int64(maxBytes)
	})
	if walkErr != nil {
		return SegmentRange{}, walkErr
	}
	if notFound || !started {
		return SegmentRange{}, fmt.Errorf("offset %d not found", startOffset)
	}

	segmentRange.Length = total
	return segmentRange, nil
}

// OpenReader opens a brand-new, independent, read-only handle to this segment's log
// file -> avoid race condition while reading from the segment's log file concurrently with appends.
func (s *Segment) OpenReader() (*os.File, error) {
	return os.Open(s.logPath)
}
