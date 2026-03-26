package storage

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
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

func (s *Segment) Init(log *os.File, baseOffset, nextOffset, maxLogSize uint64) {

}

func (s *Segment) Append(message []byte) (offset uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	curOffset := s.nextOffset
	// Write the message size and offset as header
	header := make([]byte, lenWidth+offsetWidth)
	enc.PutUint64(header[:lenWidth], uint64(len(message)))
	enc.PutUint64(header[lenWidth:], curOffset)

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
	_, pos, err := s.index.Read(int64(relOffset))
	if err != nil {
		return nil, err
	}

	header := make([]byte, offsetWidth+lenWidth)
	if _, err := s.log.ReadAt(header, int64(pos)); err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("failed to read log header: %w", err)
	}

	// add recordSize to header
	// Offset is bytes [0:8], Size is bytes [8:12]
	recordSize := enc.Uint32(header[offsetWidth:])

	// handle our data from the 12 byte
	dataPos := int64(pos + uint32(offsetWidth+lenWidth))
	if _, err := s.log.ReadAt(data, dataPos); err != nil {
		return nil, fmt.Errorf("failed to read log data: %w", err)
	}
	return data, nil
}
