package storage

import (
	"io"
	"os"
	"sync"

	"github.com/tysonmote/gommap"
)

const (
	offWidth   = 4                   // 4 bytes for relative offset
	posWidth   = 4                   // 4 bytes for physical position
	entryWidth = offWidth + posWidth // total entry size (8 bytes)
)

type Index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
	mu   sync.Mutex
}

// NewIndex creates a new index file and memory-maps it
func NewIndex(f *os.File, maxIndexSize uint64) (*Index, error) {
	idx := &Index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// Truncate the file to the maximum size before mapping
	if err := f.Truncate(int64(maxIndexSize)); err != nil {
		return nil, err
	}

	// Map the file into memory
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *Index) Write(offset uint32, physicalPos uint64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if uint64(len(i.mmap)) < i.size + entryWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], offset)
	enc.PutUint32(i.mmap[i.size+offWidth:i.size+entryWidth], uint32(physicalPos))

	i.size += uint64(entryWidth)
	return nil
}

func (i *Index) Read(in int64) (offset uint32, physicalPos uint64, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.size == 0 {
		return 0, 0, io.EOF
	}

	target := uint32(in)
	totalEntries := int64(i.size / uint64(entryWidth))

	// low/high are signed so that high can go below zero when the target
	// is smaller than every stored offset, instead of wrapping around as
	// a huge uint64 and driving the mmap access out of bounds.
	low := int64(0)
	high := totalEntries - 1

	for low <= high {
		mid := low + (high-low)/2
		pos := uint64(mid) * uint64(entryWidth)

		offsetAtMid := enc.Uint32(i.mmap[pos : pos+uint64(offWidth)])

		if offsetAtMid == target {
			actualPhysicalPos := enc.Uint32(i.mmap[pos+uint64(offWidth) : pos+uint64(entryWidth)])
			return offsetAtMid, uint64(actualPhysicalPos), nil
		}

		if offsetAtMid < target {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if high < 0 {
		// target is smaller than every stored offset: there is no entry
		// at-or-before it.
		return 0, 0, io.EOF
	}

	finalPos := uint64(high) * uint64(entryWidth)
	offsetVal := enc.Uint32(i.mmap[finalPos : finalPos+uint64(offWidth)])
	physicalPosVal := enc.Uint32(i.mmap[finalPos+uint64(offWidth) : finalPos+uint64(entryWidth)])

	return offsetVal, uint64(physicalPosVal), nil
}

// Close ensures the file is truncated to its actual data size and synced
func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Sync memory changes to disk
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// Flush file buffers
	if err := i.file.Sync(); err != nil {
		return err
	}
	// Truncate file to remove unused space allocated for mmap
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
