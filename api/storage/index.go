package storage

import (
	"io"
	"os"
	"sync"

	"github.com/tysonmote/gommap"
)

const (
	offWidth = 4                   // 4 bytes for relative offset
	posWidth = 4                   // 4 bytes for physical position
	entWidth = offWidth + posWidth // total entry size (8 bytes)
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

func (i *Index) Write(off uint32, pos uint64) error { // CHANGED: pos is now uint64
	i.mu.Lock()
	defer i.mu.Unlock()

	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// Write 4-byte relative offset
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	i.size += uint64(entWidth)
	return nil
}

func (i *Index) Read(in int64) (out uint32, pos uint64, err error) { // CHANGED: pos is now uint64
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.size == 0 {
		return 0, 0, io.EOF
	}

	var outRange uint64
	if in == -1 {
		outRange = i.size - entWidth
	} else {
		outRange = uint64(in) * entWidth
	}

	if i.size < outRange+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[outRange : outRange+offWidth])
	// CHANGED: Read 8-byte physical position
	pos = enc.Uint64(i.mmap[outRange+offWidth : outRange+entWidth])
	return out, pos, nil
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
