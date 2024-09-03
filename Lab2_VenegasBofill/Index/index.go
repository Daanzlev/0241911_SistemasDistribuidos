package Index

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	OffWidth   = 4
	PosWidth   = 8
	EntryWidth = OffWidth + PosWidth
)

type Index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// NewIndex creates a new index by opening or creating a file and memory-mapping it.
func NewIndex(path string) (*Index, error) {
	// Open or create the file with read/write permissions.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	// Get the current size of the file.
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())

	// Memory-map the file with read/write permissions.
	mmap, err := gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// Return the index with the file, memory map, and size.
	return &Index{file: f, mmap: mmap, size: size}, nil
}

// Read reads the offset and position from the index at the given entry number.
func (i *Index) Read(in int64) (out uint32, pos uint64, err error) {
	// Determine the entry number to read from.
	if in == -1 {
		out = uint32((i.size / uint64(EntryWidth)) - 1)
	} else {
		out = uint32(in)
	}

	// Calculate the starting position for reading in the mmap.
	pos = uint64(out) * uint64(EntryWidth)

	// Check if the read would go beyond the current size of the index.
	if i.size < pos+uint64(EntryWidth) {
		return 0, 0, io.EOF
	}

	// Read the offset from the mmap.
	out = binary.BigEndian.Uint32(i.mmap[pos : pos+uint64(OffWidth)])

	// Read the position from the mmap.
	pos = binary.BigEndian.Uint64(i.mmap[pos+uint64(OffWidth) : pos+uint64(EntryWidth)])

	return out, pos, nil
}

// Write writes an offset and position to the index.
func (i *Index) Write(off uint32, pos uint64) error {
	// Check if there is enough space to write the new entry.
	if i.size+uint64(EntryWidth) > uint64(len(i.mmap)) {
		return errors.New("not enough space in index to write entry")
	}

	// Write the offset to the mmap.
	binary.BigEndian.PutUint32(i.mmap[i.size:i.size+uint64(OffWidth)], off)

	// Write the position to the mmap.
	binary.BigEndian.PutUint64(i.mmap[i.size+uint64(OffWidth):i.size+uint64(EntryWidth)], pos)

	// Update the size of the index.
	i.size += uint64(EntryWidth)
	return nil
}

// Close closes the index, syncing the memory map to the file and truncating it.
func (i *Index) Close() error {
	// Sync the memory map to ensure all changes are written to the file.
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// Truncate the file to the size of the index.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	// Close the file.
	return i.file.Close()
}

func (i *Index) FileName() string {
	return i.file.Name()
}
