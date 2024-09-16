package Log2

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	OffWidth   uint64 = 4
	PosWidth   uint64 = 8
	EntryWidth        = OffWidth + PosWidth
)

type Index struct {
	file *os.File
	mmap gommap.MMap
	Size uint64
}

func NewIndex(pf *os.File, c Config) (*Index, error) {
	idx := &Index{
		file: pf,
	}
	fi, err := os.Stat(pf.Name())
	if err != nil {
		return nil, err
	}
	// Initialize the Size of the index to the size of the file
	idx.Size = uint64(fi.Size())
	if err = os.Truncate(
		pf.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read reads the offset and position from the index at the given entry number.
func (i *Index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.Size == 0 {
		// The index is empty
		return 0, 0, io.EOF
	}

	if in == -1 {
		// Read the last entry
		if i.Size < uint64(EntryWidth) {
			// If the index size is smaller than one entry, no valid entries exist
			return 0, 0, io.EOF
		}
		out = uint32((i.Size / uint64(EntryWidth)) - 1)
	} else {
		// Read the specified entry
		out = uint32(in)
	}

	pos = uint64(out) * uint64(EntryWidth)

	if pos+uint64(EntryWidth) > i.Size {
		// The requested position is out of the index range
		return 0, 0, io.EOF
	}

	// Read the offset and position from the index
	out = binary.BigEndian.Uint32(i.mmap[pos : pos+OffWidth])
	pos = binary.BigEndian.Uint64(i.mmap[pos+OffWidth : pos+EntryWidth])

	return out, pos, nil
}

// Write writes an offset and position to the index.
func (i *Index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.Size+uint64(EntryWidth) {
		return io.EOF
	}
	// Write the offset and position to the memory map
	binary.BigEndian.PutUint32(i.mmap[i.Size:i.Size+OffWidth], off)
	binary.BigEndian.PutUint64(i.mmap[i.Size+OffWidth:i.Size+EntryWidth], pos)
	i.Size += uint64(EntryWidth)
	return nil
}

// Close closes the index, syncing the memory map to the file and truncating it.
func (i *Index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	// Truncate the file to the Size of the index.
	if err := i.file.Truncate(int64(i.Size)); err != nil {
		return err
	}

	// Close the file.
	return i.file.Close()
}

func (i *Index) FileName() string {
	return i.file.Name()
}
