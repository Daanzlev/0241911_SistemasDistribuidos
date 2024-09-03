package Store

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	LenWidth = 8
)

type Store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func NewStore(f *os.File) (*Store, error) {

	return &Store{File: f, buf: bufio.NewWriter(f)}, nil
}

func (s *Store) Append(p []byte) (off uint64, n uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Determine the current offset in the file.
	off = s.size

	// Write the length of the data to the buffer.
	if err = binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write the actual data to the buffer.
	written, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// Update the size of the file.
	s.size += uint64(written) + LenWidth

	// Return the starting offset and the number of bytes written.
	return off, uint64(written) + LenWidth, nil
}

// ReadAt reads data from the file at the given offset after flushing the buffer.
func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the buffer to ensure all data is written to the file.
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	// Read the data from the file at the specified offset.
	return s.File.ReadAt(p, off)
}

// Close flushes the buffer and closes the file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the buffer to ensure all data is written to the file.
	if err := s.buf.Flush(); err != nil {
		return err
	}

	// Close the file.
	return s.File.Close()
}
func (s *Store) FileName() string {
	return s.File.Name()
}
