package Log2

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
	Size uint64
}

func NewStore(f *os.File) (*Store, error) {

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	Size := uint64(fi.Size())
	return &Store{
		File: f,
		Size: Size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *Store) Append(p []byte) (off uint64, n uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	off = s.Size
	if err = binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	written, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	s.Size += uint64(written) + LenWidth
	return off, uint64(written) + LenWidth, nil
}
func (s *Store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	Size := make([]byte, LenWidth)
	if _, err := s.File.ReadAt(Size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(Size))
	if _, err := s.File.ReadAt(b, int64(pos+LenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt reads data from the file at the given offset after flushing the buffer.
func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close flushes the buffer and closes the file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}

	// Close the file.
	return s.File.Close()
}
func (s *Store) FileName() string {
	return s.File.Name()
}
