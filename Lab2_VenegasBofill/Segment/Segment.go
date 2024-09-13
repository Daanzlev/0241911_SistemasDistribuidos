package Segment

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/golang/protobuf/proto"

	"lab2/Config"
	"lab2/Index"
	"lab2/Store"

	api "lab2/api/v1"
)

type Segment struct {
	Store      *Store.Store
	Index      *Index.Index
	Base, next uint64
	Config     Config.Config
}

func NewSegment(dir string, base uint64, c Config.Config) (*Segment, error) {
	s := &Segment{
		Base:   base,
		Config: c,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", base, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.Store, err = Store.NewStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", base, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.Index, err = Index.NewIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.Index.Read(-1); err != nil {
		s.next = base
	} else {
		s.next = base + uint64(off) + 1
	}
	return s, nil
}

// Append agrega un nuevo registro al segmento y devuelve el offset del registro
func (s *Segment) Append(record *api.Record) (off uint64, err error) {
	cur := s.next
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.Store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.Index.Write(
		// index offsets are relative to base offset
		uint32(s.next-uint64(s.Base)),
		pos,
	); err != nil {
		return 0, err
	}
	s.next++
	return cur, nil
}

// Read lee un registro del segmento dado un offset
func (s *Segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.Index.Read(int64(off - s.Base))
	if err != nil {
		return nil, err
	}

	data := make([]byte, Store.LenWidth)
	_, err = s.Store.ReadAt(data, int64(pos))
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, err
	}

	return record, nil
}

// IsFull verifica si el segmento ha alcanzado su capacidad máxima
func (s *Segment) IsFull() bool {
	return s.Store.size >= s.Config.Segment.MaxStoreBytes ||
		s.Index.size >= s.Config.Segment.MaxIndexBytes
}

// Remove elimina el segmento del sistema de archivos
func (s *Segment) Remove() error {
	if err := s.Close(); err != nil {
		return fmt.Errorf("error closing segment: %w", err)
	}

	storeFilePath := filepath.Join(filepath.Dir(s.Store.FileName()), fmt.Sprintf("%d.store", s.Base))
	indexFilePath := filepath.Join(filepath.Dir(s.Index.FileName()), fmt.Sprintf("%d.index", s.Base))

	if err := os.Remove(storeFilePath); err != nil {
		return fmt.Errorf("failed to remove store file: %w", err)
	}

	if err := os.Remove(indexFilePath); err != nil {
		return fmt.Errorf("failed to remove index file: %w", err)
	}

	return nil
}

// Close cierra los archivos del segmento
func (s *Segment) Close() error {
	if err := s.Store.Close(); err != nil {
		return err
	}
	return s.Index.Close()
}
func Nearest(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j / k) - 1) * k
}
