package Log

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	api "lab2/api/v1"
)

type Segment struct {
	Store   *Store
	Index   *Index
	Base    uint64
	NextOff uint64
	Config  Config
	Size    uint64
}

func NewSegment(dir string, base uint64, c Config) (*Segment, error) {
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
	if s.Store, err = NewStore(storeFile); err != nil {
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
	if s.Index, err = NewIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.Index.Read(-1); err != nil {
		s.NextOff = base
	} else {
		s.NextOff = base + uint64(off) + 1
	}
	return s, nil
}

// Append agrega un nuevo registro al segmento y devuelve el offset del registro
func (s *Segment) Append(record *api.Record) (off uint64, err error) {
	cur := s.NextOff
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
		uint32(s.NextOff-uint64(s.Base)),
		pos,
	); err != nil {
		return 0, err
	}
	s.NextOff++
	return cur, nil
}

// Read lee un registro del segmento dado un offset
func (s *Segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.Index.Read(int64(off - s.Base))
	if err != nil {
		return nil, err
	}

	data := make([]byte, LenWidth)
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
	return s.Store.Size >= s.Config.Segment.MaxStoreBytes ||
		s.Index.Size >= s.Config.Segment.MaxIndexBytes
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
