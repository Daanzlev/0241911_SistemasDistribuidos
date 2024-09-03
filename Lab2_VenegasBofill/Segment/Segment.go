package Segment

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang/protobuf/proto"

	"lab2/Config"
	"lab2/Index"
	"lab2/Store"

	api "lab2/api/v1"
)

const (
	MaxStoreBytes = 1024 // Tamaño máximo del segmento en bytes
)

type Segment struct {
	Store  *Store.Store
	Index  *Index.Index
	Base   uint64               // Base offset del segmento
	Size   uint64               // Tamaño actual del segmento
	Config Config.SegmentConfig // Añadir la configuración aquí
}

func NewSegment(dir string, base uint64, cfg Config.SegmentConfig) (*Segment, error) {
	storeFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d.store", base)), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	store, err := Store.NewStore(storeFile)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d.index", base)), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	index, err := Index.NewIndex(indexFile.Name())
	if err != nil {
		return nil, err
	}

	return &Segment{
		Store:  store,
		Index:  index,
		Base:   base,
		Size:   0,
		Config: cfg,
	}, nil
}

// Append agrega un nuevo registro al segmento y devuelve el offset del registro
func (s *Segment) Append(record *api.Record) (uint64, uint64, error) {
	if s.Size >= s.Config.MaxStoreBytes {
		return 0, 0, fmt.Errorf("segment full")
	}

	// Convertir el record a []byte
	data, err := proto.Marshal(record)
	if err != nil {
		return 0, 0, err
	}

	pos, n, err := s.Store.Append(data)
	if err != nil {
		return 0, 0, err
	}

	err = s.Index.Write(uint32(s.Base+s.Size), pos)
	if err != nil {
		return 0, 0, err
	}

	s.Size += n
	return s.Base + s.Size - n, pos, nil
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
	return s.Size >= s.Config.MaxStoreBytes
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
