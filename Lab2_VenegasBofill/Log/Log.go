package Log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	//Importamos los dolores de cabeza JAJAJ
	//"api/v1/log_v1"
	"lab2/Config"
	"lab2/Segment"
	"lab2/Store"
	api "lab2/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config.Config
	activeSegment *Segment.Segment
	segments      []*Segment.Segment
}

// NewLog crea un nuevo registro de log, inicializando los segmentos según la configuración.
func NewLog(dir string, c Config.Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

// setup configura los segmentos del log, creando nuevos si es necesario.
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		i++
	}
	if len(l.segments) == 0 {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	l.activeSegment = l.segments[len(l.segments)-1]
	return nil
}

// Append agrega un nuevo registro al log.
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsFull() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// Read lee un registro del log a partir de un offset.
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *Segment.Segment
	for _, segment := range l.segments {
		if segment.Base <= off && off < segment.NextOff {
			s = segment
			break
		}
	}
	// START: before
	if s == nil || s.NextOff <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	// END: before
	return s.Read(off)
}

// newSegment crea un nuevo segmento en el log.
func (l *Log) newSegment(off uint64) error {
	s, err := Segment.NewSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

// Close cierra todos los segmentos del log.
func (l *Log) Close() error {
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove elimina el directorio del log y todos sus segmentos.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset reinicia el log, eliminando todos los segmentos y volviendo a configurar.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// LowestOffset devuelve el offset más bajo del log.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0, fmt.Errorf("no segments available")
	}
	return l.segments[0].Base, nil
}

// HighestOffset devuelve el offset más alto del log.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0, nil
	}
	lastSegment := l.segments[len(l.segments)-1]
	return lastSegment.Base + lastSegment.Size - 1, nil
}

// Truncate elimina los segmentos del log cuyo offset sea menor que el mínimo especificado.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*Segment.Segment
	for _, s := range l.segments {
		if s.Base+s.Size <= lowest {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

// Reader devuelve un lector que lee desde todos los segmentos del log.
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{Store: segment.Store, off: 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*Store.Store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
