package Log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"lab2/Config" // Ajusta esta importación si tu configuración está en otro paquete
	"lab2/Index"
)

func TestIndex(t *testing.T) {
	// Crear un archivo temporal para el índice
	f, err := os.CreateTemp("", "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// Configuración de prueba
	cfg := Config.Config{}
	cfg.Segment.MaxIndexBytes = 1024

	// Abrir el archivo como nombre de archivo para el índice
	idx, err := Index.NewIndex(f.Name()) // Usa el nombre del archivo
	require.NoError(t, err)

	// Leer del índice vacío debe devolver un error
	_, _, err = idx.Read(-1)
	require.Error(t, err)

	// Entradas de prueba para escribir y leer
	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}
	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)
		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// Leer más allá de las entradas existentes debe devolver io.EOF
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)

	// Cerrar el índice
	err = idx.Close()
	require.NoError(t, err)

	// Reabrir el archivo y reconstruir el índice
	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)
	idx, err = Index.NewIndex(f.Name()) // Usa el nombre del archivo
	require.NoError(t, err)

	// Leer los valores del índice reconstruido
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
