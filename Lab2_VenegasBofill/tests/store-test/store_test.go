package Store

import (
	"bytes"
	"lab2/Store"
	"os"
	"testing"
)

const LenWidth = 8 // Define LenWidth as a constant

func TestStore(t *testing.T) {
	// Crear un archivo temporal para usar con Store.
	file, err := os.Create("testfile.dat")
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	defer file.Close()
	defer os.Remove("testfile.dat") // Eliminar el archivo después de las pruebas.

	// Crear una nueva instancia de Store.
	store, err := Store.NewStore(file)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer store.Close()

	// Datos de prueba.
	data1 := []byte("Hello, world!")
	data2 := []byte("Another piece of data.")

	// Probar Append.
	offset1, n1, err := store.Append(data1)
	if err != nil {
		t.Fatalf("Error appending data1: %v", err)
	}
	if n1 != uint64(len(data1)+LenWidth) {
		t.Fatalf("Expected %d bytes written, got %d", len(data1)+LenWidth, n1)
	}

	offset2, n2, err := store.Append(data2)
	if err != nil {
		t.Fatalf("Error appending data2: %v", err)
	}
	if n2 != uint64(len(data2)+LenWidth) {
		t.Fatalf("Expected %d bytes written, got %d", len(data2)+LenWidth, n2)
	}

	// Probar ReadAt.
	buf1 := make([]byte, len(data1)+LenWidth)
	n, err := store.ReadAt(buf1, int64(offset1))
	if err != nil {
		t.Fatalf("Error reading data1: %v", err)
	}
	if n != len(buf1) {
		t.Fatalf("Expected %d bytes read, got %d", len(buf1), n)
	}
	if !bytes.Equal(buf1[LenWidth:], data1) {
		t.Fatalf("Expected %s, got %s", data1, buf1[LenWidth:])
	}

	buf2 := make([]byte, len(data2)+LenWidth)
	n, err = store.ReadAt(buf2, int64(offset2))
	if err != nil {
		t.Fatalf("Error reading data2: %v", err)
	}
	if n != len(buf2) {
		t.Fatalf("Expected %d bytes read, got %d", len(buf2), n)
	}
	if !bytes.Equal(buf2[LenWidth:], data2) {
		t.Fatalf("Expected %s, got %s", data2, buf2[LenWidth:])
	}
}
