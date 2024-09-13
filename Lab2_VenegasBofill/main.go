package main

import (
	"fmt"
	"lab2/Store" // Asegúrate de que este sea el nombre correcto del paquete
	"log"
	"os"
)

func main() {
	// Crear un archivo temporal para usar con Store.
	file, err := os.Create("testfile.dat")
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}
	defer file.Close()

	// Crear una nueva instancia de Store.
	store, err := Store.NewStore(file)
	if err != nil {
		log.Fatalf("Error creating store: %v", err)
	}
	defer store.Close()

	// Agregar datos al Store.
	data1 := []byte("Hello, world!")
	offset1, n1, err := store.Append(data1)
	if err != nil {
		log.Fatalf("Error appending data1: %v", err)
	}
	fmt.Printf("Appended data1 at offset %d, total bytes written: %d\n", offset1, n1)

	data2 := []byte("Another piece of data.")
	offset2, n2, err := store.Append(data2)
	if err != nil {
		log.Fatalf("Error appending data2: %v", err)
	}
	fmt.Printf("Appended data2 at offset %d, total bytes written: %d\n", offset2, n2)

	// Leer los datos del archivo.
	buf1 := make([]byte, len(data1)+Store.LenWidth)
	n, err := store.ReadAt(buf1, int64(offset1))
	if err != nil {
		log.Fatalf("Error reading data1: %v", err)
	}
	fmt.Printf("Read %d bytes: %s\n", n, buf1[Store.LenWidth:])

	buf2 := make([]byte, len(data2)+Store.LenWidth)
	n, err = store.ReadAt(buf2, int64(offset2))
	if err != nil {
		log.Fatalf("Error reading data2: %v", err)
	}
	fmt.Printf("Read %d bytes: %s\n", n, buf2[Store.LenWidth:])
}
