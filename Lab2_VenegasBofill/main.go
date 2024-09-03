package main

import (
	"fmt"
	"log"
	"os"

	"lab2/Index"
	"lab2/Store"
)

func main() {
	// Inicializar el índice
	idx, err := Index.NewIndex("index_file")
	if err != nil {
		log.Fatalf("No se pudo crear el índice: %v", err)
	}
	defer func() {
		if err := idx.Close(); err != nil {
			log.Fatalf("Error al cerrar el índice: %v", err)
		}
	}()

	// Inicializar el store
	f, err := os.OpenFile("store_file", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		log.Fatalf("No se pudo abrir el archivo de store: %v", err)
	}
	st, err := Store.NewStore(f)
	if err != nil {
		log.Fatalf("No se pudo crear el store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Fatalf("Error al cerrar el store: %v", err)
		}
	}()

	// Escribir en el store
	data := []byte("datos de prueba")
	off, writtenBytes, err := st.Append(data)
	if err != nil {
		log.Fatalf("No se pudo escribir en el store: %v", err)
	}
	fmt.Printf("Datos escritos en el store, offset %d, bytes escritos %d\n", off, writtenBytes)

	// Leer desde el store
	readBuf := make([]byte, len(data))
	_, err = st.ReadAt(readBuf, int64(off))
	if err != nil {
		log.Fatalf("No se pudo leer del store: %v", err)
	}
	fmt.Printf("Datos leídos del store: %s\n", string(readBuf))

	// Escribir en el índice
	// Aquí asumimos que el offset cabe en un uint32, si no, podrías necesitar manejarlo diferente
	err = idx.Write(uint32(off), uint64(off))
	if err != nil {
		log.Fatalf("No se pudo escribir en el índice: %v", err)
	}
	fmt.Println("Datos escritos en el índice")

	// Leer desde el índice
	out, pos, err := idx.Read(-1) // Leer la última entrada escrita
	if err != nil {
		log.Fatalf("No se pudo leer del índice: %v", err)
	}
	fmt.Printf("Datos leídos del índice: offset %d, posición %d\n", out, pos)
}
