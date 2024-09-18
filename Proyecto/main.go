package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type Log struct {
	mu      sync.Mutex
	records []Record
}

var logfile = Log{records: []Record{}}
var offsetCounter uint64 = 0

func WriteLGHandler(w http.ResponseWriter, r *http.Request) {
	var record Record

	// Deserializamos el JSON
	err := json.NewDecoder(r.Body).Decode(&record)
	if err != nil {
		http.Error(w, "Error deserializando el JSON", http.StatusBadRequest)
		return
	}

	record.Offset = offsetCounter
	offsetCounter++

	logfile.mu.Lock()
	logfile.records = append(logfile.records, record)
	logfile.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(record)
}

func ReadLGHandler(w http.ResponseWriter, r *http.Request) {
	var requestData struct {
		Offset *uint64 `json:"offset"`
	}

	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		http.Error(w, "404: No se pudo deserializar el JSON", http.StatusBadRequest)
		return
	}

	// Buscamos en el log el offset
	logfile.mu.Lock()
	defer logfile.mu.Unlock()

	for _, record := range logfile.records {
		if record.Offset == *requestData.Offset {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]Record{"record": record})
			return
		}
	}

	http.Error(w, "404: No encontrado", http.StatusNotFound)
}

func startServer() {
	http.HandleFunc("/write", WriteLGHandler)
	http.HandleFunc("/read", ReadLGHandler)

	log.Println("Servidor en ejecución -> http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Error al iniciar el servidor: %v", err)
	}
}

func main() {
	startServer()
}
