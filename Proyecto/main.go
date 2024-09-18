package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	api "Proyecto/api/v1" // Asegúrate de que esta ruta sea correcta
	Logpk "Proyecto/log"  // Importa el paquete de log con un alias
)

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type Log struct {
	mu      sync.Mutex
	logFile *Logpk.Log // Usa tu log como un campo
}

var logfile *Log
var offsetCounter uint64 = 0

func WriteLGHandler(w http.ResponseWriter, r *http.Request) {
	var requestData struct {
		Record Record `json:"record"`
	}

	// Deserializar el JSON
	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		http.Error(w, "Error desmashaleando el JSON", http.StatusBadRequest)
		return
	}

	record := requestData.Record
	record.Offset = offsetCounter
	offsetCounter++

	// Agregamos el registro al log
	logfile.mu.Lock()
	defer logfile.mu.Unlock()
	apiRecord := &api.Record{
		Value: record.Value, // Ajusta esto según cómo tengas estructurado `api.Record`
	}
	_, err = logfile.logFile.Append(apiRecord)
	if err != nil {
		http.Error(w, "Error al escribir en el log", http.StatusInternalServerError)
		return
	}

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
		http.Error(w, "404: Not desmashaleado JSON", http.StatusBadRequest)
		return
	}

	// Leemos el registro desde el log
	logfile.mu.Lock()
	defer logfile.mu.Unlock()

	record, err := logfile.logFile.Read(*requestData.Offset)
	if err != nil {
		http.Error(w, "404: Not found", http.StatusNotFound)
		return
	}

	response := map[string]Record{
		"record": {
			Value:  record.Value,
			Offset: record.Offset,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func startServer() {
	http.HandleFunc("/write", WriteLGHandler)
	http.HandleFunc("/read", ReadLGHandler)

	log.Println("Server ON -> http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server OFF -> ERROR: %v", err)
	}
}

func main() {
	// Configuración del log
	config := Logpk.Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxIndexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 1024,
			MaxIndexBytes: 1024,
			InitialOffset: 0,
		},
	}

	logDir := "temp/mi_log" // Cambia esto según sea necesario
	var err error
	logfile = &Log{}

	logfile.logFile, err = Logpk.NewLog(logDir, config) // Maneja los dos valores devueltos
	if err != nil {
		log.Fatalf("Error al inicializar el log: %v", err)
	}

	startServer()
}
