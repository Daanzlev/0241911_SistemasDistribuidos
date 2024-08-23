package main

import (
	//Server
	"encoding/json"
	"net/http"

	//Log
	"log"
	"sync"
	//File
	//"os"
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

	// Desmashaleamos el JSON
	err := json.NewDecoder(r.Body).Decode(&record)
	if err != nil {
		http.Error(w, "Error desmashaleando el JSON", http.StatusBadRequest)
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
		http.Error(w, "404: Not desmashaleado JSON", http.StatusBadRequest)
		return
	}

	// Buscamos en el LOG el offset
	logfile.mu.Lock()
	defer logfile.mu.Unlock()

	for _, record := range logfile.records {
		if record.Offset == *requestData.Offset {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]Record{"record": record})
			return
		}
	}

	http.Error(w, "404: Not found", http.StatusNotFound)
}
func startServer() {
	http.HandleFunc("/write", WriteLGHandler)
	http.HandleFunc("/read", ReadLGHandler)

	log.Println("Server ON -> http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server OFF -> ERROR", err)
	}
}

//Intentamos jugar con un archivo para guardar, F
/*
func saveLogToFile() error {
	file, err := os.Create("log.txt")
	if err != nil {
		return err
	}
	defer file.Close()

	logfile.mu.Lock()
	defer logfile.mu.Unlock()

	for _, record := range logfile.records {
		_, err := file.WriteString(string(record.Value) + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}*/
func main() {
	startServer()
	/*err := saveLogToFile()
	if err != nil {
		log.Fatalf("Error saving log to file: %v", err)
	} else {
		log.Println("Log successfully saved to log.txt")
	}*/
}
