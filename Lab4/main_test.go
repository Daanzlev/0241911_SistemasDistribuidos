package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteLGHandler(t *testing.T) {
	record := Record{Value: []byte("Hello, World!")}

	body, _ := json.Marshal(record)

	req, err := http.NewRequest("POST", "/write", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("Error creando la solicitud: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(WriteLGHandler)

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("Código de estado inesperado: got %v want %v", status, http.StatusCreated)
	}

	var responseRecord Record
	if err := json.NewDecoder(rr.Body).Decode(&responseRecord); err != nil {
		t.Errorf("Error deserializando la respuesta: %v", err)
	}

	if !bytes.Equal(responseRecord.Value, record.Value) {
		t.Errorf("Valor inesperado: got %v want %v", responseRecord.Value, record.Value)
	}
}

func TestReadLGHandler(t *testing.T) {
	record := Record{Value: []byte("Hello, World!"), Offset: 0}
	logfile.records = append(logfile.records, record)

	body, _ := json.Marshal(struct {
		Offset uint64 `json:"offset"`
	}{Offset: 0})

	req, err := http.NewRequest("POST", "/read", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("Error creando la solicitud: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(ReadLGHandler)

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Código de estado inesperado: got %v want %v", status, http.StatusOK)
	}

	var response struct {
		Record Record `json:"record"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Errorf("Error deserializando la respuesta: %v", err)
	}

	if !bytes.Equal(response.Record.Value, record.Value) {
		t.Errorf("Valor inesperado: got %v want %v", response.Record.Value, record.Value)
	}
}
