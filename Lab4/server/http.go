package server

import (
	"encoding/json"
	"net/http"
	"strconv"

	api "Proyecto/api/v1"
	logpkg "Proyecto/log"
)

type HTTPServer struct {
	Log *logpkg.Log
}

func NewHTTPServer(addr string, log *logpkg.Log) *http.Server {
	httpSrv := &HTTPServer{
		Log: log,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/produce", httpSrv.handleProduce)
	mux.HandleFunc("/consume", httpSrv.handleConsume)

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}

func (s *HTTPServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req api.ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Error desmashaleando el JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Devolver el offset del nuevo registro
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]uint64{"offset": offset})
}

func (s *HTTPServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	// Obtener el offset de los parámetros de la URL
	offsetStr := r.URL.Query().Get("offset")
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid offset", http.StatusBadRequest)
		return
	}

	// Leer el registro desde el log
	record, err := s.Log.Read(offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Crear una respuesta intermedia para evitar el problema del Mutex
	resp := map[string]interface{}{"record": record}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
