// Package testprober provides an HTTP engine fixture for transport conformance tests only.
// Production accepted memory uses MemoryHostService exclusively.
package testprober

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	engine "sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/traffic"
)

// API paths served by the prober.
const (
	PathHealth    = "/healthz"
	PathWindows   = "/v1/windows"
	PathBursts    = "/v1/bursts"
	PathReset     = "/v1/reset"
	PathPrimitive = "/v1/primitive"
	PathDials     = "/v1/dials"
)

// WindowsResponse is the body of GET /v1/windows.
type WindowsResponse struct {
	Windows map[string]traffic.Window `json:"windows"`
}

// Handler serves the prober API.
func Handler(prober *engine.Prober) http.Handler {
	handlers := map[string]http.HandlerFunc{}
	handlers[PathHealth] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}
	handlers[PathWindows] = func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			http.Error(w, "use GET", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, WindowsResponse{Windows: prober.Windows()})
	}
	handlers[PathReset] = func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		prober.Reset()
		w.WriteHeader(http.StatusNoContent)
	}
	handlers[PathBursts] = func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var burst engine.BurstRequest
		decoder := json.NewDecoder(io.LimitReader(request.Body, 64*1024))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&burst); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, "decode burst request: "+err.Error(), http.StatusBadRequest)
			return
		}
		result, err := prober.Burst(request.Context(), burst)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
	handlers[PathDials] = func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var dials engine.DialRequest
		decoder := json.NewDecoder(io.LimitReader(request.Body, 64*1024))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&dials); err != nil {
			http.Error(w, "decode dial request: "+err.Error(), http.StatusBadRequest)
			return
		}
		result, err := prober.DialTargets(request.Context(), dials)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
	handlers[PathPrimitive] = func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var probe engine.PrimitiveRequest
		decoder := json.NewDecoder(io.LimitReader(request.Body, 8*1024))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&probe); err != nil {
			http.Error(w, "decode primitive request: "+err.Error(), http.StatusBadRequest)
			return
		}
		result, err := prober.Primitive(request.Context(), probe)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handler, ok := handlers[r.URL.Path]; ok {
			handler(w, r)
			return
		}
		http.NotFound(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
