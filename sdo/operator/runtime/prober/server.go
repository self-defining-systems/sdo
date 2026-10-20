package prober

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// API paths served by the prober.
const (
	PathHealth    = "/healthz"
	PathWindows   = "/v1/windows"
	PathBursts    = "/v1/bursts"
	PathReset     = "/v1/reset"
	PathPrimitive = "/v1/primitive"
	DefaultPort   = 8080
)

// WindowsResponse is the body of GET /v1/windows.
type WindowsResponse struct {
	Windows map[string]traffic.Window `json:"windows"`
}

// Handler serves the prober API.
func Handler(prober *Prober) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(PathHealth, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc(PathWindows, func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			http.Error(w, "use GET", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, WindowsResponse{Windows: prober.Windows()})
	})
	mux.HandleFunc(PathReset, func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		prober.Reset()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc(PathBursts, func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var burst BurstRequest
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
	})
	mux.HandleFunc(PathPrimitive, func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		var probe PrimitiveRequest
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
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// ParseWorkloads decodes workload documents (JSON) and rejects duplicates.
func ParseWorkloads(documents map[string][]byte) ([]traffic.Workload, error) {
	workloads := make([]traffic.Workload, 0, len(documents))
	for name, document := range documents {
		workload, err := traffic.ParseWorkloadJSON(document)
		if err != nil {
			return nil, fmt.Errorf("workload %s: %w", name, err)
		}
		if workload.Name != name {
			return nil, fmt.Errorf("workload file %s declares name %q", name, workload.Name)
		}
		workloads = append(workloads, workload)
	}
	return workloads, nil
}

// Main is the prober binary's entry point. The controller builder generates
// a main package that calls it with the application's generators and the
// workload documents compiled in, so the pod needs no repository access.
func Main(catalog traffic.Catalog, workloadDocuments map[string][]byte) {
	flags := flag.NewFlagSet("sdo-prober", flag.ExitOnError)
	namespace := flags.String("namespace", "", "application namespace whose Services the scenarios call")
	clusterDomain := flags.String("cluster-domain", "cluster.local", "cluster DNS domain")
	listen := flags.String("listen", fmt.Sprintf(":%d", DefaultPort), "address of the prober API")
	check := flags.Bool("check", false, "validate generators and workloads, then exit")
	_ = flags.Parse(os.Args[1:])
	if err := run(*namespace, *clusterDomain, *listen, *check, catalog, workloadDocuments); err != nil {
		fmt.Fprintln(os.Stderr, "sdo-prober:", err)
		os.Exit(1)
	}
}

func run(namespace string, clusterDomain string, listen string, check bool, catalog traffic.Catalog, documents map[string][]byte) error {
	workloads, err := ParseWorkloads(documents)
	if err != nil {
		return err
	}
	if check {
		namespace = "check"
	}
	prober, err := New(Config{Namespace: namespace, ClusterDomain: clusterDomain, Catalog: catalog, Workloads: workloads})
	if err != nil {
		return err
	}
	if check {
		if failures := traffic.CheckCatalog(context.Background(), catalog); len(failures) > 0 {
			return errors.Join(failures...)
		}
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	server := &http.Server{Addr: listen, Handler: Handler(prober), ReadHeaderTimeout: 5 * time.Second}
	go prober.Run(ctx)
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	encoder := json.NewEncoder(os.Stdout)
	_ = encoder.Encode(map[string]any{"prober": "started", "namespace": namespace, "scenarios": len(catalog), "workloads": len(workloads)})
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
