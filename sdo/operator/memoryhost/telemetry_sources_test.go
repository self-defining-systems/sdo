package memoryhost

import (
	"bytes"
	"context"
	"testing"
)

func TestReadyCarriesTheCompiledTelemetrySources(t *testing.T) {
	server, err := NewServer("commit", "digest", nil)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := server.Ready(context.Background(), nil)
	if err != nil || len(ready.GetTelemetrySources()) != 0 {
		t.Fatalf("a host built without bindings reports %q, %v", ready.GetTelemetrySources(), err)
	}
	sources := []byte("version: 1\nsources: []\n")
	server.WithTelemetrySources(sources)
	sources[0] = 'X' // the host keeps its own copy
	ready, err = server.Ready(context.Background(), nil)
	if err != nil || !bytes.Equal(ready.GetTelemetrySources(), []byte("version: 1\nsources: []\n")) {
		t.Fatalf("Ready telemetry sources = %q, %v", ready.GetTelemetrySources(), err)
	}
}
