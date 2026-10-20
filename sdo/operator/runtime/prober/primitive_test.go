package prober_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/traffic"
)

func TestPrimitiveProbeStaysInConfiguredNamespaceAndUsesProberNetwork(t *testing.T) {
	application := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer application.Close()
	p, err := prober.New(prober.Config{Namespace: "app", Catalog: catalog(), BaseURL: func(traffic.Target) string { return application.URL }})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(prober.Handler(p))
	defer api.Close()
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	call := func(ref sdk.ObjectRef, request *program.HTTPProbe) int {
		t.Helper()
		body, _ := json.Marshal(prober.PrimitiveRequest{To: ref, Port: 80, HTTP: request})
		response, err := http.Post(api.URL+prober.PathPrimitive, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode == http.StatusOK {
			var result program.ProbeResult
			if err := json.NewDecoder(response.Body).Decode(&result); err != nil || !result.Success {
				t.Fatalf("probe: %+v %v", result, err)
			}
		}
		return response.StatusCode
	}
	if got := call(ref, &program.HTTPProbe{Method: "GET", Path: "/health"}); got != http.StatusOK {
		t.Fatalf("in-namespace probe: %d", got)
	}
	ref.Namespace = "other"
	if got := call(ref, &program.HTTPProbe{Method: "GET", Path: "/health"}); got != http.StatusBadRequest {
		t.Fatalf("out-of-namespace probe: %d", got)
	}
	ref.Namespace = "app"
	if _, err := p.Primitive(context.Background(), prober.PrimitiveRequest{To: ref, Port: 80, HTTP: &program.HTTPProbe{Method: "POST", Path: "/"}}); err == nil {
		t.Fatal("mutating HTTP method was allowed")
	}
}
