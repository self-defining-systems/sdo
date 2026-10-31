package prober

import (
	"fmt"
	"sigs.k8s.io/yaml"
	"strings"
	"testing"
)

func TestLinkWorkloadYAML(t *testing.T) {
	for _, tc := range []struct {
		options   string
		errorText string
	}{
		{"protocol: http\n    http: {path: /health, statusMin: 200, statusMax: 299}", ""},
		{"protocol: grpc\n    grpc: {tls: true}", ""},
		{"protocol: bogus", "unknown protocol"},
		{"protocol: redis\n    http: {path: /}", "http options require"},
		{"protocol: http\n    http: {statusMin: 400, statusMax: 200}", "status range"},
		{"protocol: grpc\n    grpc: {typo: true}", "unknown field"},
	} {
		doc := fmt.Sprintf("apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: links\npurpose: link-probe\nlinks:\n  - from: web\n    to: backend\n    port: 1234\n    %s\n", tc.options)
		b, err := yaml.YAMLToJSON([]byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		_, err = ParseWorkloads(map[string][]byte{"links": b})
		if tc.errorText == "" {
			if err != nil {
				t.Fatal(err)
			}
		} else if err == nil || !strings.Contains(err.Error(), tc.errorText) {
			t.Fatalf("%s: %v", tc.options, err)
		}
	}
}
