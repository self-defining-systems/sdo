package builder

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"strings"
	"testing"
)

func TestOperatorCorpusRequiresProductionClosedLoop(t *testing.T) {
	testutil.OfflineGo(t, "..")
	for _, valid := range []bool{true, false} {
		t.Run(map[bool]string{true: "accepted source", false: "Secret source refused"}[valid], func(t *testing.T) {
			root := programFixture(t)
			dir := filepath.Join(root, ".sdo/corpus/targetport-mismatch")
			write := func(name string, data []byte) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			write("operator.json", []byte(`{"schema_version":1,"origin":"operator","description":"Hand-written port mismatch"}`))
			source := `[{"apiVersion":"v1","kind":"Secret","metadata":{"name":"canary","namespace":"demo"}}]`
			if valid {
				source = `[{"apiVersion":"v1","kind":"Service","metadata":{"name":"front","namespace":"demo"},"spec":{"selector":{"app":"front"},"ports":[{"port":80,"targetPort":8080}]}}]`
			}
			write("accepted.json", []byte(source))
			// Check's fixed comparison retains both unrelated peers.
			var fixed, healthy map[string]any
			data, err := os.ReadFile(filepath.Join(dir, "fixed.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(data, &fixed); err != nil {
				t.Fatal(err)
			}
			data, err = os.ReadFile(filepath.Join(dir, "healthy.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(data, &healthy); err != nil {
				t.Fatal(err)
			}
			for k, v := range healthy {
				if list, ok := v.([]any); ok {
					old, _ := fixed[k].([]any)
					fixed[k] = append(old, list...)
				}
			}
			data, err = json.Marshal(fixed)
			if err != nil {
				t.Fatal(err)
			}
			write("fixed.json", data)
			w, err := buildProgramFixture(t, root)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			err = w.Check(context.Background())
			if valid && err != nil {
				t.Fatal(err)
			}
			if !valid && (err == nil || !strings.Contains(err.Error(), "Secret manifest refused")) {
				t.Fatalf("Secret source must fail production replay: %v", err)
			}
		})
	}
}
