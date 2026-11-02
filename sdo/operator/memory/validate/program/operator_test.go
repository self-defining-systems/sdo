package program

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOperatorCorpusProvenance(t *testing.T) {
	for _, name := range []string{"valid", "missing marker", "unknown field", "unknown version", "capture claim", "missing description", "missing source", "missing positive", "trailing JSON"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			fixture := "../../../../../tests/fixtures/sdo/playbook_program_app/.sdo/corpus/targetport-mismatch"
			if err := os.CopyFS(dir, os.DirFS(fixture)); err != nil {
				t.Fatal(err)
			}
			marker := `{"schema_version":1,"origin":"operator","description":"Hand-written fault"}`
			labels := `{"root_cause_program_ids":["service-targetport-mismatch"]}`
			switch name {
			case "unknown field":
				marker = `{"schema_version":1,"origin":"operator","description":"fault","trusted":true}`
			case "unknown version":
				marker = `{"schema_version":2,"origin":"operator","description":"fault"}`
			case "capture claim":
				labels = `{"root_cause_program_ids":["service-targetport-mismatch"],"provenance":{"incident_id":"fake"}}`
			case "missing description":
				marker = `{"schema_version":1,"origin":"operator"}`
			case "missing positive":
				labels = `{"no_match_program_ids":["service-targetport-mismatch"]}`
			case "trailing JSON":
				marker += " {}"
			}
			write := func(file, body string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, file), []byte(body), 0644); err != nil {
					t.Fatal(err)
				}
			}
			write("labels.json", labels)
			if name != "missing marker" {
				write("operator.json", marker)
			}
			if name != "missing source" {
				write("accepted.json", "[]")
			}
			marked, err := OperatorCorpus(dir)
			switch name {
			case "valid":
				if !marked || err != nil {
					t.Fatalf("valid operator corpus: %v %v", marked, err)
				}
			case "missing marker":
				if marked || err != nil {
					t.Fatalf("unmarked case became operator corpus: %v %v", marked, err)
				}
			default:
				if !marked || err == nil {
					t.Fatalf("invalid provenance accepted: %v %v", marked, err)
				}
			}
		})
	}
}
