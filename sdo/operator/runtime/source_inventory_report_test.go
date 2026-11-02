package runtime

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceInventoryReadOnlyReport(t *testing.T) {
	declared, live := hotelInventory()
	index := NewSourceInventory("app", "accepted", declared)
	live = append(live, latentInventoryObjects()...)
	path := filepath.Join(t.TempDir(), "source-drift.json")
	var output bytes.Buffer
	if err := WriteSourceInventoryReport(&output, path, index, live); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "5 unexplained objects") || !strings.Contains(output.String(), path) {
		t.Fatal(output.String())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Healthy  bool              `json:"healthy"`
		Findings []json.RawMessage `json:"findings"`
	}
	if err := json.Unmarshal(data, &report); err != nil || report.Healthy || len(report.Findings) != 5 {
		t.Fatalf("report=%s error=%v", data, err)
	}
	if err := WriteSourceInventoryReport(&output, "relative.json", index, live); err == nil {
		t.Fatal("accepted an ambiguous output path")
	}
}

