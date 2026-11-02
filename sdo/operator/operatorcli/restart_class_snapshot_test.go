package operatorcli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// RT1: sweep the durable healthy-capture transport around the historical MiB
// boundary. A restart must retain each complete observation, never a truncation.
func TestRestartClassSnapshotTransport(t *testing.T) {
	for _, size := range []int{(1 << 20) - 1024, (1 << 20) + 1024, 2 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			document := json.RawMessage(`{"namespace":"synthetic","configMaps":[{"metadata":{"name":"fixture"},"data":{"asset":"` + strings.Repeat("x", size) + `"}}]}`)
			files := map[string]json.RawMessage{}
			for i := 0; i < 3; i++ {
				files[fmt.Sprintf("healthy-%d.json", i)] = document
			}
			input, err := json.Marshal(files)
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			for restart := 0; restart < 2; restart++ {
				if err := RunBaselineWrite(bytes.NewReader(input), root); err != nil {
					t.Fatalf("restart=%d complete snapshot=%d: %v", restart, size, err)
				}
				for name, want := range files {
					got, err := os.ReadFile(filepath.Join(root, name))
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("restart=%d lost snapshot %s: bytes=%d err=%v", restart, name, len(got), err)
					}
				}
			}
		})
	}
}
