package layout

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadPlaybookRetirementOnlyRequiresLifecycleMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		retired    bool
	}{
		{"legacy program", "---\nfault_class: example\n---\nLegacy program.\n", false},
		{"large body", "---\nschema_version: 1\n---\n" + strings.Repeat("x", 300*1024), false},
		{"retired legacy program", "---\nfault_class: example\nstatus: retired\nretired_reason: failed\n---\nLegacy program.\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, PlaybookFileName), []byte(tc.text), 0644); err != nil {
				t.Fatal(err)
			}
			retired, err := ReadPlaybookRetired(root)
			if err != nil || retired != tc.retired {
				t.Fatalf("retired=%v want=%v: %v", retired, tc.retired, err)
			}
		})
	}
}
