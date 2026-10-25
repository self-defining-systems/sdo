package validate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRetiredPlaybookMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		valid        bool
	}{
		{"legacy", "", true}, {"active", "status: active\n", true},
		{"retired", "status: retired\nretired_reason: mitigation was undone\n", true},
		{"missing reason", "status: retired\n", false},
		{"blank reason", "status: retired\nretired_reason: '  '\n", false},
		{"numeric reason", "status: retired\nretired_reason: 42\n", false},
		{"unknown status", "status: disabled\n", false},
		{"numeric status", "status: 7\n", false},
		{"orphan reason", "retired_reason: failed\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := goodTree(t)
			p := filepath.Join(root, ".sdo/playbooks/base/README.md")
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			write(t, root, ".sdo/playbooks/base/README.md", strings.Replace(string(data), "schema_version: 1\n", "schema_version: 1\n"+tc.fields, 1))
			got := diagnose(t, Request{Root: root, Layers: Structural | Semantic})
			if tc.valid && len(got) != 0 {
				t.Fatalf("valid metadata rejected: %v", got)
			}
			if !tc.valid && len(got) == 0 {
				t.Fatal("invalid retirement metadata accepted")
			}
		})
	}
}
