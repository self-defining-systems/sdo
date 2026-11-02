package broker

import (
	"context"
	"go/format"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeJudgeFile(t *testing.T, root, path, content string) {
	t.Helper()
	if strings.HasSuffix(path, ".go") {
		if formatted, err := format.Source([]byte(content)); err == nil {
			content = string(formatted)
		}
	}
	target := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// This fake authors final files just as an external judge does.
func fakeJudgeEdits(t *testing.T, root string) {
	t.Helper()
	writeJudgeFile(t, root, ".sdo/diagnostics/detectors/health/health-objective/manifest.yaml", detectorYAML("health", "health-objective", 2, "health-objective"))
	writeJudgeFile(t, root, ".sdo/diagnostics/detectors/health/health-objective/detector.go", "package objective\n// judge authored\n")
	writeJudgeFile(t, root, ".sdo/playbooks/health-objective/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: health-objective-failure\noriginating_incident: lifecycle-bootstrap\n---\nRepair <AFFECTED_RESOURCE>.\n")
	writeJudgeFile(t, root, ".sdo/playbooks/README.md", "# Playbooks\n\n- [base](base/README.md)\n- [health-objective](health-objective/README.md)\n")
	writeJudgeFile(t, root, ".sdo/diagnostics/traffic/workloads/links.yaml", "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: links\npurpose: link-probe\nlinks:\n  - from: api\n    to: db\n    port: 80\n")
}

func judgeBroker(t *testing.T) (*Broker, string) {
	t.Helper()
	root := repository(t)
	memoryFixture(t, root)
	if err := os.RemoveAll(filepath.Join(root, ".sdo/diagnostics/detectors")); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", "-A")
	git(t, root, "commit", "-qm", "before diagnostics")
	b, err := New(root, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return b, root
}
