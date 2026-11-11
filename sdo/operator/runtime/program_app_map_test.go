package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// A program reads the procedures the accepted architecture records next to
// the components, with typed parameters and the effect class.
func TestAcceptedProgramMapCarriesProcedures(t *testing.T) {
	doc := "# inventory\n\n" + appmap.BeginMarker + "\n```yaml\nversion: 1\ncomponents:\n  - {name: api, workload: Deployment/api, services: [api], holds_data: false}\nedges: []\noperations: []\nprobes: []\nprocedures:\n  - {id: self-check, component: api, container: app, command: [tool, check, \"{{level}}\"], params: [{name: level, type: int, default: 1}], effect: check, timeout: 30s, source: docs/ops.md}\n```\n" + appmap.EndMarker + "\n"
	read := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if len(args) != 2 || args[0] != "show" || args[1] != "abc:"+layout.ArchitectureFile {
			t.Fatalf("read %v", args)
		}
		return []byte(doc), nil
	}
	m, err := acceptedProgramMap(context.Background(), read, "repo", "abc")
	if err != nil {
		t.Fatal(err)
	}
	p, ok := m.Procedure("self-check")
	if !ok || p.Component != "api" || p.Effect != "check" || p.Timeout != 30*time.Second || len(p.Params) != 1 || p.Params[0].Default != 1 {
		t.Fatalf("procedures = %+v", m.Procedures)
	}
	if argv, err := p.Bind(nil); err != nil || argv[2] != "1" {
		t.Fatalf("argv = %q, err = %v", argv, err)
	}
}
