package validate

import (
	"path/filepath"
	"strings"
	"testing"
)

const readerSource = "package detector\n\nimport (\n\t\"context\"\n\n\t\"sdo.dev/operator/sdk/appstate\"\n)\n\ntype Reader struct{}\n\nfunc (Reader) ReadState(context.Context, appstate.View) (appstate.Values, error) {\n\treturn appstate.Values{}, nil\n}\n"

const requireSource = "package detector\n\nimport (\n\t\"sdo.dev/operator/sdk\"\n\t\"sdo.dev/operator/sdk/appstate\"\n)\n\nfunc use(view sdk.DetectionContext) { _, _ = appstate.Require(view, \"settings\") }\n"

func TestReaderClassRuleBindsReadersToHealthDetectors(t *testing.T) {
	for _, tc := range []struct {
		name, class, source string
		want                bool
	}{
		{"incident reader", "incidents", readerSource, true},
		{"incident reader through the interface name", "incidents", "package detector\n\nimport \"sdo.dev/operator/sdk/appstate\"\n\nvar _ appstate.Reader = Reader{}\n", true},
		{"health reader", "health", readerSource, false},
		{"incident detector that only requires a reader's state", "incidents", requireSource, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := ".sdo/diagnostics/detectors/" + tc.class + "/probe/detector.go"
			write(t, root, path, tc.source)
			ds := readerClassDiagnostics(root, []string{path})
			if got := len(ds) > 0; got != tc.want {
				t.Fatalf("diagnostics = %v, want refusal=%v", ds, tc.want)
			}
			for _, d := range ds {
				if d.Rule != RuleDetectorReaderClass || !strings.Contains(d.Message, "health") || filepath.ToSlash(d.Path) != path {
					t.Errorf("diagnostic = %+v", d)
				}
			}
		})
	}
}
