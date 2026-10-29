package validate

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplayOffIgnoresCorpusButRetainsSourceChecks(t *testing.T) {
	for _, replay := range []string{"", "on", "off"} {
		t.Run("mode="+replay, func(t *testing.T) {
			root := goodTree(t)
			remove(t, root, ".sdo/playbooks/base/scripts")
			write(t, root, ".sdo/playbooks/base/diagnose.go", "package base\n\ntype Diagnose struct{}\n\nfunc (Diagnose) Spec() {}\nfunc (Diagnose) Diagnose() {}\n")
			write(t, root, ".sdo/corpus/broken/labels.json", "not json")
			ds, err := Validate(context.Background(), Request{Root: root, Layers: Structural | Semantic, Replay: replay})
			if err != nil {
				t.Fatal(err)
			}
			invalid := false
			for _, d := range ds {
				invalid = invalid || strings.Contains(d.Message, "labels.json")
			}
			if invalid != (replay != "off") {
				t.Fatalf("replay %q corpus validation: %v", replay, ds)
			}
			write(t, root, ".sdo/playbooks/base/diagnose.go", "package base\nimport \"os/exec\"\ntype Diagnose struct{}\nfunc(Diagnose)Spec(){}\nfunc(Diagnose)Diagnose(){}\n")
			ds, err = Validate(context.Background(), Request{Root: root, Layers: Structural | Semantic, Replay: replay})
			if err != nil {
				t.Fatal(err)
			}
			forbidden, format := false, false
			for _, d := range ds {
				forbidden = forbidden || strings.Contains(d.Message, "may not import")
				format = format || d.Rule == "go.format"
			}
			if !forbidden || !format {
				t.Fatalf("source gates skipped: %v", ds)
			}
		})
	}
}

func TestReplayOffDoesNotSelectHealthyCapture(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", "package cache\n")
	var gaps []Diagnostic
	r, err := selectCompileBaseline(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, Replay: "off", HealthyBaseline: filepath.Join(root, "missing"), CoverageGaps: &gaps})
	if err != nil || r.HealthyBaseline != "" || len(gaps) != 0 {
		t.Fatalf("off still selected replay: %+v %v %v", r, err, gaps)
	}
}
