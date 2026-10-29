package validate

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

func TestReplayOffIgnoresCorpusButRetainsSourceChecks(t *testing.T) {
	for _, replay := range []string{"", "on", "off"} {
		t.Run("mode="+replay, func(t *testing.T) {
			root := goodTree(t)
			remove(t, root, ".sdo/playbooks/base/scripts")
			write(t, root, ".sdo/playbooks/base/diagnose.go", "package base\n\ntype Diagnose struct{}\n\nfunc (Diagnose) Spec() {}\nfunc (Diagnose) Diagnose() {}\n")
			write(t, root, ".sdo/corpus/broken/labels.json", "not json")
			write(t, root, ".sdo/corpus/broken/ignored.go", "package ignored\nfunc  Ignored() {}\n")
			ds, err := Validate(context.Background(), Request{Root: root, Layers: Structural | Semantic, Replay: replay})
			if err != nil {
				t.Fatal(err)
			}
			invalid := false
			for _, d := range ds {
				invalid = invalid || strings.Contains(d.Message, "labels.json")
				if replay == "off" && strings.HasPrefix(d.Path, ".sdo/corpus/") {
					t.Fatalf("off checked inert corpus: %v", d)
				}
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

func TestReplayOffCorpusNeverEntersPublicationOrWorkerSnapshot(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "accepted memory")
	write(t, root, ".sdo/corpus/not-a-case/ignored.go", "package inert\nfunc  Inert() {}\n")
	called := false
	ds, err := Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: Human, Phase: ImportPhase, Replay: "off"}, func(_ context.Context, r Request) ([]Diagnostic, error) {
		called = true
		if r.Replay != "off" {
			t.Fatal("lost replay policy")
		}
		return nil, nil
	})
	if err != nil || len(ds) != 0 || !called {
		t.Fatalf("inert corpus blocked publication: %v %v %v", ds, err, called)
	}
}

func TestReplayOffImportStillRequiresScriptReceiptProvenance(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "accepted memory")
	copyTree(t, "../../../../sdo/skills/sdo-playbook-authoring/assets/examples/playbooks/crashloop-script-repair", filepath.Join(root, ".sdo/playbooks/crashloop-script-repair"))
	if _, err := layout.WritePlaybookIndex(root); err != nil {
		t.Fatal(err)
	}
	ds, err := Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: Human, Phase: ImportPhase, Replay: "off"}, func(context.Context, Request) ([]Diagnostic, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range ds {
		found = found || d.Rule == RuleProgramMitigationRepair
	}
	if !found {
		t.Fatalf("import bypassed script receipt proof: %v", ds)
	}
}
