//go:build integration

package broker

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sdo.dev/operator/patchset"
)

func TestProposalCheckoutMaterializesPatchesAndPartitionsChanges(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	w.addPatch(t, "deployer", replaceLine("v05", "v05 patched"))

	for _, phase := range []Phase{ProposalPhase, InitialPhase} {
		p, err := w.broker.CreateProposalForPhase(ctx, "inc-"+string(phase), phase)
		if err != nil {
			t.Fatal(err)
		}
		text, err := os.ReadFile(filepath.Join(p.Path, "lib", "vendor.txt"))
		if err != nil || !strings.Contains(string(text), "v05 patched\n") {
			t.Fatalf("%s checkout holds %q, %v; want the patched vendor file", phase, text, err)
		}
		if part, err := PartitionChanges(ctx, p.Path, p.BaseCommit); err != nil || len(part.SDO)+len(part.Scratch) != 0 {
			t.Fatalf("%s: a fresh checkout has changes %+v, %v", phase, part, err)
		}
		if paths, _ := w.broker.ChangedPaths(ctx, "inc-"+string(phase), phase); len(paths) != 0 {
			t.Fatalf("%s: ChangedPaths reports the materialized files: %v", phase, paths)
		}
	}

	p, err := w.broker.CreateProposalForPhase(ctx, "inc-proposal", ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{"app.txt": "one\ntwo\nthree fixed\n", ".sdo/playbooks/p.md": "note\n"})
	part, err := w.broker.ProposalPartition(ctx, "inc-proposal", ProposalPhase)
	if err != nil || !reflect.DeepEqual(part.Scratch, []string{"app.txt"}) || !reflect.DeepEqual(part.SDO, []string{".sdo/playbooks/p.md"}) {
		t.Fatalf("partition = %+v, %v", part, err)
	}
	feedback, err := ScratchFeedback(ctx, p.Path)
	if err != nil || !strings.Contains(feedback, UncapturedRule) || !strings.Contains(feedback, "app.txt") || !strings.Contains(feedback, "patch create") {
		t.Fatalf("feedback = %q, %v", feedback, err)
	}

	created, err := patchset.Create(ctx, p.Path, patchset.CreateRequest{Owner: patchset.OwnerResponder, Incident: "inc-proposal", Reason: patchReason})
	if err != nil {
		t.Fatal(err)
	}
	if feedback, err := ScratchFeedback(ctx, p.Path); err != nil || feedback != "" {
		t.Fatalf("feedback after capture = %q, %v", feedback, err)
	}
	part, err = w.broker.ProposalPartition(ctx, "inc-proposal", ProposalPhase)
	if err != nil || len(part.Scratch) != 0 || !reflect.DeepEqual(part.SDO, []string{".sdo/deployment/patches/" + created.Name, ".sdo/playbooks/p.md"}) {
		t.Fatalf("partition after capture = %+v, %v", part, err)
	}
}

func TestReflectionCheckoutIsNotMaterialized(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	w.addPatch(t, "deployer", replaceLine("v05", "v05 patched"))
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-r", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := os.ReadFile(filepath.Join(p.Path, "lib", "vendor.txt"))
	if strings.Contains(string(text), "patched") {
		t.Fatal("reflection checkout was materialized")
	}
	if _, ok, err := patchset.LoadView(ctx, p.Path); ok || err != nil {
		t.Fatalf("view = %v, %v", ok, err)
	}
}

func TestUncapturedScratchNamesAPatchThatDoesNotApply(t *testing.T) {
	ctx := context.Background()
	w := newPatchWorld(t, false)
	p, err := w.broker.CreateProposalForPhase(ctx, "inc-bad", ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, p.Path, map[string]string{".sdo/deployment/patches/0001-bad.patch": "SDO-Patch: 1\n"})
	if _, err := UncapturedScratch(ctx, p.Path); err == nil {
		t.Fatal("a malformed series produced a scratch verdict")
	}
}
