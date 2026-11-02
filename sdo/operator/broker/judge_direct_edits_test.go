package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
)

func TestFakeJudgeDirectEditsBootstrapAndRefinementOwnership(t *testing.T) {
	ctx := context.Background()
	b, root := judgeBroker(t)
	proposal, err := b.CreateProposalForPhase(ctx, "judge-bootstrap", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	fakeJudgeEdits(t, proposal.Path)
	receipt, err := b.Publish(ctx, "judge-bootstrap", HealthJudge, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"service-endpoints", "workload-health", "service-reachability", "traffic-links"} {
		path := ".sdo/diagnostics/detectors/health/" + id + "/manifest.yaml"
		if !slices.Contains(receipt.ChangedPaths, path) {
			t.Fatalf("Go bootstrap file missing from receipt: %s", path)
		}
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Fatal(err)
		}
	}
	source := ".sdo/diagnostics/detectors/health/health-objective/detector.go"
	proposal, err = b.CreateProposalForPhase(ctx, "judge-refine", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeJudgeFile(t, proposal.Path, source, "package objective\n// refined without changing registration\n")
	if _, err := b.Publish(ctx, "judge-refine", HealthJudge, InitialPhase); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, source))
	if err != nil || !strings.Contains(string(data), "refined") {
		t.Fatalf("refinement missing: %s %v", data, err)
	}
	for _, path := range []string{"app.txt", ".sdo/goal.md", ".sdo/playbooks/base/README.md", ".sdo/outcomes/forbidden.json", ".sdo/diagnostics/detectors/incidents/fault/detector.go"} {
		t.Run(path, func(t *testing.T) {
			id := "forbidden-" + strings.ReplaceAll(path, "/", "-")
			p, err := b.CreateProposalForPhase(ctx, id, InitialPhase)
			if err != nil {
				t.Fatal(err)
			}
			writeJudgeFile(t, p.Path, path, "forbidden change\n")
			_, err = b.Publish(ctx, id, HealthJudge, InitialPhase)
			var failure *validate.Failure
			if !errors.As(err, &failure) {
				t.Fatalf("expected typed ownership rejection: %v", err)
			}
			found := false
			for _, d := range failure.Diagnostics {
				found = found || d.Rule == validate.RuleOwnershipDenied
			}
			if !found {
				t.Fatalf("missing ownership diagnostic: %v", failure)
			}
			if err := b.ReleaseProposal(id, InitialPhase); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJudgeCheckDoesNotOverwriteAuthoredGenericPolicy(t *testing.T) {
	ctx := context.Background()
	b, _ := judgeBroker(t)
	proposal, err := b.CreateProposalForPhase(ctx, "generic-conflict", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	fakeJudgeEdits(t, proposal.Path)
	if err := memory.PrepareHealth(proposal.Path); err != nil {
		t.Fatal(err)
	}
	path := ".sdo/diagnostics/detectors/health/service-endpoints/detector.go"
	before, err := os.ReadFile(filepath.Join(proposal.Path, path))
	if err != nil {
		t.Fatal(err)
	}
	authored := string(before) + "\n// authored policy override\n"
	writeJudgeFile(t, proposal.Path, path, authored)
	_, err = b.Publish(ctx, "generic-conflict", HealthJudge, InitialPhase)
	var failure *validate.Failure
	if !errors.As(err, &failure) || len(failure.Diagnostics) != 1 || failure.Diagnostics[0].Rule != validate.RuleGenericHealth {
		t.Fatalf("expected named generic-policy rejection: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(proposal.Path, path))
	if err != nil || string(after) != authored {
		t.Fatalf("authored file overwritten: %s %v", after, err)
	}
}

func TestJudgeBootstrapInstallsMetricsAlertsDetectorOnlyWhenEnabled(t *testing.T) {
	ctx := context.Background()
	alerts := ".sdo/diagnostics/detectors/health/metrics-alerts/manifest.yaml"
	for _, enabled := range []bool{false, true} {
		b, root := judgeBroker(t)
		b.MetricsDetectors = enabled
		proposal, err := b.CreateProposalForPhase(ctx, "judge-metrics", InitialPhase)
		if err != nil {
			t.Fatal(err)
		}
		fakeJudgeEdits(t, proposal.Path)
		receipt, err := b.Publish(ctx, "judge-metrics", HealthJudge, InitialPhase)
		if err != nil {
			t.Fatalf("enabled=%v: %v", enabled, err)
		}
		_, statErr := os.Stat(filepath.Join(root, alerts))
		if installed := statErr == nil; installed != enabled || slices.Contains(receipt.ChangedPaths, alerts) != enabled {
			t.Fatalf("enabled=%v but installed=%v (receipt %v)", enabled, installed, receipt.ChangedPaths)
		}
	}
}
