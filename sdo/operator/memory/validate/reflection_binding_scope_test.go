package validate

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The memory check must judge detector coverage by the controller's binding
// rule: a finding's named bindings reach only the playbooks it lists. Here a
// journey detector names Service demo/front for its own playbook, and
// reflection publishes service-targetport-mismatch (whose case binds front)
// without listing it there. The controller would never bind that program from
// the finding, so the check must say so and name the detector to edit; once
// the detector declares the playbook, the gap closes.
func TestDetectorCoverageUsesTheControllersBindingScope(t *testing.T) {
	front := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "front"}
	gateway := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "gateway"}
	at := time.Unix(100, 0)
	outcome := &contracts.OutcomeRecord{IncidentId: "first", Classification: "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{front}}},
		Findings: []*contracts.Finding{{DetectorId: "journey", Status: "active", Fingerprint: "journey", PrimaryResource: gateway,
			PlaybookIds: []string{"missing-configmap"}, ParameterBindings: map[string]*contracts.ObjectRef{"service": front}}},
		RepairActions:            []*contracts.RepairActionReceipt{{ActionId: "fixture-front", Success: true, Resources: []*contracts.ObjectRef{front}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}},
		FinalHealthDetectorState: []*contracts.DetectorEvaluation{{DetectorId: "journey", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}},
	}
	tree := func(t *testing.T, playbooks string) string {
		root := t.TempDir()
		copyTree(t, gateProgramFixture, filepath.Join(root, ".sdo"))
		manifest := gateRead(t, root, ".sdo/diagnostics/detectors/incidents/routing-relations/manifest.yaml")
		manifest = strings.Replace(manifest, "id: routing-relations", "id: journey", 1)
		manifest = strings.Replace(manifest, "  - service-targetport-mismatch\n", playbooks, 1)
		write(t, root, ".sdo/diagnostics/detectors/incidents/journey/manifest.yaml", manifest)
		write(t, root, ".sdo/diagnostics/detectors/incidents/journey/detector.go", "package journey\n")
		return root
	}

	gaps := ReflectionCoverageGaps(tree(t, "  - missing-configmap\n"), outcome, gateUpdated("service-targetport-mismatch"))
	i := slices.IndexFunc(gaps, func(d Diagnostic) bool { return d.Rule == RuleReflectionDetectorRequired })
	if i < 0 {
		t.Fatalf("no %s for a playbook the finding does not list: %v", RuleReflectionDetectorRequired, gaps)
	}
	if !strings.Contains(gaps[i].Message, "journey -> service-targetport-mismatch") || !strings.Contains(gaps[i].Hint, "possiblePlaybooks") {
		t.Fatalf("gap does not name the detector to edit: %s (hint %q)", gaps[i].Message, gaps[i].Hint)
	}

	listed := tree(t, "  - missing-configmap\n  - service-targetport-mismatch\n")
	if gaps := ReflectionCoverageGaps(listed, outcome, gateUpdated("service-targetport-mismatch")); slices.ContainsFunc(gaps, func(d Diagnostic) bool { return d.Rule == RuleReflectionDetectorRequired }) {
		t.Fatalf("detector that now lists the playbook still reported: %v", gaps)
	}
}
