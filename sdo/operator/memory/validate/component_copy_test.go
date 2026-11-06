package validate

import (
	"strings"
	"testing"
)

// Synthetic shapes of a learned per-component pair: reflection first learned
// a cache permission restore for the orders journey, then, on the same fault
// in the carts cache, published a second playbook and detector that differ
// only in the component they name (scenario, container, binding literal),
// plus formatting, local spelling and one extra command argument.

// componentArchitecture is the architecture Go writes from the rendered
// accepted source: the manifest inventory lines. Component names come from
// these lines, never from the source tree.
const componentArchitecture = `---
schema_version: 1
generated_at_commit: base
application: shop
---
# Manifest inventory

- Deployment/shop/orders-cache (source: raw)
- Deployment/shop/carts-cache (source: raw)
- Service/shop/orders-api (source: raw)
`

const componentWorkload = "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: health\npurpose: health-probe\nratePerSecond: 1\nscenarios:\n- id: orders-read\n  weight: 1\n- id: carts-read\n  weight: 1\n"

const componentDiagnose = `// Package orderscacheacl recognizes cache command denials in the orders read journey.
package orderscacheacl

import (
	"context"
	"fmt"
	"strings"

	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "orders-cache-acl", Params: []program.ParamSpec{{Name: "cacheDeployment", Kind: "Deployment", Required: true}}}
}

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["cacheDeployment"]
	window, ok := v.TrafficWindow("health")
	if !ok {
		return program.Verdict{}, fmt.Errorf("orders health traffic window unavailable")
	}
	for _, scenario := range window.Scenarios {
		if scenario.Scenario.ID != "orders-read" {
			continue
		}
		for _, sample := range scenario.Samples {
			if strings.Contains(strings.ToUpper(sample.Error), "NOPERM") {
				return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{
					Resource: ref,
					Fact:     "orders reads report a cache command denial",
					Observed: sample.Error,
				}}}, nil
			}
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
`

const componentMitigate = `package orderscacheacl

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	deployment := b.Objects["cacheDeployment"]
	var owned map[string]bool = map[string]bool{}
	for _, rs := range v.ReplicaSets() {
		for _, owner := range rs.OwnerReferences {
			if owner.Kind == "Deployment" && owner.Name == deployment.Name {
				owned[rs.Name] = true
			}
		}
	}
	var actions []program.Action
	for _, pod := range v.Pods() {
		if pod.Namespace != deployment.Namespace || pod.Status.Phase != corev1.PodRunning || len(pod.OwnerReferences) == 0 || !owned[pod.OwnerReferences[0].Name] {
			continue
		}
		podRef := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}
		probe := program.Command{Pod: podRef, Container: "orders-cache", Args: []string{"redis-cli", "GET", "probe"}}
		ensure := program.Command{Pod: podRef, Container: "orders-cache", Args: []string{"redis-cli", "ACL", "SETUSER", "default", "on", "+@all"}}
		actions = append(actions, program.EnsureExec{Observe: probe, Ensure: ensure, Expect: 0, Reason: "restore the orders cache permissions"})
	}
	return program.Plan{Actions: actions, Rationale: "probe the cache and restore permissions only when denied"}, nil
}
`

const componentDetector = `// Package orderscacheacldetector identifies cache command denials in orders read traffic.
package orderscacheacldetector

import (
	"context"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

type Detector struct{}

func New() sdk.Detector { return Detector{} }

func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "orders-cache-acl", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder,
		Description:         "detect cache NOPERM failures in the orders read journey",
		Watches:             []sdk.WatchKind{traffic.Watch},
		Interval:            2 * time.Second,
		Persistence:         sdk.PersistencePolicy{Firing: 1, Clearing: 2},
		Batching:            sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:           []string{"orders-cache-acl"},
		OriginatingIncident: "inc-0", OriginatingCommit: "abc",
	}
}

func (d Detector) Detect(ctx context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := traffic.NewDetector(d.Spec(), "health").Detect(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	filtered := findings[:0]
	for _, f := range findings {
		if f.RuleID != "scenario-slo.orders-read" || !strings.Contains(f.Evidence, "NOPERM") {
			continue
		}
		f.RuleID = "orders-cache-acl"
		f.Summary = "orders reads are denied cache permission"
		f.Fingerprint = "orders-cache-acl/" + f.PrimaryResource.Namespace
		f.ParameterBindings = map[string]sdk.ObjectRef{"cacheDeployment": {APIVersion: "apps/v1", Kind: "Deployment", Namespace: f.PrimaryResource.Namespace, Name: "orders-cache"}}
		f.Playbooks = []string{"orders-cache-acl"}
		filtered = append(filtered, f)
	}
	return filtered, nil
}
`

func componentManifest(id, playbook, origin string) string {
	return "apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\nid: " + id + "\nconstructor: New\nclass: incident\nowner: responder\nwatches:\n  - apiVersion: sdo.dev/v1alpha1\n    kind: SyntheticTraffic\ninterval: 2s\npersistence:\n  firing: 1\n  clearing: 2\nbatching:\n  severity: critical\n  debounce: 500ms\npossiblePlaybooks:\n  - " + playbook + "\noriginatingIncident: " + origin + "\noriginatingCommit: abc\n"
}

func componentReadme(id, origin string) string {
	return "---\nschema_version: 1\nowner: responder\nfault_class: " + id + "\noriginating_incident: " + origin + "\n---\n\nRestore cache command permissions.\n"
}

// toCarts is the per-component copy reflection published: every orders name
// becomes the carts one.
func toCarts(text string) string {
	return strings.NewReplacer("orderscacheacl", "cartscacheacl", "orders", "carts").Replace(text)
}

// gateComponentCopy commits the orders playbook and detector, then adds the
// carts files that change returns (nil skips one) in reflection. The carts
// files name their component, so every row also meets the literal rules.
func gateComponentCopy(name string, change func(t *testing.T, diagnose, mitigate, detector string) (string, string, string), rules ...string) gateRow {
	// The decision lists the new playbook only when the row adds one.
	decision := gateUpdated()
	return gateRow{name: name, kind: gateCheck, actor: Responder, phase: ReflectionPhase, rules: rules,
		baseMessage: gateOutcomeMessage,
		prepare: func(t *testing.T, r string) {
			write(t, r, ".sdo/arch.md", componentArchitecture)
			write(t, r, ".sdo/diagnostics/traffic/workloads/health.yaml", componentWorkload)
			write(t, r, ".sdo/playbooks/orders-cache-acl/diagnose.go", componentDiagnose)
			write(t, r, ".sdo/playbooks/orders-cache-acl/mitigate.go", componentMitigate)
			write(t, r, ".sdo/playbooks/orders-cache-acl/README.md", componentReadme("orders-cache-acl", "inc-0"))
			write(t, r, ".sdo/diagnostics/detectors/incidents/orders-cache-acl/detector.go", componentDetector)
			write(t, r, ".sdo/diagnostics/detectors/incidents/orders-cache-acl/manifest.yaml", componentManifest("orders-cache-acl", "orders-cache-acl", "inc-0"))
			generateIndex(t, r)
			gateOutcome(gateClean())(t, r)
		},
		edit: func(t *testing.T, r string) {
			diagnose, mitigate, detector := change(t, toCarts(componentDiagnose), toCarts(componentMitigate), toCarts(componentDetector))
			if diagnose != "" {
				write(t, r, ".sdo/playbooks/carts-cache-acl/diagnose.go", gateFormatted(t, diagnose))
				write(t, r, ".sdo/playbooks/carts-cache-acl/mitigate.go", gateFormatted(t, mitigate))
				write(t, r, ".sdo/playbooks/carts-cache-acl/README.md", componentReadme("carts-cache-acl", gateIncident))
				generateIndex(t, r)
				decision = gateUpdated("carts-cache-acl")
			}
			if detector != "" {
				playbook := "carts-cache-acl"
				if diagnose == "" {
					playbook = "orders-cache-acl"
					detector = strings.ReplaceAll(detector, `"carts-cache-acl"}`, `"orders-cache-acl"}`)
				}
				write(t, r, ".sdo/diagnostics/detectors/incidents/carts-cache-acl/detector.go", gateFormatted(t, detector))
				write(t, r, ".sdo/diagnostics/detectors/incidents/carts-cache-acl/manifest.yaml", componentManifest("carts-cache-acl", playbook, gateIncident))
			}
		},
		request: func(r *Request) {
			r.Decision = decision
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: gateIncident}
		}}
}

// componentRespell is what a second reflection changes besides names: layout,
// local spelling, a short variable declaration and one extra argument.
func componentRespell(diagnose, mitigate string) (string, string) {
	diagnose = strings.Replace(diagnose, "{{\n\t\t\t\t\tResource: ref,\n\t\t\t\t\tFact:     \"carts reads report a cache command denial\",\n\t\t\t\t\tObserved: sample.Error,\n\t\t\t\t}}}", "{{Resource: ref, Fact: \"carts read traffic reports denial\", Observed: sample.Error}}}", 1)
	mitigate = strings.Replace(mitigate, "var owned map[string]bool = map[string]bool{}", "owned := map[string]bool{}", 1)
	mitigate = strings.ReplaceAll(mitigate, "podRef", "target")
	return diagnose, mitigate
}

var componentCopyRows = []gateRow{
	gateComponentCopy("new playbook copies another for a different component", func(t *testing.T, diagnose, mitigate, _ string) (string, string, string) {
		diagnose, mitigate = componentRespell(diagnose, mitigate)
		if !strings.Contains(diagnose, "{{Resource: ref,") || !strings.Contains(mitigate, "owned := ") {
			t.Fatal("respelling did not apply")
		}
		return diagnose, mitigate, ""
	}, RulePlaybookComponentLiteral, RuleDuplicateRepair),
	gateComponentCopy("new playbook diagnoses the same failure on another component with a varied repair", func(t *testing.T, diagnose, mitigate, _ string) (string, string, string) {
		diagnose, mitigate = componentRespell(diagnose, mitigate)
		return diagnose, strings.Replace(mitigate, `"default", "on", "+@all"`, `"default", "+@all"`, 1), ""
	}, RulePlaybookComponentLiteral, RulePlaybookComponentCopy),
	gateComponentCopy("new playbook with the same generic diagnosis and another repair passes", func(t *testing.T, diagnose, mitigate, _ string) (string, string, string) {
		// The same Diagnose on the same component: an alternative repair for one
		// diagnosis, not a copy for another component.
		diagnose = strings.NewReplacer("orderscacheacl", "cartscacheacl", `"orders-cache-acl"`, `"carts-cache-acl"`).Replace(componentDiagnose)
		return diagnose, strings.Replace(mitigate, `"ACL", "SETUSER", "default", "on", "+@all"`, `"CONFIG", "SET", "maxmemory", "0"`, 1), ""
	}, RulePlaybookComponentLiteral),
	gateComponentCopy("new playbook with different evidence on another component passes", func(t *testing.T, diagnose, mitigate, _ string) (string, string, string) {
		return strings.Replace(diagnose, `"NOPERM"`, `"OOM"`, 1), strings.Replace(mitigate, `"ACL", "SETUSER", "default", "on", "+@all"`, `"CONFIG", "SET", "maxmemory", "0"`, 1), ""
	}, RulePlaybookComponentLiteral),
	gateComponentCopy("new incident detector copies another for a different component", func(t *testing.T, _, _, detector string) (string, string, string) {
		return "", "", strings.ReplaceAll(detector, "filtered", "kept")
	}, RuleDetectorComponentCopy, RuleDetectorComponentLiteral),
	gateComponentCopy("new incident detector with its own evidence passes", func(t *testing.T, _, _, detector string) (string, string, string) {
		return "", "", strings.Replace(detector, `"NOPERM"`, `"OOM"`, 1)
	}, RuleDetectorComponentLiteral),
}

// The rows join the gate table, so TestGateTable runs them and
// TestGateTableNamesEveryRule counts their rules.
func init() { gateRows = append(gateRows, componentCopyRows...) }
