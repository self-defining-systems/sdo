package validate

import (
	"strings"
	"testing"
)

// The generalized form of the cache permission repair: the program takes the
// cache workload by role and derives the container and the failing journeys
// from it; the detector binds every suspect workload instead of a name.

const genericDiagnose = `// Package cacheaclrestore restores a cache's command permissions.
package cacheaclrestore

import (
	"context"
	"strings"

	"sdo.dev/operator/sdk/localize"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "cache-acl-restore", Params: []program.ParamSpec{{Name: "cacheDeployment", Kind: "Deployment", Required: true}}}
}

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["cacheDeployment"]
	result, err := localize.FromContext(v, "health")
	if err != nil {
		return program.Verdict{}, err
	}
	if ok, why := result.Implicates(ref); !ok {
		return program.Verdict{Result: program.NoMatch, Evidence: []program.Evidence{{Resource: ref, Fact: "localization", Observed: why}}}, nil
	}
	failing := map[string]bool{}
	for _, id := range result.Failing {
		failing[id] = true
	}
	window, _ := v.TrafficWindow("health")
	for _, scenario := range window.Scenarios {
		if !failing[scenario.Scenario.ID] {
			continue
		}
		for _, sample := range scenario.Samples {
			if strings.Contains(strings.ToUpper(sample.Error), "NOPERM") {
				return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "a failing journey through the cache reports a command denial", Observed: sample.Error}}}, nil
			}
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
`

const genericMitigate = `package cacheaclrestore

import (
	"context"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	ref := b.Objects["cacheDeployment"]
	container := ""
	for _, deployment := range v.Deployments() {
		if deployment.Namespace == ref.Namespace && deployment.Name == ref.Name && len(deployment.Spec.Template.Spec.Containers) > 0 {
			container = deployment.Spec.Template.Spec.Containers[0].Name
		}
	}
	owned := map[string]bool{}
	for _, rs := range v.ReplicaSets() {
		for _, owner := range rs.OwnerReferences {
			if owner.Kind == "Deployment" && owner.Name == ref.Name && rs.Namespace == ref.Namespace {
				owned[rs.Name] = true
			}
		}
	}
	var actions []program.Action
	for _, pod := range v.Pods() {
		if pod.Namespace != ref.Namespace || len(pod.OwnerReferences) == 0 || !owned[pod.OwnerReferences[0].Name] {
			continue
		}
		target := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}
		probe := program.Command{Pod: target, Container: container, Args: []string{"redis-cli", "GET", "probe"}}
		ensure := program.Command{Pod: target, Container: container, Args: []string{"redis-cli", "ACL", "SETUSER", "default", "on", "+@all"}}
		actions = append(actions, program.EnsureExec{Observe: probe, Ensure: ensure, Expect: 0, Reason: "restore the cache permissions"})
	}
	return program.Plan{Actions: actions, Rationale: "probe the cache and restore permissions only when denied"}, nil
}
`

const genericDetector = `// Package cacheacldetector binds the cache permission repair to every suspect of a failing journey.
package cacheacldetector

import (
	"context"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/localize"
	"sdo.dev/operator/sdk/traffic"
)

type Detector struct{ localize.Inputs }

func New() sdk.Detector { return Detector{} }

func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "cache-acl", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder,
		Description: "detect cache command denials in any failing journey",
		Watches:     []sdk.WatchKind{traffic.Watch}, Interval: 2 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:   []string{"orders-cache-acl"}, OriginatingIncident: "inc-1", OriginatingCommit: "abc",
	}
}

func (d Detector) Detect(ctx context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := traffic.NewDetector(d.Spec(), "health").Detect(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	result, err := localize.FromContext(snapshot, "health")
	if err != nil {
		return nil, err
	}
	var bound []sdk.Finding
	for _, f := range findings {
		if !strings.HasPrefix(f.RuleID, traffic.RuleIDPrefix) || !strings.Contains(f.Evidence, "NOPERM") {
			continue
		}
		f.Playbooks = []string{"orders-cache-acl"}
		bound = append(bound, localize.BindSuspects(f, result, "cacheDeployment")...)
	}
	return bound, nil
}
`

// gateComponentLiteral commits the shop, with the orders playbook when
// withPlaybook, then applies edit in reflection.
func gateComponentLiteral(name string, withPlaybook bool, decision []string, edit func(t *testing.T, r string), rules ...string) gateRow {
	return gateRow{name: name, kind: gateCheck, actor: Responder, phase: ReflectionPhase, rules: rules,
		baseMessage: gateOutcomeMessage,
		prepare: func(t *testing.T, r string) {
			write(t, r, ".sdo/arch.md", componentArchitecture)
			write(t, r, ".sdo/diagnostics/traffic/workloads/health.yaml", componentWorkload)
			if withPlaybook {
				write(t, r, ".sdo/playbooks/orders-cache-acl/diagnose.go", componentDiagnose)
				write(t, r, ".sdo/playbooks/orders-cache-acl/mitigate.go", componentMitigate)
				write(t, r, ".sdo/playbooks/orders-cache-acl/README.md", componentReadme("orders-cache-acl", "inc-0"))
				generateIndex(t, r)
			}
			gateOutcome(gateClean())(t, r)
		},
		edit: edit,
		request: func(r *Request) {
			r.Decision = gateUpdated(decision...)
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: gateIncident}
		}}
}

func writePlaybook(id, diagnose, mitigate string) func(t *testing.T, r string) {
	return func(t *testing.T, r string) {
		write(t, r, ".sdo/playbooks/"+id+"/diagnose.go", gateFormatted(t, diagnose))
		write(t, r, ".sdo/playbooks/"+id+"/mitigate.go", gateFormatted(t, mitigate))
		write(t, r, ".sdo/playbooks/"+id+"/README.md", componentReadme(id, gateIncident))
		generateIndex(t, r)
	}
}

func writeDetector(id, source string) func(t *testing.T, r string) {
	return func(t *testing.T, r string) {
		write(t, r, ".sdo/diagnostics/detectors/incidents/"+id+"/detector.go", gateFormatted(t, source))
		write(t, r, ".sdo/diagnostics/detectors/incidents/"+id+"/manifest.yaml", componentManifest(id, "orders-cache-acl", gateIncident))
	}
}

var componentLiteralRows = []gateRow{
	gateComponentLiteral("new playbook names the component its role binds", false, []string{"orders-cache-acl"},
		writePlaybook("orders-cache-acl", componentDiagnose, componentMitigate), RulePlaybookComponentLiteral),
	gateComponentLiteral("new playbook derives its component from the role", false, []string{"cache-acl-restore"},
		writePlaybook("cache-acl-restore", genericDiagnose, genericMitigate)),
	gateComponentLiteral("playbook refined by naming another component", true, []string{"orders-cache-acl"}, func(t *testing.T, r string) {
		edit(t, r, ".sdo/playbooks/orders-cache-acl/diagnose.go", `scenario.Scenario.ID != "orders-read"`, `scenario.Scenario.ID != "orders-read" && scenario.Scenario.ID != "carts-read"`)
	}, RulePlaybookComponentLiteral),
	gateComponentLiteral("playbook edit keeps the component names it had", true, []string{"orders-cache-acl"}, func(t *testing.T, r string) {
		edit(t, r, ".sdo/playbooks/orders-cache-acl/diagnose.go", `"NOPERM"`, `"NOPERM "`)
	}),
	gateComponentLiteral("new incident detector binds a component by name", true, nil,
		writeDetector("cache-acl", strings.NewReplacer(`ID: "orders-cache-acl"`, `ID: "cache-acl"`, "orderscacheacldetector", "cacheacldetector", `OriginatingIncident: "inc-0"`, `OriginatingIncident: "inc-1"`).Replace(componentDetector)),
		RuleDetectorComponentLiteral),
	gateComponentLiteral("new incident detector binds every suspect", true, nil, writeDetector("cache-acl", genericDetector)),
}

func init() { gateRows = append(gateRows, componentLiteralRows...) }
