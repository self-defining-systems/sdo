package validate

import (
	"context"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/telemetry"
)

// The gate table. validate.Check is the one acceptance check: agents run it
// through `sdo-operator memory check` and the broker repeats it before it
// publishes. Each row is a candidate change (bad program, bad detector, bad
// playbook, bad edit by an actor in a phase) with the rule it must be rejected
// for, or a good change that must pass. Every rule in rules.go has a row here
// (TestGateTableNamesEveryRule), so a rule that stops firing is noticed.
//
// Rows that need the Go toolchain are gateCompileRows; they run through FakeGo
// here and through the real `go` in gate_real_integration_test.go.

const gateProgramFixture = "../../../../tests/fixtures/sdo/playbook_program_app/.sdo"

const gateProgramInvalid = "program.invalid"

const (
	gateCache          = ".sdo/diagnostics/detectors/incidents/cache/detector.go"
	gateCacheManifest  = ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml"
	gateHealth         = ".sdo/diagnostics/detectors/health/health/detector.go"
	gateHealthManifest = ".sdo/diagnostics/detectors/health/health/manifest.yaml"
	gateReadme         = ".sdo/playbooks/base/README.md"
	gateIncident       = "inc-1"
)

func gateIncidentManifest(id, firing, origin string) string {
	return "apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\nid: " + id + "\nconstructor: New\nclass: incident\nowner: responder\nwatches:\n  - apiVersion: v1\n    kind: Pod\ninterval: 30s\npersistence:\n  firing: " + firing + "\n  clearing: 1\nbatching:\n  severity: warn\n  debounce: \"0\"\npossiblePlaybooks:\n  - base\noriginatingIncident: " + origin + "\noriginatingCommit: abc\n"
}

func gateReadmeText(extra string) string {
	return "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nRepair <RESOURCE_NAME>. Run fix.sh to restart it.\n" + extra
}

type gateKind int

const (
	// gateStatic validates the tree as it is: structure and semantics only.
	gateStatic gateKind = iota
	// gateOwnership validates a change against the committed base for an actor and phase.
	gateOwnership
	// gateCheck runs the complete pipeline (validate.Check) on a change, with a
	// compile runner that accepts: the compile layer has its own rows.
	gateCheck
)

type gateRow struct {
	name string
	kind gateKind
	// rules is the exact set of blocking rules; empty means the change must pass.
	rules []string
	// path, when set, is where the last listed rule must point.
	path string
	// notes are informational rules the check must report without rejecting.
	notes []string

	actor Actor
	phase Phase
	// prepare shapes the base tree before it is committed; baseMessage is its commit message.
	prepare     func(t *testing.T, root string)
	baseMessage string
	// edit is the candidate change on top of the base.
	edit    func(t *testing.T, root string)
	request func(*Request)
}

func gateOverlay(name string) func(*testing.T, string) {
	return func(t *testing.T, root string) { applyCase(t, root, name) }
}

// gateDeploymentPlan is a structurally valid plan.yaml.
const gateDeploymentPlan = `{"source":{"rendererId":"raw","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig","paths":["k8s"]}}}` + "\n"

func gateWrite(rel, content string) func(*testing.T, string) {
	return func(t *testing.T, root string) { write(t, root, rel, content) }
}

// gateOutcome commits a controller outcome for gateIncident with the trailers
// the broker puts on an accepted outcome commit.
func gateOutcome(outcome *contracts.OutcomeRecord) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		outcome.SchemaVersion, outcome.IncidentId = 1, gateIncident
		for _, action := range outcome.RepairActions {
			action.Target, action.Summary, action.Details = "Service/app/api", "synthetic repair", "synthetic receipt"
			if action.StartedAt == nil {
				action.StartedAt = timestamppb.New(time.Unix(101, 0))
				action.CompletedAt = action.StartedAt
			}
		}
		raw, err := protojson.Marshal(outcome)
		if err != nil {
			t.Fatal(err)
		}
		write(t, root, layout.OutcomePath(gateIncident), string(raw)+"\n")
	}
}

const gateOutcomeMessage = "outcome\n\nSDO-Incident: " + gateIncident + "\nSDO-Phase: outcome"

func gateRepairOutcome() *contracts.OutcomeRecord {
	at := timestamppb.New(time.Unix(100, 0))
	ref := &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	app := &contracts.PlanApplication{ApplicationId: "apply-1", ProgramIds: []string{"base"}, Applied: true, VerificationCompleted: true, VerificationPassed: true, CompletedAt: at, Plan: &contracts.Plan{}}
	return &contracts.OutcomeRecord{Classification: "success",
		AppliedPlaybooks: []string{"base"},
		RepairActions:    []*contracts.RepairActionReceipt{{ActionId: "sdo-program-apply-1", Kind: "program-plan", Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: at, CompletedAt: at}},
		PlanApplications: []*contracts.PlanApplication{app},
		PriorStages:      []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{PlaybookId: "base", Result: "applied_cleared", ApplyReceipt: app}}}},
	}
}

func gateHandRepair(o *contracts.OutcomeRecord) *contracts.OutcomeRecord {
	o.RepairActions = append(o.RepairActions, &contracts.RepairActionReceipt{ActionId: "manual-port", Kind: "exec", Success: true})
	return o
}

func gateStaleOutcome() *contracts.OutcomeRecord {
	o := gateRepairOutcome()
	o.PriorStages[0].Mitigations[0].Result = "apply_error"
	return o
}

// gateReflection is a responder reflection over a committed accepted outcome.
func gateReflection(name string, outcome *contracts.OutcomeRecord, decision *contracts.ReflectionDecision, edit func(*testing.T, string), rules ...string) gateRow {
	return gateRow{name: name, kind: gateCheck, actor: Responder, phase: ReflectionPhase, rules: rules, edit: edit,
		prepare: gateOutcome(outcome), baseMessage: gateOutcomeMessage,
		request: func(r *Request) {
			r.Decision = decision
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: gateIncident}
		}}
}

// gateWithRepairVerifier gives the check the controller's closure fact: whether
// the committed outcome holds an independently verified repaired cause.
func gateWithRepairVerifier(name string, verified bool, row gateRow) gateRow {
	row.name = name
	inner := row.request
	row.request = func(r *Request) {
		inner(r)
		r.RepairVerified = func(*contracts.OutcomeRecord) bool { return verified }
	}
	return row
}

// gateUnscopedBinding rewrites the cache incident detector to bind a role by
// name, with the given manifest and Spec playbook declarations.
func gateUnscopedBinding(manifestPlaybooks, specPlaybooks string) func(*testing.T, string) {
	return func(t *testing.T, r string) {
		edit(t, r, gateCacheManifest, "possiblePlaybooks:\n  - base", manifestPlaybooks)
		edit(t, r, gateCache, `Playbooks:   []string{"base"},`, specPlaybooks)
		edit(t, r, gateCache, "func (Detector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) { return nil, nil }",
			"func (Detector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {\n\treturn []sdk.Finding{{ParameterBindings: map[string]sdk.ObjectRef{\"deployment\": {Kind: \"Deployment\", Name: \"api\"}}}}, nil\n}")
		write(t, r, gateCache, gateFormatted(t, gateRead(t, r, gateCache)))
	}
}

// gateDuplicateRepair adds a second copy of an accepted program, reshaped by
// change, as a new playbook in reflection.
func gateDuplicateRepair(name string, change func(diagnose, mitigate string) (string, string), rules ...string) gateRow {
	const source = ".sdo/playbooks/service-targetport-mismatch/"
	row := gateRow{name: name, kind: gateCheck, actor: Responder, phase: ReflectionPhase, rules: rules,
		baseMessage: gateOutcomeMessage,
		prepare: func(t *testing.T, r string) {
			for _, dir := range []string{"playbooks/service-targetport-mismatch", "lib"} {
				copyTree(t, filepath.Join(gateProgramFixture, dir), filepath.Join(r, ".sdo", dir))
			}
			write(t, r, ".sdo/diagnostics/go.mod", gateRead(t, gateProgramFixture, "diagnostics/go.mod"))
			generateIndex(t, r)
			gateOutcome(gateClean())(t, r)
		},
		edit: func(t *testing.T, r string) {
			rename := func(text string) string {
				text = strings.ReplaceAll(text, "package targetport", "package apiport")
				text = strings.ReplaceAll(text, `ID: "service-targetport-mismatch"`, `ID: "api-port-repair"`)
				return strings.ReplaceAll(text, "Program checks whether", "Program decides whether")
			}
			diagnose, mitigate := change(rename(gateRead(t, r, source+"diagnose.go")), rename(gateRead(t, r, source+"mitigate.go")))
			write(t, r, ".sdo/playbooks/api-port-repair/diagnose.go", diagnose)
			write(t, r, ".sdo/playbooks/api-port-repair/mitigate.go", mitigate)
			write(t, r, ".sdo/playbooks/api-port-repair/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: api-port-repair\noriginating_incident: "+gateIncident+"\n---\n\nRestore the api Service port.\n")
			generateIndex(t, r)
		},
		request: func(r *Request) {
			r.Decision = gateUpdated("api-port-repair")
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: gateIncident}
		}}
	if len(rules) > 0 {
		row.path = ".sdo/playbooks/api-port-repair"
	}
	return row
}

// gateStrippedRead is a Diagnose that looks for a restart through from: a
// source diff field (stripped bookkeeping, never observable) or the live object.
func gateStrippedRead(from string) string {
	return `package base

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["deployment"]
	diff, err := v.DiffAgainstSource(ref)
	if err != nil {
		return program.Verdict{}, err
	}
	annotations, err := diff.Field("spec.template.metadata.annotations")
	if err != nil {
		return program.Verdict{}, err
	}
	for _, live := range v.Deployments() {
		if restarted(` + from + `, "kubectl.kubernetes.io/restartedAt") {
			return program.Verdict{Result: program.NoMatch}, nil
		}
		_ = live
	}
	_ = annotations
	return program.Verdict{Result: program.Match}, nil
}

func restarted(value any, key string) bool { m, _ := value.(map[string]string); return m[key] != "" }
`
}

// gateLiveReadReusingNames compares against source, then reads the restart
// annotation from the live object under a shadowed name, and mentions the key
// in an error message built from the comparison's error. Nothing reads the
// annotation from the comparison.
const gateLiveReadReusingNames = `package base

import (
	"context"
	"fmt"

	"sdo.dev/operator/sdk/program"
)

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["deployment"]
	annotations, err := v.DiffAgainstSource(ref)
	if err != nil {
		return program.Verdict{}, fmt.Errorf("compare before reading kubectl.kubernetes.io/restartedAt: %w", err)
	}
	_ = annotations
	for _, live := range v.Deployments() {
		annotations := live.Spec.Template.Annotations
		if annotations["kubectl.kubernetes.io/restartedAt"] != "" {
			return program.Verdict{Result: program.NoMatch}, nil
		}
	}
	return program.Verdict{Result: program.Match}, nil
}
`

var gateNoChange = &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already covered"}

func gateUpdated(ids ...string) *contracts.ReflectionDecision {
	return &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: ids}
}

func gateClean() *contracts.OutcomeRecord { return &contracts.OutcomeRecord{Classification: "success"} }

func gateShell(r *Request) { r.CodePath = "shell" }

func gatePlaybookEdit(t *testing.T, root string) {
	write(t, root, gateReadme, gateReadmeText("New guidance.\n"))
}

func gateTrafficEdit(name, content string) func(*testing.T, string) {
	return gateWrite(".sdo/diagnostics/traffic/workloads/"+name+".yaml", content)
}

// gateEscalatedOutcome is a closure the responder escalated: health never
// returned and it confirmed an out-of-reach cause; with stage set, the
// automatic stage escalated it on an accepted playbook first.
func gateEscalatedOutcome(stage bool) *contracts.OutcomeRecord {
	store := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "store"}
	finding := &contracts.Finding{DetectorId: "traffic-links", RuleId: "link-reachability.web.store.5432", Status: "active", Severity: "warn", Summary: "link to store delayed", Evidence: "p50 2s", Fingerprint: "link", PrimaryResource: store}
	cause := &contracts.ConfirmedRootCause{
		Summary: "delay on the network path to store", Resources: []*contracts.ObjectRef{store}, CauseClass: "network-delay", Reach: "out-of-reach",
		Evidence: []*contracts.RootCauseEvidence{{Kind: "detector-finding", Source: "traffic-links", Observation: "p50 2s against 5ms healthy"}},
	}
	// Escalation learning needs a confirmed out-of-reach cause backed by an opening finding.
	o := &contracts.OutcomeRecord{Classification: "escalated", Findings: []*contracts.Finding{finding}, ConfirmedRootCauses: []*contracts.ConfirmedRootCause{cause}}
	if stage {
		o.PriorStages = []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{PlaybookId: "base", Result: "escalated"}}}}
	}
	return o
}

// gateDiagnoseOnlyPlaybook adds a new playbook over an escalated closure: the
// accepted targetport Diagnose under a new ID, with mitigate.go only when
// withMitigate is set. The closure holds no verified repair.
func gateDiagnoseOnlyPlaybook(name string, withMitigate bool, rules ...string) gateRow {
	row := gateDuplicateRepair(name, func(diagnose, mitigate string) (string, string) {
		return diagnose, strings.ReplaceAll(mitigate, "spec.ports[%d].targetPort", "spec.ports[%d].port")
	}, rules...)
	row.path = ""
	prepare, edit := row.prepare, row.edit
	row.prepare = func(t *testing.T, r string) {
		prepare(t, r)
		gateOutcome(gateEscalatedOutcome(false))(t, r)
	}
	row.edit = func(t *testing.T, r string) {
		edit(t, r)
		if !withMitigate {
			if err := os.Remove(filepath.Join(r, ".sdo/playbooks/api-port-repair/mitigate.go")); err != nil {
				t.Fatal(err)
			}
			generateIndex(t, r)
		}
	}
	return gateWithRepairVerifier(name, false, row)
}

var gateRows = []gateRow{
	// Good trees: each must pass, including the optional classes.
	{name: "good tree", kind: gateStatic},
	{name: "memory without detectors is valid (detector.none is not enforced)", kind: gateStatic, edit: gateOverlay("detector-none")},
	{name: "memory without playbooks is valid (playbook.none is not enforced)", kind: gateStatic, edit: gateOverlay("playbook-none")},
	{name: "unchanged good tree passes the whole pipeline", kind: gateCheck, actor: Responder, phase: ReflectionPhase,
		prepare: gateOutcome(gateClean()), baseMessage: gateOutcomeMessage, request: func(r *Request) {
			r.Decision = gateNoChange
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: gateIncident}
		}},

	// The shell code path (CodePath "shell"): the same Check, with markdown
	// notes and shell scripts as the only authored files.
	{name: "shell: a playbook note passes", kind: gateCheck, actor: Responder, phase: ReflectionPhase,
		request: gateShell, edit: gateWrite(".sdo/playbooks/base/notes.md", "# Notes\n\nRestart the pod.\n")},
	{name: "shell: a shell author edits a detector manifest", kind: gateCheck, actor: Responder, phase: ReflectionPhase,
		rules: []string{"shell.format"}, path: gateCacheManifest, request: gateShell,
		edit: func(t *testing.T, root string) {
			write(t, root, gateCacheManifest, gateRead(t, root, gateCacheManifest)+"# edited\n")
		}},
	{name: "shell: a judge writes a playbook note it does not own", kind: gateCheck, actor: HealthJudge, phase: InitialPhase,
		rules: []string{RuleOwnershipDenied}, path: ".sdo/playbooks/base/notes.md", request: gateShell,
		edit: gateWrite(".sdo/playbooks/base/notes.md", "# Notes\n")},

	// Structure and semantics, one bad tree per rule.
	{name: "unknown path", kind: gateStatic, rules: []string{RuleUnknownPath}, path: ".sdo/notes.txt", edit: gateOverlay("unknown-path")},
	{name: "invalid id", kind: gateStatic, rules: []string{RuleInvalidID}, path: ".sdo/playbooks/Bad_ID/README.md", edit: gateOverlay("invalid-id")},
	{name: "secret path", kind: gateStatic, rules: []string{RuleSecretPath}, path: ".sdo/diagnostics/traffic/generators/secret-token.go", edit: gateOverlay("secret-path")},
	{name: "legacy manifest", kind: gateStatic, rules: []string{RuleLegacyManifest}, path: ".sdo/diagnostics/manifest.yaml", edit: gateOverlay("legacy-manifest")},
	{name: "schema version", kind: gateStatic, rules: []string{RuleSchemaVersion}, path: ".sdo/schema-version", edit: gateOverlay("schema-version")},
	{name: "yaml parse", kind: gateStatic, rules: []string{RuleYAMLParse}, path: ".sdo/diagnostics/traffic/workloads/health.yaml", edit: gateOverlay("yaml-parse")},
	{name: "go syntax", kind: gateStatic, rules: []string{RuleGoSyntax}, path: gateCache, edit: gateOverlay("go-syntax")},
	{name: "document parse", kind: gateStatic, rules: []string{RuleDocumentParse}, path: ".sdo/arch.md", edit: gateOverlay("document-parse")},
	{name: "manifest parse", kind: gateStatic, rules: []string{RuleManifestParse}, path: gateCacheManifest, edit: gateOverlay("manifest-parse")},
	{name: "manifest invalid", kind: gateStatic, rules: []string{RuleManifestInvalid}, path: gateCacheManifest, edit: gateOverlay("manifest-invalid")},
	{name: "manifest names a playbook by path", kind: gateStatic, rules: []string{RuleManifestInvalid}, path: gateCacheManifest, edit: gateOverlay("manifest-playbook-path")},
	{name: "manifest location", kind: gateStatic, rules: []string{RuleManifestLocation}, path: gateHealthManifest, edit: gateOverlay("manifest-location")},
	{name: "architecture missing", kind: gateStatic, rules: []string{RuleArchitectureMissing}, path: ".sdo/arch.md", edit: gateOverlay("architecture-missing")},
	{name: "sources invalid", kind: gateStatic, rules: []string{RuleSourcesInvalid}, path: layout.SourcesFile, edit: gateOverlay("sources-invalid")},
	{name: "goal owner", kind: gateStatic, rules: []string{RuleGoalOwner}, path: ".sdo/goal.md", edit: gateOverlay("goal-owner")},
	{name: "manifest missing", kind: gateStatic, rules: []string{RuleManifestMissing}, path: ".sdo/diagnostics/detectors/incidents/orphan", edit: gateOverlay("manifest-missing")},
	{name: "detector without Go source", kind: gateStatic, rules: []string{RuleDetectorNoSource}, path: ".sdo/diagnostics/detectors/incidents/cache", edit: gateOverlay("detector-no-source")},
	{name: "duplicate id", kind: gateStatic, rules: []string{RuleDuplicateID}, path: ".sdo/diagnostics/detectors/incidents/health/manifest.yaml", edit: gateOverlay("duplicate-id")},
	{name: "detector names an unknown playbook", kind: gateStatic, rules: []string{RuleUnknownPlaybook}, path: gateCacheManifest, edit: gateOverlay("unknown-playbook")},
	{name: "playbook without README", kind: gateStatic, rules: []string{RulePlaybookNoReadme}, path: ".sdo/playbooks/extra", edit: gateOverlay("playbook-missing-readme")},
	{name: "playbook owner", kind: gateStatic, rules: []string{RulePlaybookOwner}, path: gateReadme, edit: gateOverlay("playbook-owner")},
	{name: "playbook originating incident", kind: gateStatic, rules: []string{RulePlaybookIncident}, path: gateReadme, edit: gateOverlay("playbook-originating-incident")},
	{name: "playbook status", kind: gateStatic, rules: []string{gateProgramInvalid, RulePlaybookStatus}, path: gateReadme,
		edit: gateWrite(gateReadme, "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\nstatus: paused\n---\nRepair <RESOURCE_NAME>. Run fix.sh to restart it.\n")},
	{name: "playbook index missing", kind: gateStatic, rules: []string{RuleIndexMissing}, path: ".sdo/playbooks/README.md", edit: gateOverlay("index-missing")},
	{name: "playbook index hand written", kind: gateStatic, rules: []string{RuleIndexGenerated}, path: ".sdo/playbooks/README.md", edit: gateOverlay("index-generated")},
	{name: "script unreferenced", kind: gateStatic, rules: []string{RuleScriptUnreferenced}, path: ".sdo/playbooks/base/scripts/other.sh", edit: gateOverlay("script-unreferenced")},
	{name: "script syntax", kind: gateStatic, rules: []string{RuleScriptSyntax}, path: ".sdo/playbooks/base/scripts/fix.sh", edit: gateOverlay("script-syntax")},
	{name: "script not executable", kind: gateStatic, rules: []string{RuleScriptNotExecutable}, edit: func(t *testing.T, root string) {
		if err := os.Chmod(filepath.Join(root, ".sdo/playbooks/base/scripts/fix.sh"), 0o644); err != nil {
			t.Fatal(err)
		}
	}},
	{name: "physical path in a playbook", kind: gateStatic, rules: []string{RulePhysicalPath}, path: gateReadme, edit: gateOverlay("physical-path")},
	{name: "physical path in a detector", kind: gateStatic, rules: []string{RulePhysicalPath}, path: gateCache, edit: gateOverlay("detector-physical-path")},
	{name: "symlink", kind: gateStatic, rules: []string{RuleSymlink}, edit: func(t *testing.T, root string) {
		if err := os.Symlink("/etc/passwd", filepath.Join(root, ".sdo", "link.md")); err != nil {
			t.Fatal(err)
		}
	}},
	{name: "not a regular file", kind: gateStatic, rules: []string{RuleNotRegular}, edit: func(t *testing.T, root string) {
		if err := syscall.Mkfifo(filepath.Join(root, ".sdo", "goal.pipe"), 0o644); err != nil {
			t.Fatal(err)
		}
	}},
	{name: "file too large", kind: gateStatic, rules: []string{RuleTooLarge}, edit: gateWrite(gateReadme, gateReadmeText(strings.Repeat("x", 300*1024)))},

	{name: "program imports a forbidden package", kind: gateStatic, rules: []string{gateProgramInvalid}, path: ".sdo/playbooks",
		edit: gateWrite(".sdo/playbooks/base/diagnose.go", "package base\n\nimport \"os/exec\"\n\ntype Diagnose struct{}\n\nfunc (Diagnose) Spec()     {}\nfunc (Diagnose) Diagnose() { _ = exec.Command }\n")},
	{name: "Go source is not gofmt formatted", kind: gateStatic, rules: []string{gateProgramInvalid, "go.format"}, path: ".sdo/playbooks/base/diagnose.go",
		edit: gateWrite(".sdo/playbooks/base/diagnose.go", "package base\ntype Diagnose struct{}\nfunc(Diagnose)Spec(){}\nfunc(Diagnose)Diagnose(){}\n")},
	{name: "observation scope is malformed", kind: gateStatic, rules: []string{"observation.scope"}, path: ".sdo/arch.md",
		edit: gateWrite(".sdo/arch.md", "---\nschema_version: 1\ngenerated_at_commit: base\napplication: demo\nobservation_scope: {version: 2}\n---\nx\n")},
	{name: "detector watches outside the observation scope", kind: gateStatic, rules: []string{"observation.watch"}, path: ".sdo/arch.md",
		request: func(r *Request) { r.ApplicationNamespace = "demo" },
		edit: func(t *testing.T, r string) {
			write(t, r, ".sdo/arch.md", "---\nschema_version: 1\ngenerated_at_commit: base\napplication: demo\nobservation_scope:\n  version: 1\n---\nx\n")
			edit(t, r, gateCacheManifest, "    kind: Pod", "    kind: Pod\n    namespace: other")
		}},
	{name: "program that decides on diff text is noted, not rejected", kind: gateStatic, rules: []string{gateProgramInvalid}, notes: []string{RuleProgramDiffText},
		edit: func(t *testing.T, r string) {
			remove(t, r, ".sdo/playbooks/base/scripts")
			write(t, r, ".sdo/playbooks/base/diagnose.go", gateFormatted(t, gateDiffTextProgram))
		}},
	{name: "corpus case claims a script origin with no receipt", kind: gateCheck, actor: Responder, phase: ReflectionPhase, rules: []string{RuleProgramMitigationRepair},
		baseMessage: gateOutcomeMessage,
		prepare: func(t *testing.T, r string) {
			for _, dir := range []string{"playbooks/service-targetport-mismatch", "lib", "corpus"} {
				copyTree(t, filepath.Join(gateProgramFixture, dir), filepath.Join(r, ".sdo", dir))
			}
			write(t, r, ".sdo/diagnostics/go.mod", gateRead(t, gateProgramFixture, "diagnostics/go.mod"))
			generateIndex(t, r)
			gateOutcome(gateClean())(t, r)
		},
		edit: func(t *testing.T, r string) {
			copyTree(t, filepath.Join(r, ".sdo/corpus/targetport-mismatch"), filepath.Join(r, ".sdo/corpus/case-2"))
			labels := gateRead(t, r, ".sdo/corpus/case-2/labels.json")
			write(t, r, ".sdo/corpus/case-2/labels.json", strings.Replace(labels, `{"root_cause`, `{"script_origins":{"sha256:abc":"no-such-receipt"},"root_cause`, 1))
		},
		request: func(r *Request) {
			r.Decision = gateUpdated()
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: gateIncident}
		}},

	// Ownership: who may change what, in which phase.
	{name: "responder edits an incident detector in reflection", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, edit: gateWrite(gateCache, "package cache\n\n// tuned\n")},
	{name: "responder adds an incident detector in reflection", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, edit: func(t *testing.T, r string) {
		write(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", gateIncidentManifest("dns", "1", "inc-2"))
		write(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", "package dns\n")
	}},
	gateAppFileDenied(Responder, ProposalPhase), gateAppFileDenied(Responder, ReflectionPhase), gateAppFileDenied(Responder, InitialPhase),
	gateAppFileDenied(Deployer, InitialPhase), gateAppFileDenied(Deployer, AdvancePhase), gateAppFileDenied(Deployer, ProposalPhase),
	gateAppFileDenied(Upkeep, InitialPhase), gateAppFileDenied(Human, InitialPhase),
	gateAppFileDenied(Human, ImportPhase), gateAppFileDenied(HealthJudge, ProposalPhase),
	{name: "responder edits source in a proposal", kind: gateOwnership, actor: Responder, phase: ProposalPhase, rules: []string{RuleOwnershipDenied}, edit: gateWrite("app.txt", "repair\n")},
	{name: "health judge edits health detectors in initial", kind: gateOwnership, actor: HealthJudge, phase: InitialPhase, edit: func(t *testing.T, r string) {
		write(t, r, gateHealth, "package objective\n\n// edit\n")
		edit(t, r, gateHealthManifest, "interval: 30s", "interval: 20s")
	}},
	{name: "human edits the goal in initial", kind: gateOwnership, actor: Human, phase: InitialPhase, edit: gateWrite(layout.GoalFile, "---\nschema_version: 1\nowner: human\n---\nNew goal\n")},
	{name: "human publishes followed sources in initial", kind: gateOwnership, actor: Human, phase: InitialPhase, edit: gateWrite(layout.SourcesFile, "sources:\n- path: .\n  kind: git\n  config: {repository: \"git://synthetic.invalid/app.git\", ref: main}\n")},
	{name: "responder cannot edit followed sources", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleOwnershipDenied}, path: layout.SourcesFile,
		edit: gateWrite(layout.SourcesFile, "sources:\n- path: .\n  kind: git\n  config: {repository: \"git://synthetic.invalid/app.git\", ref: main}\n")},
	{name: "deployer edits source in initial", kind: gateOwnership, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, edit: gateWrite("app.txt", "deployed\n")},
	{name: "deployer writes the deployment plan, files and patches in initial", kind: gateOwnership, actor: Deployer, phase: InitialPhase, edit: func(t *testing.T, r string) {
		write(t, r, layout.DeploymentPlanFile, "renderer: raw\n")
		write(t, r, ".sdo/deployment/values/prod.yaml", "replicas: 2\n")
		write(t, r, ".sdo/deployment/manifests/app/deploy.yaml", "kind: Deployment\n")
		write(t, r, ".sdo/deployment/build/Dockerfile", "FROM scratch\n")
		write(t, r, ".sdo/deployment/patches/0001-fix.patch", "SDO-Patch: 1\nSDO-Owner: deployer\n")
	}},
	{name: "human seeds every deployment kind in initial", kind: gateOwnership, actor: Human, phase: InitialPhase, edit: func(t *testing.T, r string) {
		write(t, r, layout.DeploymentPlanFile, "renderer: raw\n")
		write(t, r, ".sdo/deployment/overlays/telemetry/o.yaml", "a: b\n")
		write(t, r, ".sdo/deployment/patches/0001-fix.patch", "x\n")
	}},
	{name: "health judge writes the telemetry overlay slot", kind: gateOwnership, actor: HealthJudge, phase: InitialPhase, edit: func(t *testing.T, r string) {
		write(t, r, gateHealth, "package objective\n\n// edit\n")
		edit(t, r, gateHealthManifest, "interval: 30s", "interval: 20s")
		write(t, r, ".sdo/deployment/overlays/telemetry/o.yaml", "a: b\n")
	}},
	{name: "deployer cannot write the telemetry overlay", kind: gateOwnership, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: ".sdo/deployment/overlays/telemetry/o.yaml",
		edit: gateWrite(".sdo/deployment/overlays/telemetry/o.yaml", "a: b\n")},
	{name: "health judge cannot write the deployment plan", kind: gateOwnership, actor: HealthJudge, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, "renderer: raw\n")},
	{name: "responder adds a deployment patch in a proposal", kind: gateOwnership, actor: Responder, phase: ProposalPhase,
		prepare: gateWrite(".sdo/deployment/patches/0001-old.patch", "SDO-Owner: deployer\n"),
		edit:    gateWrite(".sdo/deployment/patches/0002-repair.patch", "SDO-Owner: responder\n")},
	{name: "responder cannot modify an accepted deployment patch", kind: gateOwnership, actor: Responder, phase: ProposalPhase, rules: []string{RuleOwnershipDenied}, path: ".sdo/deployment/patches/0001-old.patch",
		prepare: gateWrite(".sdo/deployment/patches/0001-old.patch", "SDO-Owner: responder\n"),
		edit:    gateWrite(".sdo/deployment/patches/0001-old.patch", "SDO-Owner: responder\nchanged\n")},
	{name: "responder cannot delete an accepted deployment patch", kind: gateOwnership, actor: Responder, phase: ProposalPhase, rules: []string{RuleOwnershipDenied}, path: ".sdo/deployment/patches/0001-old.patch",
		prepare: gateWrite(".sdo/deployment/patches/0001-old.patch", "SDO-Owner: responder\n"),
		edit:    func(t *testing.T, r string) { remove(t, r, ".sdo/deployment/patches/0001-old.patch") }},
	{name: "responder cannot write deployment values", kind: gateOwnership, actor: Responder, phase: ProposalPhase, rules: []string{RuleOwnershipDenied}, path: ".sdo/deployment/values/prod.yaml",
		edit: gateWrite(".sdo/deployment/values/prod.yaml", "replicas: 9\n")},
	{name: "responder cannot write deployment patches in reflection", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleOwnershipDenied}, path: ".sdo/deployment/patches/0001-x.patch",
		edit: gateWrite(".sdo/deployment/patches/0001-x.patch", "x\n")},
	{name: "deployment path outside the layout is denied to every actor", kind: gateOwnership, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: ".sdo/deployment/notes.md",
		edit: gateWrite(".sdo/deployment/notes.md", "x\n")},
	{name: "upstream advance cannot change deployment files", kind: gateOwnership, actor: Upstream, phase: AdvancePhase, rules: []string{RuleOwnershipDenied}, path: ".sdo/deployment/values/prod.yaml",
		edit: gateWrite(".sdo/deployment/values/prod.yaml", "x\n")},
	{name: "upstream advances source", kind: gateOwnership, actor: Upstream, phase: AdvancePhase, edit: gateWrite("app.txt", "upstream\n")},
	{name: "upstream advance cannot change memory", kind: gateOwnership, actor: Upstream, phase: AdvancePhase, rules: []string{RuleOwnershipDenied}, path: layout.GoalFile,
		edit: gateWrite(layout.GoalFile, "---\nschema_version: 1\nowner: human\n---\nNew goal\n")},
	{name: "upkeep stamps the schema alone", kind: gateOwnership, actor: Upkeep, phase: InitialPhase, edit: func(t *testing.T, r string) {
		write(t, r, layout.SchemaVersionFile, "8\n")
		write(t, r, layout.DiagnosticsGoModFile, "module m\n")
	}},
	{name: "upkeep migration drops a retired Go-installed generic detector", kind: gateOwnership, actor: Upkeep, phase: InitialPhase,
		prepare: func(t *testing.T, r string) {
			write(t, r, layout.SchemaVersionFile, "7\n")
			gateStrandedDetector(t, r, "synthetic-retired", "lifecycle-bootstrap")
		},
		edit: func(t *testing.T, r string) {
			write(t, r, layout.SchemaVersionFile, "8\n")
			remove(t, r, layout.DetectorDir(layout.ClassHealth, "synthetic-retired"))
		}},
	{name: "upkeep migration cannot drop an authored detector", kind: gateOwnership, actor: Upkeep, phase: InitialPhase, rules: []string{RuleUpkeepMigration},
		prepare: func(t *testing.T, r string) {
			write(t, r, layout.SchemaVersionFile, "7\n")
			gateStrandedDetector(t, r, "synthetic-authored", "initial")
		},
		edit: func(t *testing.T, r string) {
			write(t, r, layout.SchemaVersionFile, "8\n")
			remove(t, r, layout.DetectorDir(layout.ClassHealth, "synthetic-authored"))
		}},
	{name: "responder cannot edit the goal", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleOwnershipDenied}, path: layout.GoalFile,
		edit: gateWrite(layout.GoalFile, "---\nschema_version: 1\nowner: human\n---\nNew goal\n")},
	{name: "responder cannot edit outcomes", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleOwnershipDenied}, edit: gateWrite(layout.OutcomePath("inc"), "{}\n")},
	{name: "responder cannot edit source in reflection", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleOwnershipDenied}, path: "app.txt", edit: gateWrite("app.txt", "changed\n")},
	{name: "responder cannot edit memory in a proposal", kind: gateOwnership, actor: Responder, phase: ProposalPhase, rules: []string{RuleOwnershipDenied}, edit: gateWrite(gateCache, "package cache\n\n// early\n")},
	{name: "responder cannot edit health detector source", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleOwnershipDenied}, path: gateHealth, edit: gateWrite(gateHealth, "package objective\n\n// edit\n")},
	{name: "responder cannot edit an unclassified memory path", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleOwnershipDenied}, edit: gateWrite(".sdo/notes.txt", "x\n")},
	{name: "responder cannot change a health manifest", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleOwnershipDenied, RuleProtectedDetector},
		edit: func(t *testing.T, r string) { edit(t, r, gateHealthManifest, "interval: 30s", "interval: 20s") }},
	{name: "responder incident detector must fire on first match", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleResponderFiring}, edit: func(t *testing.T, r string) {
		write(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", gateIncidentManifest("dns", "2", "inc-2"))
		write(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", "package dns\n")
	}},
	{name: "responder cannot rewrite detector provenance", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, rules: []string{RuleProvenanceRewrite},
		edit: func(t *testing.T, r string) {
			edit(t, r, gateCacheManifest, "originatingIncident: inc-1", "originatingIncident: inc-9")
		}},
	{name: "health judge cannot add an incident detector", kind: gateOwnership, actor: HealthJudge, phase: InitialPhase, rules: []string{RuleOwnershipDenied, RuleProtectedDetector}, edit: func(t *testing.T, r string) {
		write(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", gateIncidentManifest("dns", "1", "inc-2"))
		write(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", "package dns\n")
	}},
	{name: "health judge initial map probe without a Go-installable traffic detector", kind: gateOwnership, actor: HealthJudge, phase: InitialPhase, rules: []string{RuleInitialNeedsDetectors},
		prepare: func(t *testing.T, r string) { write(t, r, layout.ArchitectureFile, mapInventory) },
		edit: func(t *testing.T, r string) {
			write(t, r, layout.ArchitectureFile, mapInventory+"\n"+mapRegion(mapYAML))
			dir := ".sdo/diagnostics/traffic/workloads/"
			for _, w := range [][2]string{{"probe-call", "call-all"}, {"probe-write", "write-item"}, {"probe-read-later", "items-later"}} {
				write(t, r, dir+w[0]+".yaml", strings.Replace(mapWorkload(w[0], w[1]), "purpose: health-probe", "purpose: incident-probe", 1))
			}
		}},
	{name: "health judge may add only health detectors to empty memory", kind: gateOwnership, actor: HealthJudge, phase: InitialPhase, rules: []string{RuleOwnershipDenied, RuleInitialHealthOnly},
		prepare: func(t *testing.T, r string) { remove(t, r, ".sdo/diagnostics") },
		edit: func(t *testing.T, r string) {
			copyTree(t, filepath.Join("testdata", "good", ".sdo", "diagnostics"), filepath.Join(r, ".sdo", "diagnostics"))
		}},
	{name: "upkeep cannot change detectors without a schema migration", kind: gateOwnership, actor: Upkeep, phase: InitialPhase, rules: []string{RuleUpkeepMigration}, edit: gateWrite(gateCache, "package cache\n\n// x\n")},
	{name: "unknown actor", kind: gateOwnership, actor: Actor("intern"), phase: InitialPhase, rules: []string{RuleUnknownActor}, edit: gateWrite("app.txt", "x\n")},
	{name: "unknown phase", kind: gateOwnership, actor: Responder, phase: Phase("whenever"), rules: []string{RuleUnknownPhase}, edit: gateWrite("app.txt", "x\n")},
	{name: "unsafe path", kind: gateOwnership, actor: Responder, phase: ProposalPhase, rules: []string{RuleUnsafePath}, edit: gateWrite(`we\ird.txt`, "x\n")},

	// Publication content and lifecycle: the full pipeline.
	{name: "empty proposal", kind: gateCheck, actor: Responder, phase: ProposalPhase, rules: []string{RuleEmptyDiff}},
	gateReflection("whitespace in authored memory", gateClean(), gateUpdated("base"), gateWrite(gateReadme, gateReadmeText("Recovery guidance. \n")), RuleWhitespace),
	{name: "whitespace in application bytes is not judged", kind: gateCheck, actor: Upstream, phase: AdvancePhase,
		request: func(r *Request) {
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: "advance-0123456789ab", VerifiedSourceTree: strings.Repeat("a", 40)}
		},
		edit: gateWrite("kubernetes/geo/script.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: s\ndata:\n  init.sh: |\n    echo   \n    \n")},
	{name: "reflection without an accepted outcome", kind: gateCheck, actor: Responder, phase: ReflectionPhase, rules: []string{RuleAcceptedOutcome}, edit: gatePlaybookEdit,
		request: func(r *Request) { r.Decision = gateUpdated("base") }},
	{name: "initial phase closed after detectors are accepted", kind: gateCheck, actor: Responder, phase: InitialPhase, rules: []string{RuleBootstrapClosed, RuleOwnershipDenied}, edit: gatePlaybookEdit},
	{name: "initial deployment inputs need a verified source tree", kind: gateCheck, actor: Deployer, phase: InitialPhase, rules: []string{RuleVerifiedSource},
		prepare: func(t *testing.T, r string) { remove(t, r, ".sdo/diagnostics"); remove(t, r, ".sdo/playbooks") },
		edit:    gateWrite(".sdo/deployment/manifests/app.yaml", "kind: Deployment\n")},
	{name: "the deployer's plan alone needs a verified source tree", kind: gateCheck, actor: Deployer, phase: InitialPhase, rules: []string{RuleVerifiedSource},
		prepare: func(t *testing.T, r string) { remove(t, r, ".sdo/diagnostics"); remove(t, r, ".sdo/playbooks") },
		edit:    gateWrite(layout.DeploymentPlanFile, gateDeploymentPlan)},
	{name: "a verified deployment may publish only its plan", kind: gateCheck, actor: Deployer, phase: InitialPhase,
		prepare: func(t *testing.T, r string) { remove(t, r, ".sdo/diagnostics"); remove(t, r, ".sdo/playbooks") },
		edit:    gateWrite(layout.DeploymentPlanFile, gateDeploymentPlan),
		request: func(r *Request) {
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: "source", VerifiedSourceTree: strings.Repeat("a", 40)}
		}},
	{name: "verified deployment publish passes", kind: gateCheck, actor: Deployer, phase: InitialPhase,
		prepare: func(t *testing.T, r string) { remove(t, r, ".sdo/diagnostics"); remove(t, r, ".sdo/playbooks") },
		edit:    gateWrite(".sdo/deployment/manifests/app.yaml", "kind: Deployment\n"),
		request: func(r *Request) {
			r.Publication = &PublicationContext{BaseCommit: "HEAD", VerifiedSourceTree: strings.Repeat("a", 40)}
		}},
	{name: "verified deployment publish rejects an app file", kind: gateCheck, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied, RuleVerifiedSource},
		prepare: func(t *testing.T, r string) { remove(t, r, ".sdo/diagnostics"); remove(t, r, ".sdo/playbooks") },
		edit:    gateWrite("app.txt", "deployed\n"),
		request: func(r *Request) {
			r.Publication = &PublicationContext{BaseCommit: "HEAD", VerifiedSourceTree: strings.Repeat("a", 40)}
		}},
	{name: "upstream advance needs a verified source tree", kind: gateCheck, actor: Upstream, phase: AdvancePhase, rules: []string{RuleVerifiedSource}, edit: gateWrite("app.txt", "upstream\n")},
	{name: "verified upstream advance passes", kind: gateCheck, actor: Upstream, phase: AdvancePhase, edit: gateWrite("app.txt", "upstream\n"),
		request: func(r *Request) {
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: "advance-0123456789ab", VerifiedSourceTree: strings.Repeat("a", 40)}
		}},
	{name: "upstream chart templates under a secrets-named directory advance", kind: gateCheck, actor: Upstream, phase: AdvancePhase,
		request: func(r *Request) {
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: "advance-0123456789ab", VerifiedSourceTree: strings.Repeat("a", 40)}
		},
		edit: func(t *testing.T, r string) {
			write(t, r, "chart/templates/shared-secrets/job.yaml", "kind: Job\nmetadata:\n  name: {{ .Release.Name }}-secrets\n")
			write(t, r, "chart/templates/_checkConfig_shared_secrets.tpl", "{{- define \"check\" -}}{{- end -}}\n")
		}},
	{name: "verified upstream advance may be empty", kind: gateCheck, actor: Upstream, phase: AdvancePhase,
		request: func(r *Request) {
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: "advance-0123456789ab", VerifiedSourceTree: strings.Repeat("a", 40)}
		}},
	{name: "health judge replaced generated policy", kind: gateCheck, actor: HealthJudge, phase: InitialPhase, rules: []string{RuleGenericHealth}, edit: func(t *testing.T, r string) {
		write(t, r, ".sdo/diagnostics/detectors/health/service-endpoints/manifest.yaml", strings.ReplaceAll(gateRead(t, r, gateHealthManifest), "id: health", "id: service-endpoints"))
		write(t, r, ".sdo/diagnostics/detectors/health/service-endpoints/detector.go", "package endpoints\n\n// authored policy override\n")
	}},
	gateReflection("snapshot budget", gateClean(), gateUpdated("large-a", "large-b", "large-c"), func(t *testing.T, r string) {
		for _, id := range []string{"large-a", "large-b", "large-c"} {
			write(t, r, ".sdo/playbooks/"+id+"/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: "+id+"\noriginating_incident: incident\n---\nRepair <RESOURCE_NAME>.\n"+strings.Repeat("Verified guidance paragraph.\n", 7000))
		}
		generateIndex(t, r)
	}, RuleSnapshotBudget),
	gateReflection("cluster-local value in memory", gateClean(), gateUpdated(), gateTrafficEdit("incident-x", "name: incident-x\nclusterIP: 10.96.0.12\n"), RuleClusterLocal),
	gateReflection("Secret data in memory", gateClean(), gateUpdated(), gateTrafficEdit("incident-x", "apiVersion: v1\nkind: Secret\nmetadata:\n  name: db\ndata:\n  password: c2VjcmV0\n"), RuleSecretData),
	gateReflection("credential material in memory", gateClean(), gateUpdated(), gateTrafficEdit("incident-x", "name: incident-x\nnote: key AKIA"+"ABCDEFGHIJKLMNOP\n"), RuleSecretMaterial),
	// Go derives the decision the agent never needs to get right: a diff that
	// holds a playbook is an update of exactly that playbook, an empty one is
	// no_change. An explicit decision that contradicts the diff still blocks.
	gateReflection("reflection without a decision derives an update from the diff", gateClean(), nil, gatePlaybookEdit),
	gateReflection("reflection without a decision and an empty diff derives no_change", gateClean(), nil, func(*testing.T, string) {}),
	gateReflection("reflection whose explicit decision contradicts the diff", gateClean(), gateNoChange, gatePlaybookEdit, RuleReflectionDecision),
	gateReflection("reflection updates a playbook", gateClean(), gateUpdated("base"), gatePlaybookEdit),
	gateWithRepairVerifier("README-only learning without a verified repaired cause", false, gateReflection("unverified closure updates a playbook", gateClean(), gateUpdated("base"), gatePlaybookEdit, RuleReflectionRepairRequired)),
	gateWithRepairVerifier("no_change without a verified repaired cause", false, gateReflection("unverified closure declines", gateClean(), gateNoChange, func(*testing.T, string) {})),
	gateWithRepairVerifier("README learning with a verified repaired cause", true, gateReflection("verified closure updates a playbook", gateClean(), gateUpdated("base"), gatePlaybookEdit)),
	// An escalated closure grants one update, a Diagnose-only playbook, and asks
	// for it unless an accepted playbook already escalated the fault.
	gateWithRepairVerifier("escalated closure declines", false, gateReflection("escalated closure declines", gateEscalatedOutcome(false), gateNoChange, func(*testing.T, string) {}, RuleReflectionEscalationDiagnose)),
	gateWithRepairVerifier("stage-escalated closure declines", false, gateReflection("stage-escalated closure declines", gateEscalatedOutcome(true), gateNoChange, func(*testing.T, string) {})),
	gateDiagnoseOnlyPlaybook("escalated closure saves a Diagnose-only playbook", false),
	gateDiagnoseOnlyPlaybook("escalated closure saves a repair", true, RuleReflectionRepairRequired),
	gateReflection("applied playbook failed and was not updated", gateStaleOutcome(), gateNoChange, func(*testing.T, string) {}, RuleReflectionStalePlaybook),
	// R6': without a verified repaired cause no update may publish, so the
	// check must not also demand that the applied playbook change; the two
	// rules together would refuse every result.
	gateWithRepairVerifier("unsettled playbook in a closure without a verified repair", false, gateReflection("unverified closure keeps its playbook", gateStaleOutcome(), gateNoChange, func(*testing.T, string) {})),
	gateWithRepairVerifier("failed playbook in a closure with a verified repair", true, gateReflection("verified closure keeps a failed playbook", gateStaleOutcome(), gateNoChange, func(*testing.T, string) {}, RuleReflectionStalePlaybook)),
	gateReflection("hand repair has no program", gateHandRepair(gateRepairOutcome()), gateNoChange, func(*testing.T, string) {}, RuleReflectionProgramRequired),
	gateReflection("sdk gap that is free text", gateHandRepair(gateRepairOutcome()),
		&contracts.ReflectionDecision{LearningDecision: "sdk_gap", NoChangeReason: "needs a primitive", SdkGaps: []*contracts.SdkGap{{FaultClass: "routing", Primitive: "do the repair by hand", Reason: "no primitive", ScratchScript: "inspect"}}},
		func(*testing.T, string) {}, RuleReflectionSDKGapUnavailable, RuleReflectionProgramRequired),
	gateDuplicateRepair("new playbook copies an existing program modulo names and prose", func(diagnose, mitigate string) (string, string) {
		return diagnose, strings.ReplaceAll(strings.ReplaceAll(mitigate, "use a selected pod port", "restore the api Service port"), "replacementValue", "value")
	}, RuleDuplicateRepair),
	func() gateRow {
		row := gateDuplicateRepair("new playbook that repeats only the repair is noted", func(diagnose, mitigate string) (string, string) {
			return strings.Replace(diagnose, "if len(pods) == 0 {", "if len(pods) < 2 {", 1), mitigate
		})
		row.notes = []string{RuleSharedRepair}
		return row
	}(),
	gateDuplicateRepair("new playbook with a different repair passes", func(diagnose, mitigate string) (string, string) {
		return diagnose, strings.ReplaceAll(mitigate, "spec.ports[%d].targetPort", "spec.ports[%d].port")
	}),
	gateReflection("diagnosis reads an annotation source comparison strips", gateClean(), gateUpdated("base"), func(t *testing.T, r string) {
		gatePlaybookEdit(t, r)
		write(t, r, ".sdo/playbooks/base/diagnose.go", gateFormatted(t, gateStrippedRead("annotations.Observed")))
	}, gateProgramInvalid, RuleProgramNormalizedRead),
	gateReflection("diagnosis reads the restart annotation on the live object", gateClean(), gateUpdated("base"), func(t *testing.T, r string) {
		gatePlaybookEdit(t, r)
		write(t, r, ".sdo/playbooks/base/diagnose.go", gateFormatted(t, gateStrippedRead("live.Spec.Template.Annotations")))
	}, gateProgramInvalid),
	gateReflection("diagnosis reuses a comparison's variable names for live reads", gateClean(), gateUpdated("base"), func(t *testing.T, r string) {
		gatePlaybookEdit(t, r)
		write(t, r, ".sdo/playbooks/base/diagnose.go", gateFormatted(t, gateLiveReadReusingNames))
	}, gateProgramInvalid),
	gateReflection("program names a live Pod", gateClean(), gateUpdated("base"), func(t *testing.T, r string) {
		gatePlaybookEdit(t, r)
		write(t, r, ".sdo/playbooks/base/mitigate.go", "package base\n\nconst repair = `kubectl -n app exec cache-b4d5dbdbf-vdxd5 -- redis-cli ACL SETUSER default +@read`\n")
	}, gateProgramInvalid, RuleLiveObjectName),
	gateReflection("script names a live Pod", gateClean(), gateUpdated("base"), func(t *testing.T, r string) {
		edit(t, r, ".sdo/playbooks/base/scripts/fix.sh", "\n", "\nkubectl exec api-7c9d8b6f45-x2lzq -- true\n")
	}, RuleLiveObjectName),
	gateReflection("script names a StatefulSet Pod and a Service", gateClean(), gateUpdated("base"), func(t *testing.T, r string) {
		edit(t, r, ".sdo/playbooks/base/scripts/fix.sh", "\n", "\nkubectl exec cache-0 -- true # svc/post-storage-service\n")
	}),
	gateReflection("incident detector binds roles without declaring a playbook", gateClean(), gateUpdated(), gateUnscopedBinding("possiblePlaybooks: []", "Playbooks: nil,"), RuleDetectorUnscopedBinding),
	gateReflection("incident detector binds roles for its declared playbook", gateClean(), gateUpdated(), gateUnscopedBinding("possiblePlaybooks:\n  - base", `Playbooks: []string{"base"},`)),
	{name: "accepted corpus case rewritten", kind: gateCheck, actor: Responder, phase: ReflectionPhase, rules: []string{gateProgramInvalid, RuleCorpusImmutable},
		prepare: func(t *testing.T, r string) {
			gateOutcome(gateClean())(t, r)
			write(t, r, ".sdo/corpus/case-1/labels.json", "{}\n")
		},
		baseMessage: gateOutcomeMessage,
		edit:        gateWrite(".sdo/corpus/case-1/labels.json", `{"changed":true}`+"\n"),
		request: func(r *Request) {
			r.Decision = gateUpdated()
			r.Publication = &PublicationContext{BaseCommit: "HEAD", IncidentID: gateIncident}
		}},
}

func gateStubCompile(context.Context, Request) ([]Diagnostic, error) { return nil, nil }

func runGateRow(t *testing.T, row gateRow) {
	root := goodTree(t)
	if row.prepare != nil {
		row.prepare(t, root)
	}
	var notes []Diagnostic
	request := Request{Root: root, CoverageGaps: &notes}
	var diagnostics []Diagnostic
	if row.kind == gateStatic {
		if row.edit != nil {
			row.edit(t, root)
		}
		if row.request != nil {
			row.request(&request)
		}
		request.Layers = Structural | Semantic
		diagnostics = diagnose(t, request)
	} else {
		message := row.baseMessage
		if message == "" {
			message = "base"
		}
		commit(t, root, message)
		if row.edit != nil {
			row.edit(t, root)
		}
		request.Base, request.Actor, request.Phase = "HEAD", row.actor, row.phase
		if row.request != nil {
			row.request(&request)
		}
		if row.kind == gateOwnership {
			request.Layers = Ownership
			diagnostics = diagnose(t, request)
		} else {
			var err error
			if diagnostics, err = Check(context.Background(), request, gateStubCompile); err != nil {
				t.Fatalf("Check returned an infrastructure error: %v", err)
			}
		}
	}
	if got := ruleSet(diagnostics); !same(got, row.rules) {
		t.Fatalf("rules = %v, want %v\n%v", got, row.rules, diagnostics)
	}
	for _, d := range diagnostics {
		if d.Message == "" {
			t.Errorf("diagnostic without a message: %+v", d)
		}
	}
	if row.path != "" {
		last := row.rules[len(row.rules)-1]
		if !slices.ContainsFunc(diagnostics, func(d Diagnostic) bool { return d.Rule == last && d.Path == row.path }) {
			t.Fatalf("no %s diagnostic at %s: %v", last, row.path, diagnostics)
		}
	}
	for _, want := range row.notes {
		if !slices.ContainsFunc(notes, func(d Diagnostic) bool { return d.Rule == want }) {
			t.Errorf("no %s note: %v", want, notes)
		}
	}
}

func TestGateTable(t *testing.T) {
	for _, row := range gateRows {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			runGateRow(t, row)
		})
	}
}

// A compile row is a candidate that passes every static layer and fails one Go
// stage. fake makes FakeGo fail that stage on the unchanged fixture; real edits
// the fixture so the real toolchain fails it. Both must produce the same
// rejection, which is what makes FakeGo an honest stand-in for the rows that do
// not need the compiler's own output.
type gateCompileRow struct {
	name string
	rule string
	// path is where the rejection points.
	path string
	fake testutil.GoRule
	// real mutates the compile fixture; baseline, if set, is written as the healthy capture.
	real     func(t *testing.T, root string)
	baseline bool
	// request adjusts the check request, for rows that need environment policy.
	request func(*Request)
}

const (
	gateFixtureDetector = ".sdo/diagnostics/detectors/incidents/missing-configmap/detector.go"
	gateFixtureTest     = ".sdo/diagnostics/detectors/incidents/missing-configmap/detector_test.go"
)

var gateCompileRows = []gateCompileRow{
	{name: "does not build", rule: RuleDetectorCompile, path: gateFixtureDetector,
		fake: testutil.GoRule{Verb: "build", Output: "detectors/incidents/missing-configmap/detector.go:17: undefined: undefinedValue"},
		real: func(t *testing.T, root string) {
			edit(t, root, gateFixtureDetector, "return Detector{}", "return undefinedValue")
		}},
	{name: "fails go vet", rule: RuleDetectorVet, path: gateFixtureDetector,
		fake: testutil.GoRule{Verb: "vet", Output: "detectors/incidents/missing-configmap/detector.go:17: fmt.Sprintf format %d has arg of wrong type string"},
		real: func(t *testing.T, root string) {
			edit(t, root, gateFixtureDetector, "return Detector{}", "_ = fmt.Sprintf(\"%d\", \"not a number\")\n\treturn Detector{}")
		}},
	{name: "fails its own test", rule: RuleDetectorTest, path: layout.DiagnosticsDir,
		fake: testutil.GoRule{Verb: "test", Output: "--- FAIL: TestDetectorGeneralizesAcrossNamesAndIgnoresOptionalReferences\n    detector_test.go:20: sentinel detector failure"},
		real: func(t *testing.T, root string) {
			edit(t, root, gateFixtureTest, "len(findings) != 1", "len(findings) == 1")
		}},
	{name: "fires on the healthy baseline", rule: RuleDetectorHealthyBaseline, path: layout.DetectorDir(layout.ClassIncident, "missing-configmap"), baseline: true,
		fake: testutil.GoRule{Verb: "test", ArgContains: gateHealthyBaselineStage, Output: "    healthy_baseline_test.go:15: detector.healthy-baseline[missing-configmap]: detector \"missing-configmap\" reported an active finding on healthy baseline snapshot empty-namespace.json"},
		real: gateFiresOnHealthyBaseline},
	// A healthy-baseline fault never hides another failure of the generated
	// tests: here a copied detector keeps the accepted Spec ID, so the
	// controller would refuse to start. The fault is reported, not the replay.
	{name: "healthy-baseline fault beside a duplicate detector ID", rule: RuleDetectorControllerStart, path: layout.DiagnosticsDir, baseline: true,
		fake: testutil.GoRule{Verb: "test", ArgContains: "./generated", ArgAbsent: gateHealthyBaselineStage, Output: "    healthy_baseline_test.go:15: detector.healthy-baseline[missing-configmap]: detector \"missing-configmap\" reported an active finding on healthy baseline snapshot empty-namespace.json\n--- FAIL: TestDetectorsLoadInController\n    controller_start_test.go:9: detector.controller-start: the controller would refuse to start: duplicate detector ID \"missing-configmap\""},
		real: func(t *testing.T, root string) {
			gateFiresOnHealthyBaseline(t, root)
			from := filepath.Join(root, filepath.Dir(gateFixtureDetector))
			to := filepath.Join(filepath.Dir(from), "copied-configmap")
			copyTree(t, from, to)
			edit(t, root, ".sdo/diagnostics/detectors/incidents/copied-configmap/manifest.yaml", "id: missing-configmap", "id: copied-configmap")
		}},
	{name: "a query names a source nothing binds", rule: RuleTelemetryUnbound, path: layout.DiagnosticsDir,
		fake: testutil.GoRule{Verb: "test", Output: "--- FAIL: TestDetectorQueriesResolveToBoundSources\n    telemetry_binding_test.go:30: telemetry.unbound[missing-configmap]: query \"q\": telemetry source \"nope\" is not bound; bound: none"},
		real: func(t *testing.T, root string) { gateQueriesSource(t, root, "nope") }},
	{name: "a query names a source of another kind", rule: RuleTelemetryKind, path: layout.DiagnosticsDir,
		request: func(r *Request) {
			r.Telemetry = telemetry.Policy{Provided: []telemetry.Source{{ID: "lines", Kinds: []telemetry.Kind{telemetry.KindLogs}, Adapter: "loki", Origin: telemetry.OriginOperator}}}
		},
		fake: testutil.GoRule{Verb: "test", Output: "--- FAIL: TestDetectorQueriesResolveToBoundSources\n    telemetry_binding_test.go:30: telemetry.kind[missing-configmap]: query \"q\": telemetry source \"lines\" does not serve kind \"metrics\"; bound: lines"},
		real: func(t *testing.T, root string) { gateQueriesSource(t, root, "lines") }},
	{name: "controller would refuse to start it", rule: RuleDetectorControllerStart, path: layout.DiagnosticsDir,
		fake: testutil.GoRule{Verb: "test", Output: "detector.controller-start: the controller would refuse to start: detector \"missing-configmap\": interval must be greater than zero"},
		real: func(t *testing.T, root string) {
			edit(t, root, gateFixtureDetector, "30 * time.Second", "-time.Second")
		}},
	{name: "a pod read without a Pod watch", rule: RuleDetectorControllerStart, path: layout.DiagnosticsDir,
		fake: testutil.GoRule{Verb: "test", Output: "detector.controller-start: the controller would refuse to start: detector \"missing-configmap\": pod reads require a v1 Pod watch"},
		real: func(t *testing.T, root string) { gatePodRead(t, root) }},
}

// gatePodRead makes the fixture detector declare a pod read.
func gatePodRead(t *testing.T, root string) {
	t.Helper()
	edit(t, root, gateFixtureDetector, "\"sdo.dev/operator/sdk\"\n)", "\"sdo.dev/operator/sdk\"\n\t\"sdo.dev/operator/sdk/podread\"\n)")
	edit(t, root, gateFixtureDetector, "func (Detector) Spec()", "func (Detector) PodReads() []podread.Query {\n\treturn []podread.Query{{ID: \"store-policy\", Target: podread.Target{Image: \"synthetic-store\"}, Args: []string{\"store-cli\", \"get\", \"policy\"}, Interval: time.Minute}}\n}\n\nfunc (Detector) Spec()")
}

// gateQueriesSource makes the fixture detector declare one metrics query per
// given source ("" names none, so the default-source rule applies).
func gateQueriesSource(t *testing.T, root string, sources ...string) {
	t.Helper()
	queries := ""
	for i, source := range sources {
		queries += fmt.Sprintf("{ID: \"q%d\", Source: %q, Expr: \"up\"}, ", i, source)
	}
	edit(t, root, gateFixtureDetector, "\"sdo.dev/operator/sdk\"\n)", "\"sdo.dev/operator/sdk\"\n\t\"sdo.dev/operator/sdk/metrics\"\n)")
	edit(t, root, gateFixtureDetector, "func (Detector) Spec()", "func (Detector) MetricQueries() []metrics.Query {\n\treturn []metrics.Query{"+queries+"}\n}\n\nfunc (Detector) Spec()")
}

// gateHealthyBaselineStage selects the generated healthy-baseline replay stage.
const gateHealthyBaselineStage = "-run ^TestIncidentDetectorsStayQuietOnHealthyBaseline$"

// gateFiresOnHealthyBaseline makes the fixture detector fire on an empty namespace.
func gateFiresOnHealthyBaseline(t *testing.T, root string) {
	edit(t, root, gateFixtureDetector, "findings := make([]sdk.Finding, 0)", "findings := make([]sdk.Finding, 0)\n\tif len(snapshot.Deployments()) == 0 {\n\t\treturn []sdk.Finding{{RuleID: \"missing-configmap\", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: \"always\", Evidence: \"always\", Fingerprint: \"always\"}}, nil\n\t}")
}

// gateBaseline writes the controller's healthy capture: an empty namespace.
func gateBaseline(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	snapshot, err := sdktest.CaptureSnapshot(sdktest.Snapshot{NamespaceName: "demo"}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	write(t, dir, "empty-namespace.json", string(raw))
	return dir
}

func gateCompileDiagnostics(t *testing.T, row gateCompileRow, root string) []Diagnostic {
	request := compileRequest(root)
	if row.request != nil {
		row.request(&request)
	}
	if row.baseline {
		request.HealthyBaseline = gateBaseline(t)
	}
	diagnostics, err := CompileCandidate(context.Background(), request)
	if err != nil {
		t.Fatalf("CompileCandidate returned an infrastructure error: %v", err)
	}
	return diagnostics
}

func assertGateCompile(t *testing.T, row gateCompileRow, diagnostics []Diagnostic) {
	t.Helper()
	if got := ruleSet(diagnostics); !same(got, []string{row.rule}) {
		t.Fatalf("rules = %v, want %s\n%v", got, row.rule, diagnostics)
	}
	if diagnostics[0].Path != row.path {
		t.Fatalf("rejection points at %q, want %q: %+v", diagnostics[0].Path, row.path, diagnostics[0])
	}
}

func TestGateCompileRowsOnFakeGo(t *testing.T) {
	for _, row := range gateCompileRows {
		t.Run(row.name, func(t *testing.T) {
			testutil.FakeGo(t, row.fake)
			assertGateCompile(t, row, gateCompileDiagnostics(t, row, compileTree(t)))
		})
	}
	t.Run("a candidate every stage accepts", func(t *testing.T) {
		testutil.FakeGo(t)
		if got := gateCompileDiagnostics(t, gateCompileRow{}, compileTree(t)); len(got) != 0 {
			t.Fatalf("rejected: %v", got)
		}
	})
}

// An informational rule never rejects: the candidate passes and the check
// reports the rule as a note.
type gateNoteRow struct {
	name, rule string
	notes      func(t *testing.T) []Diagnostic
}

func gateCompileNotes(t *testing.T) []Diagnostic {
	testutil.FakeGo(t)
	var notes []Diagnostic
	request := compileRequest(compileTree(t))
	request.HealthyBaseline = filepath.Join(t.TempDir(), "missing")
	request.CoverageGaps = &notes
	if ds, err := CompileCandidate(context.Background(), request); err != nil || len(ds) != 0 {
		t.Fatalf("informational notes rejected the candidate: %v %v", ds, err)
	}
	return notes
}

// gateUndetectedCause is a verified repair of Service demo/front whose
// finding is on another object, so no detector binds the program's roles.
func gateUndetectedCause(t *testing.T) []Diagnostic {
	root := t.TempDir()
	copyTree(t, gateProgramFixture, filepath.Join(root, ".sdo"))
	front := &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "front"}
	at := time.Unix(100, 0)
	outcome := &contracts.OutcomeRecord{IncidentId: "first", Classification: "success",
		ConfirmedRootCauses:      []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{front}}},
		Findings:                 []*contracts.Finding{{DetectorId: "fixture", Status: "active", Fingerprint: "other", PrimaryResource: &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "other"}}},
		RepairActions:            []*contracts.RepairActionReceipt{{ActionId: "fixture-front", Success: true, Resources: []*contracts.ObjectRef{front}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}},
		FinalHealthDetectorState: []*contracts.DetectorEvaluation{{DetectorId: "fixture", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}},
	}
	return ReflectionCoverageGaps(root, outcome, gateUpdated("service-targetport-mismatch"))
}

var gateNoteRows = []gateNoteRow{
	{"verified cause has a program no detector binds", RuleReflectionDetectorRequired, gateUndetectedCause},
	{"healthy replay input is missing", RuleReplayObservation, gateCompileNotes},
}

func TestGateNotes(t *testing.T) {
	for _, row := range gateNoteRows {
		t.Run(row.name, func(t *testing.T) {
			if !slices.ContainsFunc(row.notes(t), func(d Diagnostic) bool { return d.Rule == row.rule }) {
				t.Fatalf("no %s note", row.rule)
			}
		})
	}
}

// gateRuleCoverage names the rules with no bad row, each with the reason.
var gateRuleCoverage = map[string]string{
	RuleDataCheckInvalid:      "covered by data_checks_test.go (TestDataChecksAreValidatedAsReadOnlyAndBounded) and the datacheck package's validation table",
	RuleDataCheckComponent:    "covered by data_checks_test.go (TestDataChecksResolveToMapComponentsWithDataRoles): it needs an application map with data roles",
	RulePatchUncaptured:       "covered by the broker publish tests (broker/enforcement_integration_test.go): it needs the agent checkout's working tree, which the shared check never sees",
	RuleSnapshotProjection:    "no row yet: a defensive check behind the static layers; no authored change reaches it",
	RuleTrafficDefaultLatency: "no row yet: informational, needs generated traffic replay results from the real toolchain on a tree with health-probe scenarios",
	RuleMapMissing:            "covered by application_map_test.go (map rules are exercised with their own tiny-app fixtures)",
	RuleMapSchema:             "covered by application_map_test.go",
	RuleMapUnknownName:        "covered by application_map_test.go",
	RuleMapPathMismatch:       "covered by application_map_test.go (static path rules) and the builder's generated scenario test",
	RuleMapUncoveredOp:        "covered by application_map_test.go",
	RuleMapUncoveredWrite:     "covered by application_map_test.go",
	RuleMapUncoveredData:      "covered by application_map_test.go",
	RuleMapCoverage:           "covered by application_map_test.go",
	RuleMapUnprobed:           "covered by application_map_test.go",
	RuleMapKnownBroken:        "covered by application_map_test.go and the appmap package's known-broken tests",
	RuleMapPossibleEdge:       "covered by application_map_test.go",
	RuleMapOutsideRegion:      "covered by application_map_test.go",
	RuleMapRegionOwner:        "covered by application_map_test.go",
	RuleDetectorNone:          "never emitted: memory without detectors is valid (good row above)",
	RulePlaybookNone:          "never emitted: memory without playbooks is valid (good row above)",
	// Rules emitted outside Check's pipeline; ruleIDs reads rules.go only.
	"memory.conflict": "the broker's rule: it needs two proposals on a real repository, so it is tested in broker/memory_conflict_integration_test.go",
}

// gateRulesOutsideRulesGo are rule IDs that are string literals elsewhere in
// the package or in the broker, listed here so the coverage test notices them.
// shell.format has table rows (the "shell:" rows).
var gateRulesOutsideRulesGo = []string{"shell.format", "memory.conflict"}

func TestGateTableNamesEveryRule(t *testing.T) {
	covered := map[string]bool{}
	for _, row := range gateRows {
		for _, rule := range row.rules {
			covered[rule] = true
		}
		for _, rule := range row.notes {
			covered[rule] = true
		}
	}
	for _, row := range gateCompileRows {
		covered[row.rule] = true
	}
	for _, row := range gateNoteRows {
		covered[row.rule] = true
	}
	var missing []string
	for _, id := range append(ruleIDs(t), gateRulesOutsideRulesGo...) {
		if !covered[id] && gateRuleCoverage[id] == "" {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("rules without a gate table row: %v", missing)
	}
}

const gateDiffTextProgram = `package base

import (
	"context"
	"strings"

	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	diff, err := v.DiffAgainstSource(b.Objects["deployment"])
	if err != nil {
		return program.Verdict{}, err
	}
	if !strings.Contains(diff.Text, "image") {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Observed: diff.Text}}}, nil
}

`

func gateFormatted(t *testing.T, source string) string {
	t.Helper()
	out, err := format.Source([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func gateRead(t *testing.T, root, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// gateAppFileDenied is the negative control for the .sdo/-only rule: no agent
// actor may change an app file in any phase (only the upstream advance may).
func gateAppFileDenied(actor Actor, phase Phase) gateRow {
	return gateRow{name: fmt.Sprintf("%s may not change an app file in %s", actor, phase), kind: gateOwnership, actor: actor, phase: phase,
		rules: []string{RuleOwnershipDenied}, edit: gateWrite("app.txt", "edit\n")}
}

// gateStrandedDetector writes a synthetic health detector package with the
// given originating commit ("lifecycle-bootstrap" marks a Go-installed one).
func gateStrandedDetector(t *testing.T, root, id, origin string) {
	t.Helper()
	manifest := strings.Replace(gateRead(t, root, gateHealthManifest), "id: health", "id: "+id, 1)
	manifest = strings.Replace(manifest, "originatingCommit: initial", "originatingCommit: "+origin, 1)
	write(t, root, layout.DetectorManifestPath(layout.ClassHealth, id), manifest)
	write(t, root, layout.DetectorDir(layout.ClassHealth, id)+"/detector.go", "package synthetic\n")
}
