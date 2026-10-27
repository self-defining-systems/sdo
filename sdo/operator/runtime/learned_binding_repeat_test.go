package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/builder"
	"sdo.dev/operator/runtime/jitview"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

// Synthetic learned source repairs share the live evidence shape: one required
// resource role, with no authored link from the generic opening health detector.
type learnedRepeatProgram struct {
	program.Diagnoser
	program.Mitigator
	spec program.ProgramSpec
	role string
}

func (p learnedRepeatProgram) Spec() program.ProgramSpec { return p.spec }
func (p learnedRepeatProgram) original(b program.Bindings) program.Bindings {
	return program.Bindings{Objects: program.ObjectBindings{p.role: b.Objects["resource"]}}
}
func (p learnedRepeatProgram) Diagnose(ctx context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	return p.Diagnoser.Diagnose(ctx, v, p.original(b))
}
func (p learnedRepeatProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	return p.Mitigator.Mitigate(ctx, v, p.original(b))
}
func TestLearnedSourceRepairsBindOnRepeat(t *testing.T) {
	for _, shape := range []struct {
		id, kind, role string
		p              interface {
			program.Diagnoser
			program.Mitigator
		}
	}{
		{"deployment-template-drift", "Deployment", "deployment", pipelineImageProgram{}},
		{"service-source-drift", "Service", "service", pipelinePortProgram{}},
	} {
		for _, extra := range []string{"none", "optional-peer", "scalar-default", "string-default", "bool-default", "duration-default", "quantity-default"} {
			t.Run(shape.id+"/"+extra, func(t *testing.T) {
				spec := program.ProgramSpec{ID: shape.id, Params: []program.ParamSpec{{Name: "resource", Kind: program.Kind(shape.kind), Required: true}}}
				switch extra {
				case "optional-peer":
					spec.Params = append(spec.Params, program.ParamSpec{Name: "peer", Kind: "ConfigMap"})
				case "scalar-default":
					spec.Params = append(spec.Params, program.ParamSpec{Name: "limit", ValueKind: program.IntValue, Required: true, Default: 1})
				case "string-default":
					spec.Params = append(spec.Params, program.ParamSpec{Name: "limit", ValueKind: program.StringValue, Required: true, Default: "synthetic"})
				case "bool-default":
					spec.Params = append(spec.Params, program.ParamSpec{Name: "limit", ValueKind: program.BoolValue, Required: true, Default: false})
				case "duration-default":
					spec.Params = append(spec.Params, program.ParamSpec{Name: "limit", ValueKind: program.DurationValue, Required: true, Default: "1s"})
				case "quantity-default":
					spec.Params = append(spec.Params, program.ParamSpec{Name: "limit", ValueKind: program.QuantityValue, Required: true, Default: "100m"})
				}
				learned := learnedRepeatProgram{Diagnoser: shape.p, Mitigator: shape.p, spec: spec, role: shape.role}
				registry, err := program.NewRegistry([]program.Diagnoser{learned}, []program.Mitigator{learned})
				if err != nil {
					t.Fatal(err)
				}
				var previousFingerprint string
				var repository, commit string
				for incident := 0; incident < 2; incident++ {
					if incident == 1 && extra == "none" {
						registry, repository, commit = savedLearnedRepeatRegistry(t, shape.id, shape.kind)
					}
					loop, state := pipelineTestLoop(t, false)
					loop.Programs = registry
					if repository != "" {
						state.Request.RepositoryWorktree, state.Request.SourceCommit = repository, commit
					}
					for i := range state.Request.Findings {
						f := &state.Request.Findings[i]
						f.Playbooks = nil
						f.Fingerprint = fmt.Sprintf("synthetic-%d-%d", incident, i)
					}
					state.Findings = state.Request.Findings
					loop.Request = state.Request
					loop.findings = state.Findings
					state.Request.InitialProgramRuns = loop.InitialDiagnose(context.Background())
					state.Coverage = buildPlaybookCoverage(state.Findings, state.Request.InitialProgramRuns, loop.lastDiagnosisView, nil)
					state.Request.PlaybookCoverage = state.Coverage
					loop.Request = state.Request
					if len(state.Coverage.Matches) != 1 {
						t.Fatalf("live diagnosis did not match: %v", state.Coverage)
					}
					if len(state.Coverage.Matches[0].ExplainedFindingFingerprints) != 0 {
						t.Fatal("fixture must not invent authored coverage")
					}
					fingerprint := state.Request.InitialProgramRuns[0].FindingFingerprint
					if fingerprint == previousFingerprint {
						t.Fatal("repeat reused first incident fingerprint")
					}
					previousFingerprint = fingerprint
					report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
					if err != nil {
						t.Fatal(err)
					}
					if len(report.Mitigations) != 1 || !report.Mitigations[0].GetApplyReceipt().GetApplied() || report.Mitigations[0].Result != "applied_cleared" || report.Mitigations[0].GetPostApplyDiagnose().GetVerdict().GetResult() != contracts.Verdict_RESULT_NO_MATCH {
						t.Fatalf("matched learned repair failed to apply on incident %d: %v", incident, report)
					}
				}
			})
		}
	}
}

// Save first-incident learning as source and load it through the real compiler
// and process boundary for the repeat, rather than substituting a test callback.
func savedLearnedRepeatRegistry(t *testing.T, id, kind string) (program.ProgramRegistry, string, string) {
	t.Helper()
	root := nativeRepository(t)
	read, condition, path := "Services", "o.Spec.Ports[0].TargetPort.IntVal != 8080", "spec.ports[0].targetPort"
	if kind == "Deployment" {
		read, condition, path = "Deployments", `o.Spec.Template.Spec.Containers[0].Image != "good:v1"`, "spec.template.spec.containers[0].image"
	}
	source := fmt.Sprintf(`package repair
import ("context";"sdo.dev/operator/sdk/program")
type Program struct{}
func (Program) Spec() program.ProgramSpec { return program.ProgramSpec{ID:%q,Params:[]program.ParamSpec{{Name:"resource",Kind:%q,Required:true}}} }
func (Program) Diagnose(_ context.Context, v program.ReadView,b program.Bindings)(program.Verdict,error){
 ref:=b.Objects["resource"]; for _,o:=range v.%s(){if o.Name==ref.Name && o.Namespace==ref.Namespace && (%s){return program.Verdict{Result:program.Match},nil}};return program.Verdict{Result:program.NoMatch},nil
}
func (Program) Mitigate(_ context.Context,_ program.ReadView,b program.Bindings)(program.Plan,error){ref:=b.Objects["resource"];return program.Plan{Actions:[]program.Action{program.SetField{Ref:ref,Path:%q,Value:program.FromSource(ref,%q),Reason:"restore synthetic accepted source"}}},nil}
`, id, kind, read, condition, path, path)
	dir := filepath.Join(root, ".sdo/playbooks", id)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"diagnose.go": source, "README.md": "---\nfault_class: " + id + "\n---\n# Synthetic first-incident learning\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "save synthetic first-incident repair")
	workspace, binary, err := builder.BuildWorkingProgram(context.Background(), builder.Config{ApplicationRoot: root, OperatorRoot: ".."}, id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workspace.Close() })
	raw, err := jitview.Run(context.Background(), binary, jitRequest{Command: "spec"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var specs []jitSpec
	if err := json.Unmarshal(raw, &specs); err != nil || len(specs) != 1 {
		t.Fatalf("compiled declaration: %s %v", raw, err)
	}
	p := &workingProgram{spec: specs[0].Spec, canMitigate: specs[0].CanMitigate, binary: binary, workspace: workspace}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	return registry, root, nativeGit(t, root, "rev-parse", "HEAD")
}

func TestMatchedToolUsesLiveDiagnosisBindingsWithoutAuthoredCoverage(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	for i := range state.Request.Findings {
		state.Request.Findings[i].Playbooks = nil
	}
	loop.Request = state.Request
	loop.findings = state.Request.Findings
	runs := loop.InitialDiagnose(context.Background())
	loop.Request.InitialProgramRuns = runs
	loop.Request.PlaybookCoverage = buildPlaybookCoverage(loop.Request.Findings, runs, loop.lastDiagnosisView, nil)
	if len(loop.Request.PlaybookCoverage.Matches) != 1 || len(loop.Request.PlaybookCoverage.Matches[0].ExplainedFindingFingerprints) != 0 {
		t.Fatal("fixture must have a live match without authored explanation")
	}
	loop.MenuConfig = &ResponderMenuConfig{}
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "service-targetport-mismatch", Match: true})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	var selection string
	for _, option := range response.Menu.Options {
		if option.ProgramId == "service-targetport-mismatch" {
			selection = option.OptionId
			if !programBindingsState(option.Bindings).Equal(runs[0].Bindings) {
				t.Fatalf("menu changed live diagnosis bindings: %v", option)
			}
		}
	}
	if selection == "" {
		t.Fatal("matched program was not offered")
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: selection, RoundLimit: 1}})
	if applied.Error != "" || len(loop.PlanApplications()) != 1 || !loop.PlanApplications()[0].Result.Applied || !applied.TargetedHealthy || applied.Healthy {
		t.Fatalf("matched repair must verify its diagnosed finding while the peer remains unhealthy: %+v", applied)
	}
}
