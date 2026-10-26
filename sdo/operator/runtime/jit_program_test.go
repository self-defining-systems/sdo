package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

const syntheticWorkingProgram = `package repair
import("context";"sdo.dev/operator/sdk/program")
type Program struct{}
func(Program) Spec()program.ProgramSpec{return program.ProgramSpec{ID:"service-targetport-mismatch",Params:[]program.ParamSpec{{Name:"service",Kind:"Service",Required:true},{Name:"port",ValueKind:program.IntValue,Default:8080}}}}
func(Program) Diagnose(_ context.Context,v program.ReadView,b program.Bindings)(program.Verdict,error){for _,s:=range v.Services(){if s.Name==b.Objects["service"].Name && s.Spec.Ports[0].TargetPort.IntVal==8080{return program.Verdict{Result:program.NoMatch},nil}};return program.Verdict{Result:program.Match},nil}
func(Program) Mitigate(_ context.Context,v program.ReadView,b program.Bindings)(program.Plan,error){return program.Plan{Rationale:"VERSION",Actions:[]program.Action{program.SetField{Ref:b.Objects["service"],Path:"spec.ports[0].targetPort",Value:program.FromParam("port"),Reason:"synthetic repair"}}},nil}
`

func workingProgramLoop(t *testing.T) (*ResponderLoop, program.Bindings, func(string, string, string)) {
	t.Helper()
	loop, client := trustedPortLoop(t, false, true, true)
	loop.Executor.Verifier = PlanHealthVerifier{Provider: loop.Provider, Detectors: loop.Detectors}
	loop.Executor.Rollouts = KubernetesPlanRollouts{Client: client}
	root := nativeRepository(t)
	write := func(id, name, body string) {
		t.Helper()
		dir := filepath.Join(root, ".sdo/playbooks", id)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("service-targetport-mismatch", "README.md", "---\nfault_class: service-targetport-mismatch\n---\n# Synthetic port repair\n")
	write("service-targetport-mismatch", "diagnose.go", strings.ReplaceAll(syntheticWorkingProgram, "VERSION", "accepted"))
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "accepted program")
	loop.WorkingProgramRoot = root
	loop.Request.RepositoryWorktree = root
	loop.Request.SourceCommit = nativeGit(t, root, "rev-parse", "HEAD")
	loop.MenuConfig = &ResponderMenuConfig{}
	t.Setenv("SDO_OPERATOR_ROOT", "..")
	t.Cleanup(loop.CloseWorkingPrograms)
	bindings := program.Bindings{Objects: program.ObjectBindings{"service": loop.Request.Findings[0].PrimaryResource}}
	return loop, bindings, write
}

func TestResponderPlanUsesEditedWorkingProgram(t *testing.T) {
	loop, bindings, write := workingProgramLoop(t)
	// InitialDiagnose cached the accepted mitigation before the responder edit.
	write("service-targetport-mismatch", "diagnose.go", strings.ReplaceAll(syntheticWorkingProgram, "VERSION", "working"))
	response := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	if got := loop.plans[response.PlanID].plan.Rationale; got != "working" {
		t.Fatalf("ran stale accepted program: rationale=%q", got)
	}
	revision := response.ProgramRevisions["service-targetport-mismatch"]
	if !strings.HasPrefix(revision, workingTreeRevisionPrefix) {
		t.Fatalf("missing working revision: %+v", response)
	}
	// Repeated commands reuse the immutable compiled artifact.
	before := len(loop.workingPrograms)
	diagnosed := loop.Handle(context.Background(), ToolRequest{Command: "diagnose", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if diagnosed.Error != "" || len(diagnosed.ProgramRuns) != 1 || diagnosed.ProgramRuns[0].ProgramRevision != revision || len(loop.workingPrograms) != before {
		t.Fatalf("diagnosis lost revision/cache: %+v", diagnosed)
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{response.PlanID}})
	if applied.Error != "" || len(applied.ProgramRuns) != 1 || applied.ProgramRuns[0].ProgramRevision != revision || applied.ProgramRuns[0].Verdict.Result != program.NoMatch {
		t.Fatalf("edited repair did not execute and verify: %+v", applied)
	}
	runs := loop.ProgramRuns()
	fillProgramRevisions(context.Background(), runs, loop.Request.RepositoryWorktree, loop.Request.SourceCommit)
	if runs[len(runs)-1].ProgramRevision != revision {
		t.Fatal("outcome enrichment overwrote working revision")
	}
	var receipt ToolResponse
	calls := loop.ToolCalls()
	if err := json.Unmarshal([]byte(calls[len(calls)-1].ResponseJSON), &receipt); err != nil {
		t.Fatal(err)
	}
	if len(receipt.ProgramRuns) != 1 || receipt.ProgramRuns[0].ProgramRevision != revision {
		t.Fatalf("receipt lost working revision: %+v", receipt)
	}
}

func TestResponderNewWorkingProgramAndCompileError(t *testing.T) {
	loop, bindings, write := workingProgramLoop(t)
	const id = "new-port-repair"
	write(id, "README.md", "---\nfault_class: "+id+"\n---\n# Synthetic new repair\n")
	source := strings.ReplaceAll(strings.ReplaceAll(syntheticWorkingProgram, "service-targetport-mismatch", id), "VERSION", "new")
	write(id, "diagnose.go", source)
	response := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: id, Bindings: bindings})
	if response.Error != "" || loop.plans[response.PlanID].plan.Rationale != "new" {
		t.Fatalf("new program unavailable: %+v", response)
	}
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: id, Bindings: bindings})
	if menu.Error != "" {
		t.Fatal(menu.Error)
	}
	var option string
	for _, item := range menu.Menu.Options {
		if item.ProgramId == id {
			option = item.OptionId
			if !strings.HasPrefix(item.ProgramRevision, workingTreeRevisionPrefix) {
				t.Fatal("menu hid working revision")
			}
		}
	}
	if option == "" {
		t.Fatal("new program absent from menu")
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if applied.Error != "" || len(applied.ProgramRuns) != 1 || applied.ProgramRuns[0].Verdict.Result != program.NoMatch {
		t.Fatalf("new program did not mitigate: %+v", applied)
	}
	// Type errors fail the next tool call, without falling back to the cached binary.
	write(id, "diagnose.go", strings.ReplaceAll(source, "program.FromParam(\"port\")", "program.DoesNotExist()"))
	failed := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: id, Bindings: bindings})
	if !strings.Contains(failed.Error, "undefined: program.DoesNotExist") || len(failed.Error) > 1500 || failed.PlanID != "" {
		t.Fatalf("compile error was hidden or unbounded: %+v", failed)
	}
}

func TestResponderPreparedPlanPinsItsProgramRevision(t *testing.T) {
	loop, bindings, write := workingProgramLoop(t)
	write("service-targetport-mismatch", "diagnose.go", strings.ReplaceAll(syntheticWorkingProgram, "VERSION", "first"))
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if planned.Error != "" {
		t.Fatal(planned.Error)
	}
	write("service-targetport-mismatch", "diagnose.go", strings.ReplaceAll(syntheticWorkingProgram, "VERSION", "second"))
	latest := loop.Handle(context.Background(), ToolRequest{Command: "diagnose", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if latest.Error != "" {
		t.Fatal(latest.Error)
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" || len(applied.ProgramRuns) != 1 || applied.ProgramRuns[0].ProgramRevision != planned.ProgramRevisions["service-targetport-mismatch"] {
		t.Fatalf("prepared plan acquired a different revision: %+v", applied)
	}
}

func TestResponderWorkingCompileFailureNeverUsesAcceptedProgram(t *testing.T) {
	loop, bindings, write := workingProgramLoop(t)
	write("service-targetport-mismatch", "diagnose.go", "package repair\nTHIS IS INVALID GO\n")
	for _, command := range []string{"diagnose", "plan", "mitigate"} {
		started := time.Now()
		response := loop.Handle(context.Background(), ToolRequest{Command: command, ProgramID: "service-targetport-mismatch", Bindings: bindings})
		if response.Error == "" || !strings.Contains(response.Error, "fix the checkout source") || len(response.ProgramRuns) > 0 || response.PlanID != "" {
			t.Fatalf("%s fell back to accepted code: %+v", command, response)
		}
		if time.Since(started) > 5*time.Second {
			t.Fatalf("syntax error took too long: %s", time.Since(started))
		}
	}
}

func TestResponderWorkingProgramUsesSharedDependencyAndAcceptedModule(t *testing.T) {
	loop, bindings, write := workingProgramLoop(t)
	library := filepath.Join(loop.Request.RepositoryWorktree, ".sdo/lib/message")
	if err := os.MkdirAll(library, 0755); err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(library, "message.go")
	if err := os.WriteFile(helper, []byte("package message\nconst Rationale=\"accepted helper\"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	source := strings.Replace(syntheticWorkingProgram, `import(`, `import("app-diagnostics/lib/message";`, 1)
	source = strings.Replace(source, `Rationale:"VERSION"`, `Rationale:message.Rationale`, 1)
	write("service-targetport-mismatch", "diagnose.go", source)
	nativeGit(t, loop.Request.RepositoryWorktree, "add", ".")
	nativeGit(t, loop.Request.RepositoryWorktree, "commit", "-qm", "accepted helper")
	loop.Request.SourceCommit = nativeGit(t, loop.Request.RepositoryWorktree, "rev-parse", "HEAD")
	if err := os.WriteFile(helper, []byte("package message\nconst Rationale=\"working helper\"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	// Detector edits are outside incident-time JIT: never load their go.mod.
	if err := os.MkdirAll(filepath.Join(loop.Request.RepositoryWorktree, ".sdo/diagnostics"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(loop.Request.RepositoryWorktree, ".sdo/diagnostics/go.mod"), []byte("module wrong-module\n"), 0644); err != nil {
		t.Fatal(err)
	}
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if planned.Error != "" || loop.plans[planned.PlanID].plan.Rationale != "working helper" {
		t.Fatalf("shared edit/module boundary lost: %+v", planned)
	}
	prior := len(loop.workingPrograms)
	unrelated := filepath.Join(loop.Request.RepositoryWorktree, ".sdo/lib/unrelated")
	if err := os.MkdirAll(unrelated, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unrelated, "other.go"), []byte("package unrelated\nconst Value=1\n"), 0644); err != nil {
		t.Fatal(err)
	}
	again := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if again.Error != "" || len(loop.workingPrograms) != prior || again.ProgramRevisions["service-targetport-mismatch"] != planned.ProgramRevisions["service-targetport-mismatch"] {
		t.Fatalf("unrelated helper invalidated program identity: %+v", again)
	}
}

func TestHermeticResponderWorkingPlaybookCLI(t *testing.T) {
	loop, _, write := workingProgramLoop(t)
	write("service-targetport-mismatch", "diagnose.go", strings.ReplaceAll(syntheticWorkingProgram, "VERSION", "scripted responder edit"))
	directory := filepath.Join(t.TempDir(), "tools")
	server := SessionToolServer{Directory: directory, Handle: loop.Handle, PollInterval: time.Millisecond}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = server.Serve(ctx) }()
	defer func() { stop(); <-stopped; _ = server.Close() }()
	planned := fastloopTool(t, directory, "plan", "service-targetport-mismatch", "--bind", "service=Service/app/api")
	if !strings.HasPrefix(planned.ProgramRevisions["service-targetport-mismatch"], workingTreeRevisionPrefix) {
		t.Fatalf("CLI lost working revision: %+v", planned)
	}
	if loop.plans[planned.PlanID].plan.Rationale != "scripted responder edit" {
		t.Fatal("real CLI planned stale accepted code")
	}
	applied := fastloopTool(t, directory, "apply", planned.PlanID)
	if applied.Error != "" || len(applied.ProgramRuns) != 1 || applied.ProgramRuns[0].Verdict.Result != program.NoMatch || applied.ProgramRuns[0].ProgramRevision != planned.ProgramRevisions["service-targetport-mismatch"] {
		t.Fatalf("real CLI did not apply/verify exact working revision: %+v", applied)
	}
}

func TestWorkingProgramRevisionReceiptsStayBounded(t *testing.T) {
	response := ToolResponse{Command: "mitigate", Summary: "synthetic many-program response", ProgramRevisions: map[string]string{}}
	for i := 0; i < 32; i++ {
		response.ProgramRevisions[fmt.Sprintf("playbook-%02d-%s", i, strings.Repeat("a", 100))] = workingTreeRevisionPrefix + strings.Repeat("b", 64)
	}
	recorded := recordedToolResponseJSON(response, nil)
	if len(recorded) > maxRecordedToolResponse {
		t.Fatalf("revision evidence overflowed bounded record: %d", len(recorded))
	}
	var counts struct {
		Truncated             bool `json:"truncated"`
		ProgramRevisionsCount int  `json:"program_revisions_count"`
	}
	if err := json.Unmarshal(recorded, &counts); err != nil {
		t.Fatal(err)
	}
	if !counts.Truncated || counts.ProgramRevisionsCount != 32 {
		t.Fatalf("revision omission must be explicit: %s", recorded)
	}
}

func TestResponderWorkingProgramPreservesIntegerBindingsAndDefaults(t *testing.T) {
	loop, bindings, write := workingProgramLoop(t)
	const large = "9223372036854775806"
	source := strings.Replace(syntheticWorkingProgram, `import(`, `import("fmt";`, 1)
	source = strings.Replace(source, `Default:8080}}`, `Default:8080},{Name:"limit",ValueKind:program.IntValue,Default:9223372036854775806}}`, 1)
	source = strings.Replace(source, `for _,s:=range v.Services()`, `if fmt.Sprint(b.Values["limit"])!="9223372036854775806" {return program.Verdict{Result:program.Unknown},nil};for _,s:=range v.Services()`, 1)
	write("service-targetport-mismatch", "diagnose.go", source)
	bindings.Values = map[string]any{"limit": int64(9223372036854775806)}
	response := loop.Handle(context.Background(), ToolRequest{Command: "diagnose", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if response.Error != "" || len(response.ProgramRuns) != 1 || response.ProgramRuns[0].Verdict.Result != program.Match {
		t.Fatalf("integer binding changed at process boundary: %+v", response)
	}
	spec, _ := loop.Programs.Diagnoser("service-targetport-mismatch")
	encoded, err := json.Marshal(spec.Spec().Params[2].Default)
	if err != nil || string(encoded) != large {
		t.Fatalf("integer default changed: %s %v", encoded, err)
	}
}

func TestBrokenWorkingProgramDoesNotBlockRawMitigation(t *testing.T) {
	loop, _, write := workingProgramLoop(t)
	write("broken-draft", "README.md", "---\nfault_class: broken-draft\n---\n# In progress\n")
	write("broken-draft", "diagnose.go", "package repair\ninvalid working draft\n")
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if menu.Error != "" {
		t.Fatalf("broken draft poisoned the menu: %s", menu.Error)
	}
	found := false
	for _, option := range menu.Menu.Options {
		if option.ProgramId == "broken-draft" {
			found = option.DisabledReason != "" && len(option.DisabledReason) <= 256
		}
	}
	if !found {
		t.Fatal("broken draft must have an actionable disabled option")
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "SetField", RoundLimit: 1}, Parameters: map[string]string{"ref": "Service/app/api", "path": "spec.ports[0].targetPort", "value": "8080", "reason": "synthetic independent repair"}})
	if response.Error != "" || response.ApplicationID == "" {
		t.Fatalf("unrelated raw repair was blocked: %+v", response)
	}
}

func TestResponderDeletedNewProgramCannotRunCachedBinary(t *testing.T) {
	loop, bindings, write := workingProgramLoop(t)
	const id = "temporary-repair"
	write(id, "README.md", "---\nfault_class: "+id+"\n---\n# Temporary program\n")
	write(id, "diagnose.go", strings.ReplaceAll(syntheticWorkingProgram, "service-targetport-mismatch", id))
	first := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: id, Bindings: bindings})
	if first.Error != "" {
		t.Fatal(first.Error)
	}
	if err := os.Remove(filepath.Join(loop.WorkingProgramRoot, ".sdo/playbooks", id, "diagnose.go")); err != nil {
		t.Fatal(err)
	}
	after := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: id, Bindings: bindings})
	if after.Error == "" || after.PlanID != "" {
		t.Fatalf("deleted program ran cached code: %+v", after)
	}
}
