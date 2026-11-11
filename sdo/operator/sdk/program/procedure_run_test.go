package program_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// procedureSnapshot extends the synthetic map with procedures the
// application ships: a check and a data task in the frontend, and a Job.
func procedureSnapshot(liveJob bool) sdktest.Snapshot {
	s := mapSnapshot()
	m := *s.ApplicationMap
	m.Procedures = []program.Procedure{
		{ID: "front-check", Component: "front", Container: "web", Command: []string{"front-admin", "check", "--level={{level}}"}, Params: []program.ProcedureParam{{Name: "level", Type: program.ProcedureInt, Default: 1}}, Effect: program.ProcedureCheck, Source: "docs/ops.md"},
		{ID: "front-purge", Component: "front", Container: "web", Command: []string{"front-admin", "purge", "{{user}}"}, Params: []program.ProcedureParam{{Name: "user", Type: program.ProcedureString, Required: true}}, Effect: program.ProcedureData, Timeout: 2 * time.Minute, Source: "docs/ops.md"},
		{ID: "front-migrate", Job: "Job/front-migrate", Effect: program.ProcedureLong, Source: "manifest Job/front-migrate"},
	}
	s.ApplicationMap = &m
	yes := true
	s.ReplicaSetList = append(s.ReplicaSetList, appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "front-1", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "front", Controller: &yes}}}})
	if liveJob {
		s.JobList = []batchv1.Job{{TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: metav1.ObjectMeta{Name: "front-migrate", Namespace: "app", UID: "u1"}}}
	}
	return s
}

func TestRunProcedureRendersTheBoundCommandAsAScript(t *testing.T) {
	view := procedureSnapshot(false).RuntimeReadView()
	action, err := program.RunProcedure(view, "front-purge", map[string]any{"user": "o'brien"})
	if err == nil {
		t.Fatalf("a value outside the default alphabet bound: %+v", action)
	}
	action, err = program.RunProcedure(view, "front-purge", map[string]any{"user": "user-42"})
	if err != nil {
		t.Fatal(err)
	}
	script, ok := action.(program.RunScript)
	if !ok {
		t.Fatalf("action = %T", action)
	}
	if err := script.Validate(); err != nil {
		t.Fatal(err)
	}
	if script.Timeout != 2*time.Minute {
		t.Fatalf("timeout = %s", script.Timeout)
	}
	for _, want := range []string{
		`kubectl exec -n "$ns" 'deployment/front' -c 'web' -- 'front-admin' 'purge' 'user-42'`,
		`echo "sdo-procedure-exit $code"`,
	} {
		if !strings.Contains(script.Script, want) {
			t.Fatalf("script lacks %q:\n%s", want, script.Script)
		}
	}
	run, ok := program.ProcedureOf(script)
	if !ok || run.ID != "front-purge" || run.Effect != program.ProcedureData || run.Params["user"] != "user-42" {
		t.Fatalf("ProcedureOf = %+v, %v", run, ok)
	}
	again, _ := program.RunProcedure(view, "front-purge", map[string]any{"user": "user-42"})
	if again.(program.RunScript).Script != script.Script {
		t.Fatal("equal runs render different bytes")
	}
}

// The accepted map is the authority of a procedure script: exactly the bytes
// RunProcedure renders for an accepted entry pass, and any edit fails.
func TestAcceptedProcedureScriptBindsTheMapEntry(t *testing.T) {
	view := procedureSnapshot(false).RuntimeReadView()
	m, _ := view.AppMap()
	action, err := program.RunProcedure(view, "front-check", nil)
	if err != nil {
		t.Fatal(err)
	}
	script := action.(program.RunScript).Script
	if !program.AcceptedProcedureScript(m, script) {
		t.Fatalf("rendered script not accepted:\n%s", script)
	}
	// A recorded map replays JSON numbers as float64; the bytes still match.
	raw, _ := json.Marshal(m)
	var replayed program.AppMap
	if err := json.Unmarshal(raw, &replayed); err != nil || !program.AcceptedProcedureScript(replayed, script) {
		t.Fatalf("replayed map does not accept the script: %v", err)
	}
	for name, edited := range map[string]string{
		"extra command": script + "rm -rf /\n",
		"other args":    strings.Replace(script, "'--level=1'", "'--level=2'", 1),
		"not a run":     "#!/bin/bash\necho hi\n",
	} {
		if program.AcceptedProcedureScript(m, edited) {
			t.Fatalf("%s: edited script accepted", name)
		}
	}
	m.Procedures = nil
	if program.AcceptedProcedureScript(m, script) {
		t.Fatal("a script of a procedure no longer in the map was accepted")
	}
}

func TestRunProcedureRecreatesAJob(t *testing.T) {
	live, err := program.RunProcedure(procedureSnapshot(true).RuntimeReadView(), "front-migrate", nil)
	if err != nil {
		t.Fatal(err)
	}
	script, ok := live.(program.RunScript)
	if !ok || !strings.Contains(script.Script, "kubectl create -f") || !strings.Contains(script.Script, "--for=condition=complete --timeout=60s job/'front-migrate'") {
		t.Fatalf("live Job action = %#v", live)
	}
	gone, err := program.RunProcedure(procedureSnapshot(false).RuntimeReadView(), "front-migrate", nil)
	if err != nil {
		t.Fatal(err)
	}
	create, ok := gone.(program.CreateFromSource)
	if !ok || create.Ref.Kind != "Job" || create.Ref.Name != "front-migrate" || create.Ref.Namespace != "app" {
		t.Fatalf("absent Job action = %#v", gone)
	}
	if run, ok := program.ProcedureOf(create); !ok || run.Effect != program.ProcedureLong {
		t.Fatalf("ProcedureOf(create) = %+v, %v", run, ok)
	}
}

func TestObserveProcedureIsARecordedDiagnoseRead(t *testing.T) {
	s := procedureSnapshot(false)
	op := program.ExternalOperation{Exec: &program.Command{Pod: podRef("front-x"), Container: "web", Args: []string{"front-admin", "check", "--level=3"}}}
	args, _ := json.Marshal([]any{op})
	s.Readings = []sdktest.PrimitiveCall{{Method: "Observe", Arguments: args, Result: json.RawMessage("2")}}
	code, err := program.ObserveProcedure(s.RuntimeReadView(), "front-check", map[string]any{"level": 3})
	if err != nil || code != 2 {
		t.Fatalf("ObserveProcedure = %d, %v", code, err)
	}
	if _, err := program.ObserveProcedure(s.RuntimeReadView(), "front-purge", map[string]any{"user": "u"}); err == nil {
		t.Fatal("a data procedure ran as a Diagnose read")
	}
	s.PodList = []corev1.Pod{}
	if _, err := program.ObserveProcedure(s.RuntimeReadView(), "front-check", nil); err == nil {
		t.Fatal("a check without a running Pod succeeded")
	}
}
