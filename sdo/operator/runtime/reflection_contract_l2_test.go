package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

// TestT1L2ShadowReplay is opt-in because archived experiment data must never be
// committed. It only reads receipts, protected corpus files and accepted Git
// objects. Archived program code is built and run in t.TempDir(), without model
// or cluster access. No session preparation helper may touch the archive.
func TestT1L2ShadowReplay(t *testing.T) {
	run := os.Getenv("SDO_T1_L2_RUN")
	if run == "" {
		t.Skip("set SDO_T1_L2_RUN to LM1's read-only archived run")
	}
	workspace := t1OneGlob(t, filepath.Join(run, "stage_8_*/application_workspace"))
	sessions := t1OneGlob(t, filepath.Join(run, "stage_7_*/runs/*/worker_0/results/sdo_codex/*/run_1/sdo_runtime/sessions"))
	for _, index := range []int{1, 3, 4, 5, 6} {
		t.Run(fmt.Sprintf("i%02d", index+1), func(t *testing.T) {
			receipt := t1OneGlob(t, filepath.Join(run, fmt.Sprintf("stage_%d_*/runs/*/worker_0/results/sdo_codex/*/run_1/sdo_production_receipt_strict.json", index)))
			raw, err := os.ReadFile(receipt)
			if err != nil {
				t.Fatal(err)
			}
			var document struct {
				Ledger struct {
					Outcome json.RawMessage `json:"outcome_document"`
				} `json:"native_ledger"`
			}
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatal(err)
			}
			outcome := &contracts.OutcomeRecord{}
			// Historical diagnosis-verification fields no longer exist. Dropping
			// them is explicit; final-equivalence proof is never manufactured.
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document.Ledger.Outcome, outcome); err != nil {
				t.Fatal(err)
			}
			digest := fmt.Sprintf("%x", sha256.Sum256([]byte(outcome.IncidentId)))[:32]
			capture := filepath.Join(sessions, digest, "corpus")
			fixed := t1ArchivedVerdicts(t, workspace, capture, outcome)
			work := classifyCauseWork(outcome, fixed)
			gaps := appendEvidenceGaps(work, capture)
			unassigned := unassignedReflectionQuestions(outcome, fixed)
			missingFinal := 0
			for _, app := range outcome.PlanApplications {
				if app.FinalEquivalenceVerified == nil {
					missingFinal++
				}
			}
			t.Logf("receipt=%s capture=%s source=%s", receipt, capture, outcome.SourceCommit)
			keys := make([]string, 0, len(fixed))
			for key := range fixed {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				t.Logf("fixed %s = %s", key, fixed[key])
			}
			t.Logf("work=%v gaps=%d missing_final_proofs=%d trust=%+v", t1Items(work), gaps, missingFinal, ProgramTrustStats([]*contracts.OutcomeRecord{outcome}))
			for _, item := range unassigned {
				t.Logf("unassigned=%s/%s program=%s bindings=%v resources=%v", item.Disposition, item.Reason, item.ProgramId, item.Bindings, item.Resources)
			}
			// i02 is a question; no installed readiness program established exact
			// coverage. i05 is the known timing gap (not necessarily an absent
			// primitive). Every recurring case still needs affirmative final proof.
			for _, cause := range work {
				for _, item := range cause.Items {
					if item.Disposition == "covered" && (index == 1 || index == 4 || missingFinal > 0) {
						t.Errorf("MISMATCH: covered without current exact proof: %v", item)
					}
				}
			}
			if index == 4 && gaps == 0 {
				t.Errorf("MISMATCH: i05 known capture-timing gap is invisible to Go's presence-only gap flag")
			}
		})
	}
}

func t1OneGlob(t *testing.T, pattern string) string {
	t.Helper()
	paths, err := filepath.Glob(pattern)
	if err != nil || len(paths) != 1 {
		t.Fatalf("expected one %s: %v %v", pattern, paths, err)
	}
	return paths[0]
}

type t1ReplayInstance struct {
	ID       string           `json:"id"`
	Bindings program.Bindings `json:"bindings"`
	Verdict  program.Result   `json:"verdict"`
}

func t1ArchivedVerdicts(t *testing.T, workspace, capture string, outcome *contracts.OutcomeRecord) map[string]program.Result {
	t.Helper()
	instances := []t1ReplayInstance{}
	ids := map[string]bool{}
	for _, run := range outcome.ProgramRuns {
		if run.GetVerdict().GetResult() != contracts.Verdict_RESULT_MATCH {
			continue
		}
		b := program.Bindings{}
		for _, binding := range run.Bindings {
			b[binding.Name] = objectRefState(binding.Object)
		}
		instances = append(instances, t1ReplayInstance{ID: run.ProgramId, Bindings: b})
		ids[run.ProgramId] = true
	}
	results := map[string]program.Result{}
	if len(instances) == 0 {
		return results
	}
	temp := t.TempDir()
	sdk, err := filepath.Abs("../sdk")
	if err != nil {
		t.Fatal(err)
	}
	t1Write(t, temp, "go.mod", []byte(fmt.Sprintf("module t1replay\n\ngo 1.26.0\n\nrequire sdo.dev/operator/sdk v0.0.0\nreplace sdo.dev/operator/sdk => %s\n", sdk)))
	ordered := []string{}
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	imports, registrations := "", ""
	for index, id := range ordered {
		if id != "service-targetport-mismatch" && id != "image-pull-failure" {
			t.Fatalf("unexpected archived program ID %q", id)
		}
		dir := ".sdo/playbooks/" + id
		listing := nativeGit(t, workspace, "ls-tree", "-r", "--name-only", outcome.SourceCommit, "--", dir)
		for _, path := range strings.Split(listing, "\n") {
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				continue
			}
			raw, err := exec.Command("git", "-C", workspace, "show", outcome.SourceCommit+":"+path).Output()
			if err != nil {
				t.Fatal(err)
			}
			t1Write(t, temp, filepath.Join("programs", id, filepath.Base(path)), raw)
		}
		imports += fmt.Sprintf("p%d %q\n", index, "t1replay/programs/"+id)
		registrations += fmt.Sprintf("%q:p%d.Program{},\n", id, index)
	}
	// Exact accepted program code, fresh replay per independent diagnosis. The
	// classifier's order-dependence is tested separately with hermetic fixtures.
	source := `package main
import("context";"encoding/json";"os";"time";"path/filepath";"sdo.dev/operator/sdk/program";"sdo.dev/operator/sdk/sdktest";` + imports + `)
type instance struct {ID string;Bindings program.Bindings;Verdict program.Result}
func main(){
 var inputs []instance; must(json.NewDecoder(os.Stdin).Decode(&inputs))
 var snapshot sdktest.Snapshot;raw,e:=os.ReadFile(filepath.Join(os.Args[1],"fixed.json"));must(e);must(json.Unmarshal(raw,&snapshot))
 var calls []sdktest.PrimitiveCall;raw,e=os.ReadFile(filepath.Join(os.Args[1],"open@detect-recordings.json"));must(e);must(json.Unmarshal(raw,&calls))
 registry:=map[string]program.Diagnoser{` + registrations + `}
 corpus:=sdktest.CorpusCase{Recordings:calls}
 for i:=range inputs{v:=program.RunDiagnose(context.Background(),registry[inputs[i].ID],&sdktest.ReplayReadView{Snapshot:snapshot,Calls:corpus.PhaseRecordings("fixed")},inputs[i].Bindings,time.Second);inputs[i].Verdict=v.Result}
 must(json.NewEncoder(os.Stdout).Encode(inputs))
}
func must(e error){if e!=nil{panic(e)}}`
	t1Write(t, temp, "main.go", []byte(source))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "run", "-mod=mod", ".", capture)
	cmd.Dir = temp
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off")
	cmd.Stdin = strings.NewReader(string(t1JSON(t, instances)))
	raw, err := cmd.Output()
	if err != nil {
		if failed, ok := err.(*exec.ExitError); ok {
			t.Fatalf("archived program replay: %v: %s", err, failed.Stderr)
		}
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &instances); err != nil {
		t.Fatal(err)
	}
	for _, instance := range instances {
		results[matchedViewKey(instance.ID, instance.Bindings)] = instance.Verdict
	}
	return results
}
