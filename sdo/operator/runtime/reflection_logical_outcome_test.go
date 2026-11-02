package runtime

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// physicalGapText is the shape of the reflection SDK gap that blocked every
// later reflection of one social-network memory: agent prose naming playbook
// directories by their physical paths.
const physicalGapText = ".sdo/playbooks/deployment-source-drift and .sdo/playbooks/source-absent-networkpolicy contain relation-bound Diagnose/Mitigate programs."

// One incident's reflection text with physical memory paths is stored through
// the real broker; the next incident's reflection must still run on that
// memory, and the stored outcome must name the playbooks by logical ID.
func TestReflectionTextWithPhysicalPathsNeverBlocksLaterReflection(t *testing.T) {
	ctx := context.Background()
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	var priors [][]*contracts.OutcomeRecord
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(target), Catalog: memory.OpenCatalogReader(target), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			return nil, err
		}
		priors = append(priors, request.GetPriorOutcomes())
		response, err := anypb.New(&contracts.ReflectionResult{
			LearningDecision: "sdk_gap", NoChangeReason: "see " + layout.PlaybooksDir + "/base/README.md",
			SdkGaps: []*contracts.SdkGap{{FaultClass: "accepted-source drift", Primitive: "cause-specific detector playbook references", Reason: "no cause-specific detector references the new programs", ScratchScript: physicalGapText}},
		})
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
	})
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Reflector: worker, ResponderBackend: "fake", ResponderModel: "fake", Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
		return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
	})}
	for _, id := range []string{"first", "second"} {
		workspace, err := native.PrepareIncident(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte(id+" repair\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := native.ProcessClosure(ctx, nativeClosure(workspace)); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		ledger, err := native.readLedger(id)
		if err != nil {
			t.Fatal(err)
		}
		if decision := ledger.GetReflectionDecision(); decision.GetLearningDecision() != "sdk_gap" || decision.GetError() != "" {
			t.Fatalf("%s reflection failed: %v", id, decision)
		}
	}
	if len(priors) != 2 || len(priors[1]) != 1 {
		t.Fatalf("second reflection did not see the first outcome: %v", priors)
	}
	seen, err := wire.Marshal(priors[1][0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(seen, []byte("playbook:deployment-source-drift and playbook:source-absent-networkpolicy")) {
		t.Fatalf("prior outcome lost the logical playbook references: %s", seen)
	}
	stored, err := exec.Command("git", "-C", target, "show", "HEAD:"+layout.OutcomePath("first")).Output()
	if err != nil {
		t.Fatal(err)
	}
	if layout.ContainsPhysicalPath(string(stored)) {
		t.Fatalf("accepted outcome stores physical memory paths: %s", stored)
	}
}

// Memory accepted before the producer was fixed already holds an outcome with
// physical paths. Reading it is the consumer edge: the next reflection runs.
func TestReflectionReadsOlderOutcomeWithPhysicalPaths(t *testing.T) {
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	poisoned := `{"schema_version":1,"incident_id":"earlier","classification":"success","sdk_gaps":[{"fault_class":"drift","primitive":"references","reason":"saved","scratch_script":"` + physicalGapText + `"}],"reflection_rejections":[{"attempt":1,"diagnostics":[{"rule":"text.physical-path","path":".sdo/playbooks/base/README.md","message":"bad"}]}]}`
	path := filepath.Join(target, layout.OutcomePath("earlier"))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(poisoned), 0o644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, target, "add", ".")
	nativeGit(t, target, "commit", "-qm", "older memory")
	base := nativeGit(t, target, "rev-parse", "HEAD")
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(target), Catalog: memory.OpenCatalogReader(target), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: target, BaseCommit: strings.TrimSpace(base)}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success", SourceCheckNotes: []string{"edited " + layout.PlaybooksDir + "/base/scripts/fix.sh"}}
	decision, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, outcome, "repair", "outcome", ReflectionFeedback{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.LearningDecision != "no_change" {
		t.Fatalf("reflection on older memory = %+v", decision)
	}
}
