package runtime

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func stageDispatcher(t *testing.T, withOtherFault bool) (KubernetesJobDispatcher, IncidentRequest, *ResponderLoop) {
	t.Helper()
	loop, client := trustedPortLoop(t, withOtherFault, false, true)
	request := goldenRequest(t)
	request.Namespace, request.RepairPolicy, request.SourceCommit = "app", "recorded-actions", "tree-a"
	request.Findings = append([]sdk.Finding(nil), loop.Request.Findings...)
	for i := range request.Findings {
		if request.Findings[i].Summary == "" {
			request.Findings[i].Severity, request.Findings[i].Summary, request.Findings[i].Evidence = sdk.SeverityWarning, "uncovered fault", "no saved program covers this"
		}
	}
	request.RepositoryWorktree = "/workspace/worktrees/" + request.IncidentID
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t),
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: "/workspace",
		ProgramFastPath: true, ResponseStages: true, PollInterval: time.Millisecond,
		LoopFactory: func(request IncidentRequest) *ResponderLoop { loop.Request = request; return loop }}
	prepared, err := dispatcher.PrepareDispatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher, prepared, loop
}

// No trust record exists, yet the saved program is applied, verified and
// closes the incident before any responder Job is created.
func TestDeterministicStageClosesIncidentWithoutResponderJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dispatcher, request, _ := stageDispatcher(t, false)
		result, err := dispatcher.Dispatch(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if result.ResolutionPath != "deterministic" || result.Status != IncidentCompleted || result.Usage.GetInputTokens() != 0 || result.ResponderSessionID != "" {
			t.Fatalf("result = %+v", result)
		}
		if len(result.PriorStages) != 1 || result.PriorStages[0].GetMitigations()[0].GetResult() != "applied_cleared" {
			t.Fatalf("stage report = %v", result.PriorStages)
		}
		if len(result.PlanApplications) != 1 || len(result.RepairActions) == 0 || len(result.AppliedPlaybooks) != 1 {
			t.Fatalf("evidence: applications=%d repairs=%d playbooks=%d", len(result.PlanApplications), len(result.RepairActions), len(result.AppliedPlaybooks))
		}
		service, err := dispatcher.Client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
		if err != nil || service.Spec.Ports[0].TargetPort.IntVal == 9999 {
			t.Fatalf("service not repaired: %+v %v", service, err)
		}
		jobs, err := dispatcher.Client.BatchV1().Jobs("app").List(context.Background(), metav1.ListOptions{})
		if err != nil || len(jobs.Items) != 0 {
			t.Fatalf("responder Job dispatched: %+v %v", jobs, err)
		}
	})
}

// A finding no saved program covers stays open: the agent starts, is told what
// the stage did, and receives only what remains.
func TestDeterministicStageTellsAgentWhatRan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dispatcher, request, loop := stageDispatcher(t, true)
		ctx, cancel := context.WithCancel(context.Background())
		finished := make(chan error, 1)
		go func() { _, err := dispatcher.Dispatch(ctx, request); finished <- err }()
		name := IncidentJobName(request.IncidentID)
		for i := 0; ; i++ {
			synctest.Wait()
			if _, err := dispatcher.Client.BatchV1().Jobs("app").Get(context.Background(), name, metav1.GetOptions{}); err == nil {
				break
			}
			select {
			case err := <-finished:
				t.Fatalf("dispatch ended before a responder Job: %v", err)
			default:
			}
			if i > 50 {
				t.Fatal("responder Job absent")
			}
			time.Sleep(time.Millisecond)
		}
		taskMap, err := dispatcher.Client.CoreV1().ConfigMaps("app").Get(context.Background(), name+"-request", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		task := new(contracts.AgentTask)
		if err := protojson.Unmarshal([]byte(taskMap.Data[jobRequestDataKey]), task); err != nil {
			t.Fatal(err)
		}
		incident := new(contracts.IncidentRequest)
		if err := task.GetRequest().UnmarshalTo(incident); err != nil {
			t.Fatal(err)
		}
		if len(incident.GetPriorStages()) != 1 || incident.GetPriorStages()[0].GetStage() != "deterministic" {
			t.Fatalf("agent was not told what ran: %v", incident.GetPriorStages())
		}
		mitigation := incident.GetPriorStages()[0].GetMitigations()[0]
		if mitigation.GetPlaybookId() != "service-targetport-mismatch" || mitigation.GetResult() != "applied_cleared" {
			t.Fatalf("mitigation = %v", mitigation)
		}
		if len(incident.GetFindings()) != 1 || incident.GetFindings()[0].GetDetectorId() != "other" {
			t.Fatalf("agent findings = %v", incident.GetFindings())
		}
		if loop.stageWrites != 1 {
			t.Fatalf("stage writes = %d", loop.stageWrites)
		}
		cancel()
		<-finished
	})
}
