package worker_registry

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func launchedEnvironment(t *testing.T, message proto.Message, configured map[string]string) map[string]string {
	t.Helper()
	request, err := anypb.New(message)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	executor := KubernetesExecutor{Client: client, Namespace: "sdo", RepositoryPVC: "repo", Environment: configured}
	task := &contracts.AgentTask{TaskId: "task-1", Request: request, Workspace: &contracts.ProposalWorkspace{CheckoutPath: "/work", BaseCommit: "abc"}}
	if err := executor.ensureJob(context.Background(), task, &contracts.KubernetesJobLaunch{Image: "worker"}, "job-1", requestSource{ConfigMap: "req"}, "job-1-result", ""); err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs("sdo").Get(context.Background(), "job-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return jobEnvironment(job)
}

func jobEnvironment(job *batchv1.Job) map[string]string {
	environment := map[string]string{}
	for _, variable := range job.Spec.Template.Spec.Containers[0].Env {
		environment[variable.Name] = variable.Value
	}
	return environment
}

// Every agent role that edits a checkout gets its patch owner from the launch, so no agent passes --owner.
func TestLaunchedAgentsCarryTheirActor(t *testing.T) {
	cases := []struct {
		name    string
		message proto.Message
		actor   string
	}{
		{"incident responder", &contracts.IncidentRequest{}, "responder"},
		{"deployer", &contracts.SourceDeploymentRequest{}, "deployer"},
		{"reflection", &contracts.ReflectionRequest{}, "responder"},
		{"health judge", &contracts.HealthJudgeRequest{}, "health_judge"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := launchedEnvironment(t, tc.message, nil)["SDO_ACTOR"]; got != tc.actor {
				t.Fatalf("SDO_ACTOR = %q, want %q", got, tc.actor)
			}
		})
	}
}

// A controller-wide environment cannot override the actor of a launched role.
func TestConfiguredEnvironmentCannotOverrideActor(t *testing.T) {
	environment := launchedEnvironment(t, &contracts.SourceDeploymentRequest{}, map[string]string{"SDO_ACTOR": "human"})
	if environment["SDO_ACTOR"] != "deployer" {
		t.Fatalf("SDO_ACTOR = %q, want deployer", environment["SDO_ACTOR"])
	}
}
