package worker_registry

import (
	"context"
	"path"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// A deployer task whose executor serves build tools (SDO_TOOLS_DIR) gets the
// tool directory of its evidence session in the pod: the directory read-only,
// requests and responses writable, at the path the CLI reads from the env.
func TestDeployerTaskWithBuildToolsMountsTheEvidenceSessionToolDirectory(t *testing.T) {
	request, err := anypb.New(&contracts.SourceDeploymentRequest{})
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	session := path.Join(TaskEvidenceRoot, TaskEvidenceKey("op-source-deployer-1"))
	executor := KubernetesExecutor{Client: client, Namespace: "sdo", RepositoryPVC: "repo", SessionPVCSubPath: session, Environment: map[string]string{"SDO_TOOLS_DIR": "/sdo/session/tools"}}
	task := &contracts.AgentTask{TaskId: "op-source-deployer-1", Request: request, Workspace: &contracts.ProposalWorkspace{CheckoutPath: "/work", BaseCommit: "abc"}}
	if err := executor.ensureJob(context.Background(), task, &contracts.KubernetesJobLaunch{Image: "worker"}, "job-1", requestSource{ConfigMap: "req"}, "job-1-result", ""); err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs("sdo").Get(context.Background(), "job-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := jobEnvironment(job)["SDO_TOOLS_DIR"]; got != "/sdo/session/tools" {
		t.Fatalf("SDO_TOOLS_DIR = %q", got)
	}
	want := map[string]struct {
		subPath  string
		readOnly bool
	}{
		"/sdo/session/tools":           {session + "/tools", true},
		"/sdo/session/tools/requests":  {session + "/tools/requests", false},
		"/sdo/session/tools/responses": {session + "/tools/responses", false},
	}
	for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if w, ok := want[mount.MountPath]; ok {
			if mount.SubPath != w.subPath || mount.ReadOnly != w.readOnly {
				t.Errorf("%s: subPath %q readOnly %v, want %q %v", mount.MountPath, mount.SubPath, mount.ReadOnly, w.subPath, w.readOnly)
			}
			delete(want, mount.MountPath)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing mounts %v", want)
	}
}
