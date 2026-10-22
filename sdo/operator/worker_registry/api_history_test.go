package worker_registry

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestResponderHistorySidecarHasOnlyIncidentStoreAndPodIdentity(t *testing.T) {
	client := fake.NewSimpleClientset()
	payload, _ := anypb.New(&contracts.IncidentRequest{IncidentId: "incident"})
	task := &contracts.AgentTask{TaskId: "incident", Request: payload, Workspace: &contracts.ProposalWorkspace{CheckoutPath: "/work/proposal"}}
	executor := KubernetesExecutor{Client: client, Namespace: "control", ServiceAccount: "existing-rbac", RepositoryPVC: "repo", SessionPVCSubPath: "tenant/.sdo-runtime/sessions/hash", APIProxyImage: "operator@sha256:synthetic", CredentialsSecret: "provider-secret", Environment: map[string]string{"SDO_API_PROXY_URL": "forged", "SDO_API_HISTORY_DIR": "forged"}}
	if err := executor.ensureJob(context.Background(), task, &contracts.KubernetesJobLaunch{Image: "custom-agent", Command: []string{"/agent"}}, "job", requestSource{ConfigMap: "request"}, "result", "proposal"); err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs("control").Get(context.Background(), "job", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod := job.Spec.Template.Spec
	if pod.ServiceAccountName != "existing-rbac" || len(pod.InitContainers) != 1 || len(pod.Containers) != 1 {
		t.Fatalf("pod boundary: %+v", pod)
	}
	sidecar := pod.InitContainers[0]
	if sidecar.RestartPolicy == nil || *sidecar.RestartPolicy != corev1.ContainerRestartPolicyAlways || sidecar.StartupProbe == nil || sidecar.Image != "operator@sha256:synthetic" {
		t.Fatalf("native sidecar: %+v", sidecar)
	}
	if len(sidecar.EnvFrom) != 0 || len(sidecar.VolumeMounts) != 1 || sidecar.VolumeMounts[0].SubPath != "tenant/.sdo-runtime/sessions/hash/api-history" || sidecar.VolumeMounts[0].ReadOnly {
		t.Fatalf("sidecar has unrelated access: %+v", sidecar)
	}
	agent := pod.Containers[0]
	found := false
	for _, mount := range agent.VolumeMounts {
		if mount.MountPath == "/sdo/session/api-history" {
			found = true
			if !mount.ReadOnly {
				t.Fatal("agent can alter history")
			}
		}
	}
	if !found {
		t.Fatal("missing read-only history")
	}
	for _, name := range []string{"SDO_API_PROXY_URL", "SDO_API_HISTORY_DIR"} {
		count := 0
		for _, env := range agent.Env {
			if env.Name == name {
				count++
				if env.Value == "forged" {
					t.Fatal("untrusted history settings")
				}
			}
		}
		if count != 1 {
			t.Fatalf("%s entries %d", name, count)
		}
	}
}
