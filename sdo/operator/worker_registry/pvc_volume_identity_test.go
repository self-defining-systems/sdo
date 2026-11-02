package worker_registry

import (
	"context"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestWorkerPVCProjectionsShareOneVolumeAcrossLaunchModes(t *testing.T) {
	for _, session := range []bool{false, true} {
		for _, requestOnPVC := range []bool{false, true} {
			for _, readOnly := range []bool{false, true} {
				t.Run(fmt.Sprintf("session=%t/requestPVC=%t/readOnly=%t", session, requestOnPVC, readOnly), func(t *testing.T) {
					client := fake.NewSimpleClientset()
					executor := KubernetesExecutor{Client: client, Namespace: "control", RepositoryPVC: "repository", RepositoryPVCSubPath: "tenant", RepositoryReadOnly: readOnly}
					if session {
						executor.SessionPVCSubPath = "tenant/session"
					}
					request := requestSource{ConfigMap: "request"}
					if requestOnPVC {
						request = requestSource{PVCSubPath: "tenant/request"}
					}
					task := reflectionTestTask(t)
					launch := &contracts.KubernetesJobLaunch{Image: "agent:test", Command: []string{"agent"}}
					if err := executor.ensureJob(context.Background(), task, launch, "test-job", request, "result", "tenant/proposals/reflection-1"); err != nil {
						t.Fatal(err)
					}
					job, err := client.BatchV1().Jobs("control").Get(context.Background(), "test-job", metav1.GetOptions{})
					if err != nil {
						t.Fatal(err)
					}
					claims := map[string]string{}
					for _, volume := range job.Spec.Template.Spec.Volumes {
						if volume.PersistentVolumeClaim == nil {
							continue
						}
						claim := volume.PersistentVolumeClaim.ClaimName
						if previous, duplicate := claims[claim]; duplicate {
							t.Fatalf("claim %s has volumes %s and %s", claim, previous, volume.Name)
						}
						claims[claim] = volume.Name
					}
					if claims["repository"] != "repository" {
						t.Fatalf("canonical volume missing: %v", claims)
					}
					for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
						switch mount.MountPath {
						case task.Workspace.CheckoutPath:
							if mount.Name != "repository" || mount.SubPath != "tenant/proposals/reflection-1" || mount.ReadOnly != readOnly {
								t.Fatalf("checkout projection changed: %+v", mount)
							}
						case "/sdo/session":
							if mount.Name != "repository" || mount.SubPath != "tenant/session" || mount.ReadOnly {
								t.Fatalf("session projection changed: %+v", mount)
							}
						case "/sdo/request":
							if requestOnPVC && (mount.Name != "repository" || mount.SubPath != "tenant/request" || !mount.ReadOnly) {
								t.Fatalf("request projection changed: %+v", mount)
							}
						}
					}
				})
			}
		}
	}
}
