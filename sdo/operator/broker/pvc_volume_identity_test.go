package broker

import "testing"

// The candidate and healthy corpus are distinct read-only subpaths of one claim.
// OpenEBS/kubelet must receive one Volume identity, with two VolumeMounts.
func TestKubernetesValidatorUsesSingleRepositoryVolume(t *testing.T) {
	client := KubernetesValidationJobClient{RepositoryPVC: "repository"}
	job := client.validationJob("sdo-validator-preflight", "worktrees/candidate", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true)
	claims := map[string]string{}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		claim := volume.PersistentVolumeClaim.ClaimName
		if previous, exists := claims[claim]; exists {
			t.Fatalf("repository claim %s has two Volume identities %s and %s; share the Volume and retain isolated read-only subPath mounts", claim, previous, volume.Name)
		}
		claims[claim] = volume.Name
	}
	if len(job.Spec.Template.Spec.Containers[0].VolumeMounts) != 3 {
		t.Fatal("candidate, scratch and healthy corpus projections must remain")
	}
}
