package rendered

import (
	"errors"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// WorkloadSummary is one workload of a rendered inventory: its plan ID
// (Kind/Namespace/Name) and the container images its pods run.
type WorkloadSummary struct {
	ID     string   `json:"id"`
	Images []string `json:"images"`
}

// WorkloadInventory is what a deployment plan needs from a rendered inventory:
// every pod-bearing object (the pod-template table decides which) as an
// expected workload ID, and the sorted unique images of all of them.
type WorkloadInventory struct {
	Workloads []WorkloadSummary `json:"workloads"`
	Images    []string          `json:"images"`
}

// SummarizeWorkloads derives the plan's workload IDs and images from rendered
// objects, in render order. namespace defaults objects that name none.
func SummarizeWorkloads(objects []*unstructured.Unstructured, namespace string) WorkloadInventory {
	inventory := WorkloadInventory{Workloads: []WorkloadSummary{}, Images: []string{}}
	seen := map[string]bool{}
	for _, object := range objects {
		if !WorkloadKind(object.GetKind()) {
			continue
		}
		podSpec, err := PodSpec(object.Object)
		if err != nil {
			continue // not a usable pod template; the deploy path rejects it
		}
		ns := object.GetNamespace()
		if ns == "" {
			ns = namespace
		}
		workload := WorkloadSummary{ID: object.GetKind() + "/" + ns + "/" + object.GetName(), Images: []string{}}
		for _, field := range ContainerFields {
			containers, _ := podSpec[field].([]any)
			for _, raw := range containers {
				container, _ := raw.(map[string]any)
				if image, _ := container["image"].(string); image != "" {
					workload.Images = append(workload.Images, image)
					if !seen[image] {
						seen[image] = true
						inventory.Images = append(inventory.Images, image)
					}
				}
			}
		}
		inventory.Workloads = append(inventory.Workloads, workload)
	}
	sort.Strings(inventory.Images)
	return inventory
}

// CheckExpectedWorkloads checks that the plan's expected workload IDs
// (Kind/Namespace/Name) are unique, nonempty and exactly the workloads of the
// desired set. Apply and the offline operability report share it.
func CheckExpectedWorkloads(set DesiredSet, expected []string) error {
	wanted := map[string]bool{}
	for _, id := range expected {
		if id == "" || wanted[id] {
			return errors.New("expected workload IDs must be unique and nonempty")
		}
		wanted[id] = true
	}
	rendered := map[string]bool{}
	for _, object := range set.Objects() {
		if object.Workload {
			rendered[object.Ref.Kind+"/"+object.Ref.Namespace+"/"+object.Ref.Name] = true
		}
	}
	for id := range rendered {
		if !wanted[id] {
			return fmt.Errorf("workload %s is missing from deployment plan", id)
		}
	}
	if len(wanted) != len(rendered) {
		return errors.New("expected workload IDs differ from rendered manifests")
	}
	return nil
}

// ImageSites lists every container image of every workload in apply order,
// including ephemeral phases (their pods run those images too): init
// containers first, then regular containers, per workload.
func ImageSites(set DesiredSet) []string {
	var images []string
	for _, phase := range set.Phases {
		for _, object := range phase.Objects {
			if !object.Workload {
				continue
			}
			podSpec, err := PodSpec(object.Object.Object)
			if err != nil {
				continue // Normalize rejected this shape before it entered the set.
			}
			for _, field := range ContainerFields {
				containers, _ := podSpec[field].([]any)
				for _, raw := range containers {
					container, _ := raw.(map[string]any)
					image, _ := container["image"].(string)
					images = append(images, image)
				}
			}
		}
	}
	return images
}
