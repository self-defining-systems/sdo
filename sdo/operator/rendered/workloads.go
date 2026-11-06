package rendered

import (
	"errors"
	"fmt"
)

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
