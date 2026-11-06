package rendered

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// The shape of a desired set is the topology the health judge's policy is
// written against. It is the only thing that decides whether the judge must
// run again; source bytes (a new image build, an env value) never do.
//
// Included, for every non-ephemeral object in the set (foreign ones too):
//   - identity: apiVersion, kind, namespace, name (an added, removed or
//     renamed object of any kind, Secrets and ConfigMaps included);
//   - workloads (Deployment, StatefulSet, DaemonSet, Job): the name of every
//     container and init container, each container's ports, and its
//     readiness, liveness and startup probes;
//   - Services: type, selector and ports.
//
// Excluded: images and build inputs, command, args, env, resources, replicas,
// labels, annotations, volumes, Secret and ConfigMap data, and every spec
// field of other kinds.
func shapeDigest(objects []DesiredObject) (string, error) {
	items := make([]map[string]any, 0, len(objects))
	for _, object := range objects {
		item := map[string]any{"ref": shapeRef(object)}
		doc := object.Object.Object
		switch {
		case object.Workload:
			item["containers"] = workloadShape(doc)
		case object.Ref.Kind == "Service":
			item["service"] = serviceShape(doc)
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i]["ref"].(string) < items[j]["ref"].(string) })
	encoded, err := json.Marshal(items)
	if err != nil {
		return "", fmt.Errorf("digest desired shape: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// shapeRef is the object's identity in the shape. A Job's generated name
// suffix is not part of it (StableWorkloadID): a release that changes only the
// inputs the suffix hashes renames the Job without changing the topology.
func shapeRef(object DesiredObject) string {
	if object.Workload && object.Ref.Kind == "Job" {
		ref := object.Ref
		ref.Name = StableWorkloadName(ref.Kind, ref.Name)
		return refKey(ref)
	}
	return refKey(object.Ref)
}

func workloadShape(doc map[string]any) map[string][]map[string]any {
	podSpec, err := PodSpec(doc)
	if err != nil {
		return nil // Normalize already validated; unreachable for a Workload.
	}
	out := map[string][]map[string]any{}
	for _, field := range ContainerFields {
		list, _ := podSpec[field].([]any)
		for _, raw := range list {
			container, _ := raw.(map[string]any)
			entry := map[string]any{"name": container["name"]}
			for _, key := range []string{"ports", "readinessProbe", "livenessProbe", "startupProbe"} {
				if value, ok := container[key]; ok {
					entry[key] = value
				}
			}
			out[field] = append(out[field], entry)
		}
	}
	return out
}

func serviceShape(doc map[string]any) map[string]any {
	spec, _ := doc["spec"].(map[string]any)
	out := map[string]any{}
	for _, key := range []string{"type", "selector", "ports"} {
		if value, ok := spec[key]; ok {
			out[key] = value
		}
	}
	return out
}
