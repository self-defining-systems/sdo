package memory

import (
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/mapseed"
	"sdo.dev/operator/rendered"
)

// SourceFacts are a bounded, logical projection of the accepted source's
// rendered desired state.
type SourceFacts struct {
	Resources []*contracts.SourceResource
	// Seed is the application map Go proposes from the same objects, for an
	// architecture that has none yet.
	Seed mapseed.Seed
}

// SourceFactsOf projects rendered desired-state objects (the renderer
// catalog's output for the accepted source, Secrets excluded by the caller)
// into logical facts. renderer names where the objects come from: a rendered
// object has no authored file, so the inventory's source column carries the
// renderer ID. Memory never parses manifest YAML itself; whatever the source
// is (raw files, a Kustomize overlay, a Helm chart), only the render package
// turns it into objects.
func SourceFactsOf(objects []*unstructured.Unstructured, renderer string) SourceFacts {
	resources := make([]*contracts.SourceResource, 0, len(objects))
	for _, object := range objects {
		kind, name := object.GetKind(), object.GetName()
		if kind == "" || name == "" {
			continue
		}
		namespace := object.GetNamespace()
		if namespace == "" {
			namespace = "default"
		}
		resources = append(resources, &contracts.SourceResource{Kind: kind, Name: name, Namespace: namespace, Source: renderer, Dependencies: sourceDependencies(object.Object)})
	}
	slices.SortFunc(resources, func(a, b *contracts.SourceResource) int {
		return strings.Compare(a.Namespace+"\x00"+a.Kind+"\x00"+a.Name+"\x00"+a.Source, b.Namespace+"\x00"+b.Kind+"\x00"+b.Name+"\x00"+b.Source)
	})
	return SourceFacts{Resources: resources, Seed: mapseed.FromObjects(objects)}
}

func mapValue(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func sourceDependencies(doc map[string]any) []string {
	set := map[string]struct{}{}
	spec := mapValue(doc["spec"])
	selector := mapValue(spec["selector"])
	if labels := mapValue(selector["matchLabels"]); labels != nil {
		selector = labels
	}
	for key, value := range selector {
		if v, ok := value.(string); ok {
			set["selector:"+key+"="+v] = struct{}{}
		}
	}
	templateMetadata, _ := rendered.PodTemplateMetadata(doc, false)
	for key, value := range mapValue(templateMetadata["labels"]) {
		if v, ok := value.(string); ok {
			set["pod-label:"+key+"="+v] = struct{}{}
		}
	}
	pod, _ := rendered.PodSpec(doc)
	if account, ok := pod["serviceAccountName"].(string); ok && account != "" {
		set["ServiceAccount/"+account] = struct{}{}
	}
	if volumes, ok := pod["volumes"].([]any); ok {
		for _, value := range volumes {
			if name, ok := mapValue(mapValue(value)["configMap"])["name"].(string); ok {
				set["ConfigMap/"+name] = struct{}{}
			}
		}
	}
	for _, group := range rendered.ContainerFields {
		if containers, ok := pod[group].([]any); ok {
			for _, value := range containers {
				container := mapValue(value)
				if image, ok := container["image"].(string); ok && image != "" {
					set["image:"+image] = struct{}{}
				}
				if env, ok := container["envFrom"].([]any); ok {
					for _, source := range env {
						if name, ok := mapValue(mapValue(source)["configMapRef"])["name"].(string); ok {
							set["ConfigMap/"+name] = struct{}{}
						}
					}
				}
			}
		}
	}
	result := make([]string, 0, len(set))
	for dependency := range set {
		result = append(result, dependency)
	}
	slices.Sort(result)
	return result
}

// ArchitectureInventory renders the manifest-declared facts in stable
// inventory order and, when the objects declare workloads, the seeded
// application map: its summary and entry candidates, then the map region.
// WriteArchitecture keeps an accepted region in place of the seed.
func ArchitectureInventory(facts SourceFacts) string {
	var text strings.Builder
	text.WriteString("# Manifest inventory\n\nManifest facts only; read application semantics from source where it is shipped.\n")
	for _, resource := range facts.Resources {
		fmt.Fprintf(&text, "\n- %s/%s/%s (source: %s)\n", resource.Kind, resource.Namespace, resource.Name, resource.Source)
		for _, dependency := range resource.Dependencies {
			fmt.Fprintf(&text, "  - %s\n", dependency)
		}
	}
	if len(facts.Seed.Map.Components) > 0 {
		text.WriteString("\n" + facts.Seed.Summary() + "\n" + facts.Seed.Region())
	}
	return text.String()
}
