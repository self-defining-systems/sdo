package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"

	"sdo.dev/operator/memory/layout"

	"gopkg.in/yaml.v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/internal/memoryio"
)

// SourceFacts are a bounded, logical projection of an accepted source commit.
// Only this storage edge knows which tracked paths contain operational memory.
type SourceFacts struct {
	Resources []*contracts.SourceResource
}

var fullCommitID = regexp.MustCompile(`^[0-9a-f]{40}$`)

func SourceInventory(ctx context.Context, checkout, acceptedCommit string) (SourceFacts, error) {
	if !fullCommitID.MatchString(acceptedCommit) {
		return SourceFacts{}, fmt.Errorf("invalid accepted source commit")
	}
	listing, err := memoryio.Git(ctx, checkout, false, nil, "ls-tree", "-r", "-z", "--full-tree", acceptedCommit)
	if err != nil {
		return SourceFacts{}, fmt.Errorf("read accepted source tree %s: %w", acceptedCommit, err)
	}
	if len(listing) > 4<<20 {
		return SourceFacts{}, fmt.Errorf("accepted source tree listing exceeds limit")
	}
	resources := []*contracts.SourceResource{}
	var total int64
	for _, entry := range bytes.Split(listing, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		meta, pathBytes, found := bytes.Cut(entry, []byte{'\t'})
		if !found {
			return SourceFacts{}, fmt.Errorf("invalid accepted source tree entry")
		}
		parts := strings.Fields(string(meta))
		if len(parts) != 3 {
			return SourceFacts{}, fmt.Errorf("invalid accepted source tree metadata")
		}
		path := string(pathBytes)
		if path == layout.Dir || layout.IsMemoryPath(path) {
			continue
		}
		if parts[1] != "blob" || parts[0] == "120000" {
			continue
		}
		content, err := memoryio.Git(ctx, checkout, false, nil, "cat-file", "blob", parts[2])
		if err != nil {
			return SourceFacts{}, fmt.Errorf("read accepted source blob %s: %w", path, err)
		}
		total += int64(len(content))
		if total > 64<<20 {
			return SourceFacts{}, fmt.Errorf("accepted source inventory exceeds 64 MiB")
		}
		if !strings.HasSuffix(strings.ToLower(path), ".yaml") && !strings.HasSuffix(strings.ToLower(path), ".yml") {
			continue
		}
		decoder := yaml.NewDecoder(bytes.NewReader(content))
		for {
			var doc map[string]any
			err := decoder.Decode(&doc)
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			} // Non-Kubernetes YAML is source, not topology.
			kind, _ := doc["kind"].(string)
			metadata := mapValue(doc["metadata"])
			name, _ := metadata["name"].(string)
			if kind == "" || name == "" {
				continue
			}
			namespace, _ := metadata["namespace"].(string)
			if namespace == "" {
				namespace = "default"
			}
			resources = append(resources, &contracts.SourceResource{Kind: kind, Name: name, Namespace: namespace, Source: path, Dependencies: sourceDependencies(doc)})
		}
	}
	slices.SortFunc(resources, func(a, b *contracts.SourceResource) int {
		return strings.Compare(a.Namespace+"\x00"+a.Kind+"\x00"+a.Name+"\x00"+a.Source, b.Namespace+"\x00"+b.Kind+"\x00"+b.Name+"\x00"+b.Source)
	})
	return SourceFacts{Resources: resources}, nil
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
	template := mapValue(spec["template"])
	for key, value := range mapValue(mapValue(template["metadata"])["labels"]) {
		if v, ok := value.(string); ok {
			set["pod-label:"+key+"="+v] = struct{}{}
		}
	}
	pod := mapValue(template["spec"])
	if doc["kind"] == "Pod" {
		pod = spec
	}
	if doc["kind"] == "CronJob" {
		pod = mapValue(mapValue(mapValue(mapValue(spec["jobTemplate"])["spec"])["template"])["spec"])
	}
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
	for _, group := range []string{"initContainers", "containers"} {
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

// ArchitectureInventory renders only manifest-declared facts, in stable inventory order.
// Readers needing application semantics inspect the referenced source directly.
func ArchitectureInventory(facts SourceFacts) string {
	var text strings.Builder
	text.WriteString("# Manifest inventory\n\nApplication semantics must be read from source.\n")
	for _, resource := range facts.Resources {
		fmt.Fprintf(&text, "\n- %s/%s/%s (source: %s)\n", resource.Kind, resource.Namespace, resource.Name, resource.Source)
		for _, dependency := range resource.Dependencies {
			fmt.Fprintf(&text, "  - %s\n", dependency)
		}
	}
	return text.String()
}
