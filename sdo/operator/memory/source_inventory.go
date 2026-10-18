package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// SourceFacts are a bounded, logical projection of an accepted source commit.
// Only this storage edge knows which tracked paths contain operational memory.
type SourceFacts struct {
	Fingerprint string
	Resources   []*contracts.SourceResource
}

var fullCommitID = regexp.MustCompile(`^[0-9a-f]{40}$`)

func SourceInventory(ctx context.Context, checkout, acceptedCommit string) (SourceFacts, error) {
	if !fullCommitID.MatchString(acceptedCommit) {
		return SourceFacts{}, fmt.Errorf("invalid accepted source commit")
	}
	cmd := exec.CommandContext(ctx, "git", "-C", checkout, "ls-tree", "-r", "-z", "--full-tree", acceptedCommit)
	listing, err := cmd.Output()
	if err != nil {
		return SourceFacts{}, fmt.Errorf("read accepted source tree %s: %w", acceptedCommit, err)
	}
	if len(listing) > 4<<20 {
		return SourceFacts{}, fmt.Errorf("accepted source tree listing exceeds limit")
	}
	hash := sha256.New()
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
		if path == ".sdo" || strings.HasPrefix(path, ".sdo/") {
			continue
		}
		if parts[1] != "blob" || parts[0] == "120000" {
			continue
		}
		blob := exec.CommandContext(ctx, "git", "-C", checkout, "cat-file", "blob", parts[2])
		content, err := blob.Output()
		if err != nil {
			return SourceFacts{}, fmt.Errorf("read accepted source blob %s: %w", path, err)
		}
		total += int64(len(content))
		if total > 64<<20 {
			return SourceFacts{}, fmt.Errorf("accepted source inventory exceeds 64 MiB")
		}
		_, _ = hash.Write(pathBytes)
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(content)
		_, _ = hash.Write([]byte{0})
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
	return SourceFacts{Fingerprint: hex.EncodeToString(hash.Sum(nil)), Resources: resources}, nil
}

func mapValue(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func sourceDependencies(doc map[string]any) []string {
	set := map[string]struct{}{}
	spec := mapValue(doc["spec"])
	for key, value := range mapValue(spec["selector"]) {
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
