package deployment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// CommandBackend performs raw manifest deployment with the caller's configured
// Docker registry and Kubernetes credentials. It never accepts readiness from
// an agent: it reads the live workload after applying stamped manifests.
type CommandBackend struct {
	DockerBinary  string
	KubectlBinary string
	KubeContext   string
	ObserveLimit  time.Duration
}

func (b CommandBackend) docker() string {
	if b.DockerBinary != "" {
		return b.DockerBinary
	}
	return "docker"
}
func (b CommandBackend) kubectl() string {
	if b.KubectlBinary != "" {
		return b.KubectlBinary
	}
	return "kubectl"
}

func (b CommandBackend) BuildImage(ctx context.Context, _ string, build *contracts.ImageBuild) (string, error) {
	if build == nil || build.GetImage() == "" {
		return "", errors.New("image build is required")
	}
	metadataDir, err := os.MkdirTemp("", "sdo-image-build-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(metadataDir)
	metadata := filepath.Join(metadataDir, "metadata.json")
	command := exec.CommandContext(ctx, b.docker(), "buildx", "build", "--push", "--metadata-file", metadata, "--file", build.DockerfilePath, "--tag", build.Image, build.ContextPath)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("push image %s: %w: %s", build.Image, err, strings.TrimSpace(string(output)))
	}
	data, err := os.ReadFile(metadata)
	if err != nil {
		return "", fmt.Errorf("read image build metadata: %w", err)
	}
	var details struct {
		Digest string `json:"containerimage.digest"`
	}
	if err := json.Unmarshal(data, &details); err != nil {
		return "", fmt.Errorf("image build metadata: %w", err)
	}
	if !strings.HasPrefix(details.Digest, "sha256:") || len(details.Digest) != 71 {
		return "", errors.New("image build did not return a sha256 digest")
	}
	return imageRepository(build.Image) + "@" + details.Digest, nil
}

func (b CommandBackend) Apply(ctx context.Context, documents []byte) error {
	if len(documents) == 0 {
		return errors.New("deployment manifests are empty")
	}
	args := b.kubectlArgs("apply", "--server-side", "--field-manager=sdo", "--filename=-")
	command := exec.CommandContext(ctx, b.kubectl(), args...)
	command.Stdin = bytes.NewReader(documents)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("apply Kubernetes manifests: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (b CommandBackend) Observe(ctx context.Context, resourceID string) (WorkloadObservation, error) {
	parts := strings.Split(resourceID, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return WorkloadObservation{}, fmt.Errorf("invalid workload ID %q", resourceID)
	}
	limit := b.ObserveLimit
	if limit <= 0 {
		limit = 2 * time.Minute
	}
	bounded, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	var lastErr error
	for {
		args := b.kubectlArgs("get", parts[0], parts[2], "--namespace", parts[1], "--output=json")
		output, err := exec.CommandContext(bounded, b.kubectl(), args...).CombinedOutput()
		if err != nil {
			lastErr = fmt.Errorf("get workload %s: %w: %s", resourceID, err, strings.TrimSpace(string(output)))
		} else {
			observed, parseErr := parseObservedWorkload(resourceID, output)
			if parseErr != nil {
				return WorkloadObservation{}, parseErr
			}
			if observed.Ready {
				return observed, nil
			}
			lastErr = fmt.Errorf("workload %s has not completed its rollout", resourceID)
		}
		select {
		case <-bounded.Done():
			return WorkloadObservation{}, fmt.Errorf("observe workload %s: %w: %v", resourceID, bounded.Err(), lastErr)
		case <-time.After(2 * time.Second):
		}
	}
}

func (b CommandBackend) kubectlArgs(args ...string) []string {
	if b.KubeContext == "" {
		return args
	}
	return append([]string{"--context", b.KubeContext}, args...)
}

func parseObservedWorkload(resourceID string, data []byte) (WorkloadObservation, error) {
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		return WorkloadObservation{}, fmt.Errorf("decode observed workload: %w", err)
	}
	parts := strings.Split(resourceID, "/")
	if len(parts) != 3 {
		return WorkloadObservation{}, errors.New("invalid resource identity")
	}
	metadata := objectMap(object, "metadata")
	if objectString(object, "kind") != parts[0] || objectString(metadata, "namespace") != parts[1] || objectString(metadata, "name") != parts[2] || objectString(metadata, "uid") == "" {
		return WorkloadObservation{}, errors.New("observed workload identity differs from deployment plan")
	}
	spec := objectMap(object, "spec")
	template := objectMap(spec, "template")
	tree := objectString(objectMap(metadata, "annotations"), "sdo.dev/source-tree")
	if tree == "" || tree != objectString(objectMap(objectMap(template, "metadata"), "annotations"), "sdo.dev/source-tree") {
		return WorkloadObservation{}, errors.New("observed workload lacks matching source revision")
	}
	images := []string{}
	for _, field := range []string{"initContainers", "containers"} {
		for _, raw := range objectSlice(objectMap(template, "spec"), field) {
			container, ok := raw.(map[string]any)
			if !ok {
				return WorkloadObservation{}, errors.New("observed workload has malformed container")
			}
			image := objectString(container, "image")
			if !digestReference.MatchString(image) {
				return WorkloadObservation{}, fmt.Errorf("observed workload image is mutable: %q", image)
			}
			images = append(images, image)
		}
	}
	if len(images) == 0 {
		return WorkloadObservation{}, errors.New("observed workload has no images")
	}
	status := objectMap(object, "status")
	generation := objectInt(metadata, "generation")
	ready := generation > 0 && objectInt(status, "observedGeneration") >= generation
	switch parts[0] {
	case "Deployment", "StatefulSet":
		desired := objectInt(spec, "replicas")
		if _, set := spec["replicas"]; !set {
			desired = 1
		}
		ready = ready && desired > 0 && objectInt(status, "updatedReplicas") >= desired && objectInt(status, "readyReplicas") >= desired
		if parts[0] == "Deployment" {
			ready = ready && objectInt(status, "availableReplicas") >= desired
		}
	case "DaemonSet":
		desired := objectInt(status, "desiredNumberScheduled")
		ready = ready && desired > 0 && objectInt(status, "updatedNumberScheduled") >= desired && objectInt(status, "numberReady") >= desired && objectInt(status, "numberAvailable") >= desired
	case "Job":
		desired := objectInt(spec, "completions")
		if _, set := spec["completions"]; !set {
			desired = 1
		}
		ready = desired > 0 && objectInt(status, "succeeded") >= desired
	default:
		return WorkloadObservation{}, fmt.Errorf("raw strategy cannot attest workload kind %q", parts[0])
	}
	return WorkloadObservation{ResourceID: resourceID, Revision: tree, Images: images, Ready: ready}, nil
}

func objectMap(parent map[string]any, key string) map[string]any {
	value, _ := parent[key].(map[string]any)
	return value
}
func objectSlice(parent map[string]any, key string) []any {
	value, _ := parent[key].([]any)
	return value
}
func objectString(parent map[string]any, key string) string {
	value, _ := parent[key].(string)
	return value
}
func objectInt(parent map[string]any, key string) int {
	switch value := parent[key].(type) {
	case float64:
		return int(value)
	case json.Number:
		number, _ := strconv.Atoi(string(value))
		return number
	default:
		return 0
	}
}
