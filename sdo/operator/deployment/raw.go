package deployment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/rendered"
)

type WorkloadObservation struct {
	ResourceID string
	Revision   string
	Images     []string
	Ready      bool
}

// RawBackend owns the external effects. BuildImage must return a pushed
// immutable reference; Observe must read actual rollout state independently.
type RawBackend interface {
	BuildImage(context.Context, string, *contracts.ImageBuild) (string, error)
	Apply(context.Context, []byte) error
	Observe(context.Context, string) (WorkloadObservation, error)
}

type RawStrategy struct {
	Namespace string
	Backend   RawBackend
}

func RawRegistration(id, namespace string, backend RawBackend) Registration {
	strategy := RawStrategy{Namespace: namespace, Backend: backend}
	return Registration{
		ID: id, Config: &contracts.RawManifestStrategy{},
		Apply: func(ctx context.Context, checkout, tree string, message proto.Message) (Observation, error) {
			config, ok := message.(*contracts.RawManifestStrategy)
			if !ok {
				return Observation{}, errors.New("raw manifest strategy has wrong config type")
			}
			return strategy.ApplyAndVerify(ctx, checkout, tree, config)
		},
		Verify: func(ctx context.Context, _ string, namespace string, receipt lifecycle.DeploymentReceipt) (Observation, error) {
			if namespace != strategy.Namespace {
				return Observation{}, errors.New("accepted source namespace differs from registered strategy")
			}
			return strategy.VerifyAccepted(ctx, receipt)
		},
	}
}

func (strategy RawStrategy) VerifyAccepted(ctx context.Context, receipt lifecycle.DeploymentReceipt) (Observation, error) {
	if strategy.Backend == nil || !receipt.Ready || receipt.StrategyID == "" || len(receipt.ResourceIDs) == 0 {
		return Observation{}, errors.New("trusted raw deployment evidence is required")
	}
	images := []string{}
	ids := []string{}
	for _, id := range receipt.ResourceIDs {
		parts := strings.Split(id, "/")
		if len(parts) != 3 || parts[1] != strategy.Namespace {
			return Observation{}, fmt.Errorf("workload %q is outside the registered namespace", id)
		}
		observed, err := strategy.Backend.Observe(ctx, id)
		if err != nil {
			return Observation{}, err
		}
		if !observed.Ready || observed.ResourceID != id || observed.Revision != receipt.ProposalTreeDigest {
			return Observation{}, fmt.Errorf("workload %s no longer matches accepted source", id)
		}
		images = append(images, observed.Images...)
		ids = append(ids, id)
	}
	if !equalStrings(images, receipt.Images) {
		return Observation{}, errors.New("accepted workload images differ from deployed images")
	}
	return Observation{Revision: receipt.ProposalTreeDigest, Images: images, ResourceIDs: ids, Ready: true}, nil
}

func (strategy RawStrategy) ApplyAndVerify(ctx context.Context, checkout, tree string, plan *contracts.RawManifestStrategy) (Observation, error) {
	if strategy.Backend == nil || strategy.Namespace == "" || plan == nil {
		return Observation{}, errors.New("raw deployment backend, namespace, and plan are required")
	}
	if len(plan.ManifestPaths) == 0 || len(plan.ExpectedWorkloadIds) == 0 {
		return Observation{}, errors.New("raw deployment needs manifests and expected workloads")
	}
	imageRefs := map[string]string{}
	for _, build := range plan.ImageBuilds {
		if build == nil || build.Image == "" || build.ContextPath == "" || build.DockerfilePath == "" {
			return Observation{}, errors.New("image build needs image, context, and Dockerfile")
		}
		if _, exists := imageRefs[build.Image]; exists {
			return Observation{}, fmt.Errorf("duplicate image build %q", build.Image)
		}
		contextPath, err := safeCandidatePath(checkout, build.ContextPath)
		if err != nil {
			return Observation{}, err
		}
		dockerfilePath, err := safeCandidatePath(checkout, build.DockerfilePath)
		if err != nil {
			return Observation{}, err
		}
		resolved := proto.Clone(build).(*contracts.ImageBuild)
		resolved.ContextPath, resolved.DockerfilePath = contextPath, dockerfilePath
		immutable, err := strategy.Backend.BuildImage(ctx, checkout, resolved)
		if err != nil {
			return Observation{}, fmt.Errorf("build image %s: %w", build.Image, err)
		}
		if !digestReference.MatchString(immutable) || imageRepository(immutable) != imageRepository(build.Image) {
			return Observation{}, fmt.Errorf("builder returned wrong or mutable image for %s: %q", build.Image, immutable)
		}
		imageRefs[build.Image] = immutable
	}
	var rendered bytes.Buffer
	workloads := map[string][]string{}
	manifestPaths, err := candidateManifestPaths(checkout, plan.ManifestPaths)
	if err != nil {
		return Observation{}, err
	}
	for _, path := range manifestPaths {
		resolved, err := safeCandidatePath(checkout, path)
		if err != nil {
			return Observation{}, err
		}
		file, err := os.Open(resolved)
		if err != nil {
			return Observation{}, err
		}
		decoder := yaml.NewDecoder(file)
		for {
			var document map[string]any
			decodeErr := decoder.Decode(&document)
			if decodeErr == io.EOF {
				break
			}
			if decodeErr != nil {
				_ = file.Close()
				return Observation{}, fmt.Errorf("manifest %s: %w", path, decodeErr)
			}
			if len(document) == 0 {
				continue
			}
			id, images, err := RenderManifest(document, strategy.Namespace, tree, func(_ string, _ string, _ string, _ bool, image string) string {
				if built := imageRefs[image]; built != "" {
					return built
				}
				return image
			})
			if err != nil {
				_ = file.Close()
				return Observation{}, fmt.Errorf("manifest %s: %w", path, err)
			}
			for _, image := range images {
				if !digestReference.MatchString(image) {
					_ = file.Close()
					return Observation{}, fmt.Errorf("manifest %s: workload image is not immutable: %q", path, image)
				}
			}
			if id != "" {
				if _, exists := workloads[id]; exists {
					_ = file.Close()
					return Observation{}, fmt.Errorf("duplicate workload %s", id)
				}
				workloads[id] = images
			}
			encoded, err := yaml.Marshal(document)
			if err != nil {
				_ = file.Close()
				return Observation{}, err
			}
			rendered.WriteString("---\n")
			rendered.Write(encoded)
		}
		if err := file.Close(); err != nil {
			return Observation{}, err
		}
	}
	wanted := map[string]bool{}
	for _, id := range plan.ExpectedWorkloadIds {
		if id == "" || wanted[id] {
			return Observation{}, errors.New("expected workload IDs must be unique and nonempty")
		}
		wanted[id] = true
	}
	if len(wanted) != len(workloads) {
		return Observation{}, errors.New("expected workload IDs differ from rendered manifests")
	}
	for id := range workloads {
		if !wanted[id] {
			return Observation{}, fmt.Errorf("workload %s is missing from deployment plan", id)
		}
	}
	if err := strategy.Backend.Apply(ctx, rendered.Bytes()); err != nil {
		return Observation{}, fmt.Errorf("apply candidate manifests: %w", err)
	}
	ids := make([]string, 0, len(workloads))
	for id := range workloads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	allImages := []string{}
	for _, id := range ids {
		observed, err := strategy.Backend.Observe(ctx, id)
		if err != nil {
			return Observation{}, fmt.Errorf("observe workload %s: %w", id, err)
		}
		expectedImages := append([]string(nil), workloads[id]...)
		actualImages := append([]string(nil), observed.Images...)
		sort.Strings(expectedImages)
		sort.Strings(actualImages)
		if !observed.Ready || observed.ResourceID != id || observed.Revision != tree || strings.Join(expectedImages, "\x00") != strings.Join(actualImages, "\x00") {
			return Observation{}, fmt.Errorf("workload %s is not ready at the candidate revision and images", id)
		}
		allImages = append(allImages, observed.Images...)
	}
	return Observation{Revision: tree, Images: allImages, ResourceIDs: ids, Ready: true}, nil
}

// RenderManifest applies the common deployment and lock renderer.
func RenderManifest(document map[string]any, namespace, tree string, imageFor func(string, string, string, bool, string) string) (string, []string, error) {
	return rendered.RenderManifest(document, namespace, tree, imageFor)
}

func safeCandidatePath(root, raw string) (string, error) {
	if raw == "" || filepath.IsAbs(raw) || raw == ".." || strings.HasPrefix(filepath.Clean(raw), ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("candidate path escapes proposal: %q", raw)
	}
	base, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	path, err := filepath.EvalSymlinks(filepath.Join(base, raw))
	if err != nil {
		return "", err
	}
	if rel, err := filepath.Rel(base, path); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("candidate path escapes proposal: %q", raw)
	}
	return path, nil
}

func imageRepository(image string) string {
	if at := strings.IndexByte(image, '@'); at >= 0 {
		return image[:at]
	}
	lastSlash := strings.LastIndexByte(image, '/')
	if colon := strings.LastIndexByte(image, ':'); colon > lastSlash {
		return image[:colon]
	}
	return image
}
