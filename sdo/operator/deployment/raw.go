//archcheck:allow R1 until=S3-13b reason="raw strategy plans BuildKit builds via buildkit.PlanBuilds until the deployer takes images from the catalog"
//archcheck:allow R4 until=S3-13b reason="raw strategy decoded in core deployment package"
//archcheck:ratchet R5 count=3 until=S3-13b
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
	"time"

	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

type WorkloadObservation struct {
	ContainerImages []rendered.DeployedImage
	ResourceID      string
	Revision        string
	Images          []string
	Ready           bool
}

// RawBackend owns the external effects. BuildImage must return a pushed
// immutable reference; Observe must read actual rollout state independently.
type RawBackend interface {
	BuildImage(context.Context, string, *contracts.ImageBuild) (string, error)
	Apply(context.Context, []byte) error
	Observe(context.Context, string) (WorkloadObservation, error)
}

// ImageResolver pins a mutable image reference (a tag) to its current
// manifest digest. It is trusted Go code: the deployer never supplies digests,
// so an LLM cannot choose or invent the bytes that run.
type ImageResolver interface {
	// ResolveImage returns repository@sha256:<digest> for ref.
	ResolveImage(ctx context.Context, ref string) (string, error)
}

type RawStrategy struct {
	Namespace string
	Backend   RawBackend
	// Resolver pins workload images named by tag that no image build covers.
	// Without it such images are rejected with guidance.
	Resolver ImageResolver
	// BuildBudget bounds all image builds of one apply together; zero means
	// only the backend's per-build deadline applies.
	BuildBudget time.Duration
}

// buildContext derives the context shared by the builds of one apply.
func (strategy RawStrategy) buildContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if strategy.BuildBudget <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, strategy.BuildBudget)
}

// tagImageRejection tells the deployer what to do about an unpinned tag. A
// transient failure (rate limit, timeout, network) is not a defect of the
// manifest, so the advice is to retry; a missing or private image needs a
// different reference.
func tagImageRejection(path, image string, cause error) error {
	var resolve *imagesrc.ResolveError
	switch {
	case cause == nil:
		return fmt.Errorf("manifest %s: image %q is not immutable and the operator has no image resolver configured. "+
			"Write the reference as <repository>@sha256:<64 hex digest>, or build it from source by listing it in image_builds under the trusted registry", path, image)
	case errors.As(cause, &resolve) && resolve.Kind == imagesrc.ResolveTransient:
		return fmt.Errorf("manifest %s: image %q could not be pinned to a digest right now (%w). This is a transient registry or network problem, "+
			"not a defect of the manifest: keep the reference and retry the deploy in a minute. If it keeps failing, write the reference as "+
			"<repository>@sha256:<64 hex digest> or build it from source by listing it in image_builds", path, image, cause)
	case errors.As(cause, &resolve) && resolve.Kind == imagesrc.ResolveRefused:
		return fmt.Errorf("manifest %s: image %q cannot be pinned: %s. Use an image on a public registry name, write the reference as "+
			"<repository>@sha256:<64 hex digest>, or build it from source by listing it in image_builds under the trusted registry", path, image, cause.Error())
	}
	return fmt.Errorf("manifest %s: image %q is not immutable and the operator could not resolve it: %s. Name an existing tag of a public registry image, "+
		"write the reference as <repository>@sha256:<64 hex digest>, or build it from source by listing it in image_builds under the trusted registry", path, image, cause.Error())
}

// resolveBudget bounds all tag resolution of one apply, so a slow registry
// cannot hold an admission for images x timeout.
const resolveBudget = 90 * time.Second

// RawOption adjusts a raw registration.
type RawOption func(*RawStrategy)

// WithBuildBudget bounds all image builds of one deployment attempt.
func WithBuildBudget(budget time.Duration) RawOption {
	return func(strategy *RawStrategy) { strategy.BuildBudget = budget }
}

func RawRegistration(id, namespace string, backend RawBackend, resolver ImageResolver, options ...RawOption) Registration {
	strategy := RawStrategy{Namespace: namespace, Backend: backend, Resolver: resolver}
	for _, option := range options {
		option(&strategy)
	}
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
		if len(receipt.ContainerImages) > 0 {
			expected := []rendered.DeployedImage{}
			for _, image := range receipt.ContainerImages {
				if image.Ref.Kind+"/"+image.Ref.Namespace+"/"+image.Ref.Name == id {
					expected = append(expected, image)
				}
			}
			if !sameNamedImages(expected, observed.ContainerImages) {
				return Observation{}, fmt.Errorf("workload %s named images differ from recorded deployment", id)
			}
		}
		images = append(images, observed.Images...)
		ids = append(ids, id)
	}
	if !equalStrings(images, receipt.Images) {
		return Observation{}, errors.New("accepted workload images differ from deployed images")
	}
	return Observation{ContainerImages: receipt.ContainerImages, Revision: receipt.ProposalTreeDigest, Images: images, ResourceIDs: ids, Ready: true}, nil
}

func (strategy RawStrategy) ApplyAndVerify(ctx context.Context, checkout, tree string, plan *contracts.RawManifestStrategy) (Observation, error) {
	if strategy.Backend == nil || strategy.Namespace == "" || plan == nil {
		return Observation{}, errors.New("raw deployment backend, namespace, and plan are required")
	}
	if len(plan.ManifestPaths) == 0 || len(plan.ExpectedWorkloadIds) == 0 {
		return Observation{}, errors.New("raw deployment needs manifests and expected workloads")
	}
	imageRefs := map[string]string{}
	imageInputs := map[string]rendered.ImageInputs{}
	planned, err := buildkit.PlanBuilds(checkout, plan.ImageBuilds)
	if err != nil {
		return Observation{}, err
	}
	buildCtx, stopBuilds := strategy.buildContext(ctx)
	defer stopBuilds()
	for _, item := range planned {
		build := item.Build
		resolved, err := item.Absolute(checkout)
		if err != nil {
			return Observation{}, err
		}
		immutable, err := strategy.Backend.BuildImage(buildCtx, checkout, resolved)
		if err != nil {
			if timeout := buildTimeout(ctx, buildCtx, build.Image, strategy.BuildBudget, err); timeout != nil {
				return Observation{}, timeout
			}
			return Observation{}, fmt.Errorf("build image %s: %w", build.Image, err)
		}
		if !digestReference.MatchString(immutable) || imageRepository(immutable) != imageRepository(build.Image) {
			return Observation{}, fmt.Errorf("builder returned wrong or mutable image for %s: %q", build.Image, immutable)
		}
		imageRefs[build.Image] = immutable
		imageInputs[build.Image] = rendered.ImageInputs{SourceImage: build.Image, ContextPath: item.ContextRel, DockerfilePath: item.DockerfileRel}
	}
	pinnedImages := map[string]string{}
	pinCtx, stopPinning := context.WithTimeout(ctx, resolveBudget)
	defer stopPinning()
	var documents bytes.Buffer
	namedImages := []rendered.DeployedImage{}
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
			ref, workload, err := rendered.Normalize(document, strategy.Namespace)
			if err != nil {
				_ = file.Close()
				return Observation{}, fmt.Errorf("manifest %s: %w", path, err)
			}
			id := ""
			if workload {
				id = ref.Kind + "/" + ref.Namespace + "/" + ref.Name
			}
			var resolveErr error
			images, err := Stamp(document, rendered.BaseTree(tree), func(site ImageSite) string {
				image := site.Image
				inputs := imageInputs[image]
				if inputs.SourceImage == "" {
					inputs.SourceImage = image
				}
				if built := imageRefs[image]; built != "" {
					image = built
				} else if !digestReference.MatchString(image) && strategy.Resolver != nil && resolveErr == nil {
					pinned, err := strategy.pin(pinCtx, image, pinnedImages)
					if err != nil {
						resolveErr = tagImageRejection(path, image, err)
					} else {
						image = pinned
					}
				}
				namedImages = append(namedImages, rendered.DeployedImage{Ref: sdk.ObjectRef{APIVersion: ref.APIVersion, Kind: site.Kind, Namespace: ref.Namespace, Name: site.Name}, Name: site.Container, Init: site.Init, Applied: image, Digest: image, Inputs: inputs})
				return image
			})
			if resolveErr != nil {
				_ = file.Close()
				return Observation{}, resolveErr
			}
			if err != nil {
				_ = file.Close()
				return Observation{}, fmt.Errorf("manifest %s: %w", path, err)
			}
			for _, image := range images {
				if !digestReference.MatchString(image) {
					_ = file.Close()
					return Observation{}, tagImageRejection(path, image, nil)
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
			documents.WriteString("---\n")
			documents.Write(encoded)
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
	if err := strategy.Backend.Apply(ctx, documents.Bytes()); err != nil {
		return Observation{}, fmt.Errorf("apply candidate manifests: %w", err)
	}
	ids := make([]string, 0, len(workloads))
	for id := range workloads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	allImages := []string{}
	verifiedImages := []rendered.DeployedImage{}
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
		// Legacy/custom backends that supply only a flat image list cannot
		// authorize per-container substitutions in a durable lock.
		if len(observed.ContainerImages) > 0 {
			expected := []rendered.DeployedImage{}
			for _, image := range namedImages {
				if image.Ref.Kind+"/"+image.Ref.Namespace+"/"+image.Ref.Name == id {
					expected = append(expected, image)
				}
			}
			if !sameNamedImages(expected, observed.ContainerImages) {
				return Observation{}, fmt.Errorf("workload %s container names or roles differ from applied images", id)
			}
			verifiedImages = append(verifiedImages, expected...)
		}
		allImages = append(allImages, observed.Images...)
	}
	return Observation{ContainerImages: verifiedImages, Revision: tree, Images: allImages, ResourceIDs: ids, Ready: true}, nil
}

// pin resolves one unbuilt tag reference once per plan, so every container
// naming it runs the same bytes.
func (strategy RawStrategy) pin(ctx context.Context, image string, cache map[string]string) (string, error) {
	if pinned, ok := cache[image]; ok {
		return pinned, nil
	}
	pinned, err := strategy.Resolver.ResolveImage(ctx, image)
	if err != nil {
		return "", err
	}
	if !digestReference.MatchString(pinned) {
		return "", fmt.Errorf("resolver returned a mutable reference %q", pinned)
	}
	cache[image] = pinned
	return pinned, nil
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

func sameNamedImages(expected, actual []rendered.DeployedImage) bool {
	if len(expected) != len(actual) {
		return false
	}
	counts := map[rendered.DeployedImage]int{}
	for _, image := range expected {
		image.Inputs = rendered.ImageInputs{} // observation verifies identities, not build declarations
		counts[image]++
	}
	for _, image := range actual {
		image.Inputs = rendered.ImageInputs{}
		counts[image]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}
