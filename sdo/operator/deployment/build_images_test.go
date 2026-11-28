package deployment

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
	"sdo.dev/operator/render/rendertest"
)

const buildRegistry = "registry.test/team/app"

// buildSource stands in for BuildKit: it records what it built and fails the
// images it is told to.
type buildSource struct {
	mu    sync.Mutex
	built []string
	fail  map[string]error
}

func (b *buildSource) Resolve(_ context.Context, req imagesrc.Request, _ *wrapperspb.StringValue) (imagesrc.Resolved, error) {
	b.mu.Lock()
	b.built = append(b.built, req.Image)
	b.mu.Unlock()
	if err := b.fail[req.Image]; err != nil {
		return imagesrc.Resolved{}, err
	}
	return imagesrc.Resolved{Ref: imagesrc.Repository(req.Image) + "@" + imagesrctest.Digest(req.Image), Inputs: imagesrc.Inputs{SourceImage: req.Image}}, nil
}

func buildDeployer(t *testing.T, files rendertest.MapFS, fail map[string]error) (Deployer, string, *buildSource, *imagesrctest.Fake) {
	t.Helper()
	root := depCommit(t, files)
	deployer, prebuilt := depDeployer(t, root, &clusterFake{})
	builds := &buildSource{fail: fail}
	if err := imagesrc.Register[*wrapperspb.StringValue](deployer.Images, "build", builds, imagesrc.Options{}); err != nil {
		t.Fatal(err)
	}
	if err := deployer.Images.SetTrustedPushHost(buildRegistry); err != nil {
		t.Fatal(err)
	}
	deployer.Images.SetBuildDerivation("build", func(contextPath, dockerfile string) (*anypb.Any, error) {
		return anypb.New(wrapperspb.String(contextPath + "|" + dockerfile))
	})
	return deployer, root, builds, prebuilt
}

func twoBuildTree() rendertest.MapFS {
	deployment := func(name, image string) string {
		return "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: " + name + "}\nspec:\n  selector: {matchLabels: {app: " + name + "}}\n  template:\n    metadata: {labels: {app: " + name + "}}\n    spec: {containers: [{name: c, image: \"" + image + "\"}]}\n"
	}
	return rendertest.MapFS{
		"deploy/frontend.yaml": deployment("frontend", buildRegistry+"/frontend:1"),
		"deploy/currency.yaml": deployment("currency", buildRegistry+"/currency:1"),
		"deploy/cache.yaml":    deployment("cache", "busybox:1"),
		"frontend/Dockerfile":  "FROM scratch\n",
		"currency/Dockerfile":  "FROM scratch\n",
	}
}

// The deployer's turn-time build builds every image Go's deploy would build
// from the committed tree, and nothing Go would only pin.
func TestBuildImagesBuildsWhatTheDeployBuilds(t *testing.T) {
	deployer, root, builds, prebuilt := buildDeployer(t, twoBuildTree(), nil)
	report, err := deployer.BuildImages(context.Background(), root, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(builds.built)
	if want := []string{buildRegistry + "/currency:1", buildRegistry + "/frontend:1"}; !slices.Equal(builds.built, want) {
		t.Fatalf("built %v, want %v", builds.built, want)
	}
	if len(prebuilt.Calls()) != 0 {
		t.Fatalf("a build must not pin prebuilt images, pinned %v", prebuilt.Calls())
	}
	if report.Err != nil || len(report.Built) != 2 || !strings.Contains(report.Text(), "currency@sha256:") {
		t.Fatalf("report = %+v\n%s", report, report.Text())
	}
}

// One image is named by its rendered reference, its repository or the last
// path segment of its repository.
func TestBuildImagesBuildsOnlyTheNamedImage(t *testing.T) {
	for _, name := range []string{"currency", buildRegistry + "/currency", buildRegistry + "/currency:1"} {
		deployer, root, builds, _ := buildDeployer(t, twoBuildTree(), nil)
		if _, err := deployer.BuildImages(context.Background(), root, nil, []string{name}); err != nil {
			t.Fatal(err)
		}
		if want := []string{buildRegistry + "/currency:1"}; !slices.Equal(builds.built, want) {
			t.Fatalf("%s: built %v, want %v", name, builds.built, want)
		}
	}
}

func TestBuildImagesRefusesAnImageGoDoesNotBuild(t *testing.T) {
	deployer, root, builds, _ := buildDeployer(t, twoBuildTree(), nil)
	_, err := deployer.BuildImages(context.Background(), root, nil, []string{"busybox"})
	if err == nil || !strings.Contains(err.Error(), "frontend") || !strings.Contains(err.Error(), "currency") || len(builds.built) != 0 {
		t.Fatalf("err = %v built %v; want a refusal that lists the buildable images", err, builds.built)
	}
}

// A failing build is reported with its own error (BuildKit's carries the
// failing step and log tail); the other images still build.
func TestBuildImagesReportsEachFailingImage(t *testing.T) {
	deployer, root, builds, _ := buildDeployer(t, twoBuildTree(), map[string]error{buildRegistry + "/currency:1": errors.New("CMake Error: Could NOT find Protobuf")})
	report, err := deployer.BuildImages(context.Background(), root, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	text := report.Text()
	if len(builds.built) != 2 || report.Err == nil || !strings.Contains(text, "Could NOT find Protobuf") || !strings.Contains(text, "failed  "+buildRegistry+"/currency:1") || !strings.Contains(text, "ok      "+buildRegistry+"/frontend:1") {
		t.Fatalf("built %v report:\n%s", builds.built, text)
	}
}

func TestBuildImagesWithNothingToBuildSaysSo(t *testing.T) {
	deployer, root, _, _ := buildDeployer(t, rendertest.MapFS{"deploy/cache.yaml": twoBuildTree()["deploy/cache.yaml"]}, nil)
	_, err := deployer.BuildImages(context.Background(), root, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "no image") {
		t.Fatalf("err = %v; a tree whose images are all prebuilt has nothing to build", err)
	}
}
