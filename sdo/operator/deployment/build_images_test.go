package deployment

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
	"sdo.dev/operator/internal/testutil"
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

// BuildInputs reads the "context|dockerfile" config the test derivation writes.
func (b *buildSource) BuildInputs(_ fs.FS, config *wrapperspb.StringValue) (imagesrc.BuildInputs, error) {
	contextPath, dockerfile, _ := strings.Cut(config.GetValue(), "|")
	return imagesrc.BuildInputs{Paths: []string{contextPath, dockerfile}}, nil
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
	report, err := deployer.BuildImages(context.Background(), root, nil, nil, BuildOptions{})
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
		if _, err := deployer.BuildImages(context.Background(), root, nil, []string{name}, BuildOptions{}); err != nil {
			t.Fatal(err)
		}
		if want := []string{buildRegistry + "/currency:1"}; !slices.Equal(builds.built, want) {
			t.Fatalf("%s: built %v, want %v", name, builds.built, want)
		}
	}
}

func TestBuildImagesRefusesAnImageGoDoesNotBuild(t *testing.T) {
	deployer, root, builds, _ := buildDeployer(t, twoBuildTree(), nil)
	_, err := deployer.BuildImages(context.Background(), root, nil, []string{"busybox"}, BuildOptions{})
	if err == nil || !strings.Contains(err.Error(), "frontend") || !strings.Contains(err.Error(), "currency") || len(builds.built) != 0 {
		t.Fatalf("err = %v built %v; want a refusal that lists the buildable images", err, builds.built)
	}
}

// A failing build is reported with its own error (BuildKit's carries the
// failing step and log tail); the other images still build.
func TestBuildImagesReportsEachFailingImage(t *testing.T) {
	deployer, root, builds, _ := buildDeployer(t, twoBuildTree(), map[string]error{buildRegistry + "/currency:1": errors.New("CMake Error: Could NOT find Protobuf")})
	report, err := deployer.BuildImages(context.Background(), root, nil, nil, BuildOptions{})
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
	_, err := deployer.BuildImages(context.Background(), root, nil, nil, BuildOptions{})
	if err == nil || !strings.Contains(err.Error(), "no image") {
		t.Fatalf("err = %v; a tree whose images are all prebuilt has nothing to build", err)
	}
}

func commitFile(t *testing.T, root, file, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, file)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, file), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "change "+file)
}

// With no names, a build rebuilds exactly the images whose inputs changed
// since each one's last good build, says why, and lists the rest as
// unchanged; an image that failed stays selected until it builds.
func TestBuildImagesRebuildsWhatChangedSinceEachImagesLastGoodBuild(t *testing.T) {
	files := twoBuildTree()
	files["frontend/app.js"] = "1"
	files["currency/main.cc"] = "1"
	files["shared/demo.proto"] = "1"
	deployer, root, builds, _ := buildDeployer(t, files, map[string]error{buildRegistry + "/currency:1": errors.New("CMake Error")})
	record := BuildRecordFile{Root: t.TempDir(), Rel: "state/image-builds.json"}
	build := func() BuildReport {
		t.Helper()
		builds.built = nil
		report, err := deployer.BuildImages(context.Background(), root, nil, nil, BuildOptions{Record: record})
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(builds.built)
		return report
	}
	first := build()
	if want := []string{buildRegistry + "/currency:1", buildRegistry + "/frontend:1"}; !slices.Equal(builds.built, want) {
		t.Fatalf("first build built %v, want %v\n%s", builds.built, want, first.Text())
	}
	if !strings.Contains(first.Text(), "no earlier good build") {
		t.Fatalf("first build must say why every image was built:\n%s", first.Text())
	}
	// Nothing changed: the failed image is still selected, the good one is not.
	second := build()
	if want := []string{buildRegistry + "/currency:1"}; !slices.Equal(builds.built, want) {
		t.Fatalf("unchanged tree rebuilt %v, want only the failed %v\n%s", builds.built, want, second.Text())
	}
	if !strings.Contains(second.Text(), "unchanged since its last good build") || !strings.Contains(second.Text(), "frontend") {
		t.Fatalf("report must list the image it skipped:\n%s", second.Text())
	}
	// The fix lands; then a shared input changes, read only by frontend.
	delete(builds.fail, buildRegistry+"/currency:1")
	commitFile(t, root, "currency/main.cc", "2")
	if build(); !slices.Equal(builds.built, []string{buildRegistry + "/currency:1"}) {
		t.Fatalf("built %v after a currency change", builds.built)
	}
	commitFile(t, root, "frontend/app.js", "2")
	commitFile(t, root, "shared/demo.proto", "2")
	report := build()
	if want := []string{buildRegistry + "/frontend:1"}; !slices.Equal(builds.built, want) {
		t.Fatalf("built %v, want %v (only frontend reads frontend/)\n%s", builds.built, want, report.Text())
	}
	if !strings.Contains(report.Text(), "frontend/app.js") {
		t.Fatalf("report must name the changed input:\n%s", report.Text())
	}
	// Nothing left to build is a report, not an error.
	if empty := build(); len(builds.built) != 0 || empty.Err != nil || !strings.Contains(empty.Text(), "Nothing to build") {
		t.Fatalf("built %v\n%s", builds.built, empty.Text())
	}
}

// Naming images builds those; the report still names the other images a
// change since their last good build affects, so a shared input is not missed.
func TestBuildImagesNamesChangedImagesItWasNotAskedToBuild(t *testing.T) {
	deployer, root, builds, _ := buildDeployer(t, twoBuildTree(), nil)
	record := BuildRecordFile{Root: t.TempDir(), Rel: "state/image-builds.json"}
	if _, err := deployer.BuildImages(context.Background(), root, nil, nil, BuildOptions{Record: record}); err != nil {
		t.Fatal(err)
	}
	commitFile(t, root, "frontend/Dockerfile", "FROM scratch\nLABEL x=1\n")
	commitFile(t, root, "currency/Dockerfile", "FROM scratch\nLABEL x=1\n")
	builds.built = nil
	report, err := deployer.BuildImages(context.Background(), root, nil, []string{"currency"}, BuildOptions{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(builds.built, []string{buildRegistry + "/currency:1"}) || !strings.Contains(report.Text(), "not built") || !strings.Contains(report.Text(), buildRegistry+"/frontend:1") {
		t.Fatalf("built %v\n%s", builds.built, report.Text())
	}
}

// A build reports each image as it finishes, before the whole build ends.
func TestBuildImagesReportsProgressPerImage(t *testing.T) {
	deployer, root, _, _ := buildDeployer(t, twoBuildTree(), map[string]error{buildRegistry + "/currency:1": errors.New("boom")})
	var lines []string
	if _, err := deployer.BuildImages(context.Background(), root, nil, nil, BuildOptions{Progress: func(line string) { lines = append(lines, line) }}); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "Building 2 images") || !strings.Contains(joined, "failed  "+buildRegistry+"/currency:1") || !strings.Contains(joined, "ok      "+buildRegistry+"/frontend:1") || !strings.Contains(joined, "of 2") {
		t.Fatalf("progress:\n%s", joined)
	}
}
