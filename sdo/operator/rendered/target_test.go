package rendered_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

func syntheticID(prefix, release string) string { return prefix + release + strings.Repeat("0", 38) }

// releasesSource is a synthetic repository of releases "1".."3", each a commit
// whose render is three phases: config, web (pinned image) and worker.
func releasesSource(t *testing.T) rendered.Source {
	t.Helper()
	git := func(ctx context.Context, root string, args ...string) ([]byte, error) {
		for _, release := range []string{"1", "2", "3"} {
			commit, tree := syntheticID("a", release), syntheticID("b", release)
			mapped := make([]string, len(args))
			hit := false
			for i, arg := range args {
				mapped[i] = strings.ReplaceAll(strings.ReplaceAll(arg, commit, syntheticCommit), tree, syntheticTree)
				hit = hit || mapped[i] != arg
			}
			if hit || args[0] == "cat-file" && args[1] == "blob" {
				out, err := treeGit(rendertest.MapFS{"release": release})(ctx, root, mapped...)
				if err == nil {
					return []byte(strings.ReplaceAll(string(out), syntheticTree, tree)), nil
				}
				if hit {
					return nil, err
				}
			}
		}
		return nil, fmt.Errorf("no synthetic object for git %v", args)
	}
	c := render.NewCatalog()
	if err := render.Register(c, "releases", render.RendererFunc[*contracts.RawRenderConfig](func(_ context.Context, fsys render.FS, env render.Env, _ *contracts.RawRenderConfig) (render.Result, error) {
		data, err := fs.ReadFile(fsys, "release")
		if err != nil {
			return render.Result{}, err
		}
		release := string(data)
		object := func(apiVersion, kind, name string, fields map[string]any) *unstructured.Unstructured {
			o := map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": map[string]any{"name": name, "namespace": "app"}}
			for k, v := range fields {
				o[k] = v
			}
			return &unstructured.Unstructured{Object: o}
		}
		workload := func(name string) *unstructured.Unstructured {
			return object("apps/v1", "Deployment", name, map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": name + ":" + release}}}}}})
		}
		secret := func(name string) *unstructured.Unstructured {
			o := object("v1", "Secret", name, map[string]any{"stringData": map[string]any{"release": release}})
			if env.Secrets == render.SecretsOmit {
				o.Object = render.SecretIdentity(o.Object)
			}
			return o
		}
		return render.Result{Phases: []render.Phase{
			{Name: "config", Objects: []*unstructured.Unstructured{object("v1", "ConfigMap", "settings", map[string]any{"data": map[string]any{"release": release}})}},
			{Name: "web", Objects: []*unstructured.Unstructured{workload("web")}},
			{Name: "worker", Objects: []*unstructured.Unstructured{workload("worker"), object("v1", "ConfigMap", "only-"+release, nil), secret("token"), secret("token-" + release)}},
		}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	config, err := anypb.New(&contracts.RawRenderConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return rendered.Source{Repository: "/synthetic", Git: git, Catalog: c, Deployment: &contracts.DeploymentSource{RendererId: "releases", Config: config}}
}

var (
	settingsRef = sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
	workerRef   = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "worker"}
)

// releaseOf names the release an accepted object renders, or "refused".
func releaseOf(t *testing.T, s rendered.Source, revision string, ref sdk.ObjectRef) string {
	t.Helper()
	object, err := s.AcceptedManifest(context.Background(), revision, ref)
	if errors.Is(err, rendered.ErrTargetInterrupted) {
		return "refused"
	}
	if err != nil {
		t.Fatal(err)
	}
	if ref.Kind == "ConfigMap" {
		value, _, _ := unstructured.NestedString(object.Object, "data", "release")
		return value
	}
	containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
	_, tag, _ := strings.Cut(containers[0].(map[string]any)["image"].(string), ":")
	return tag
}

// While a head is applied and not accepted, comparisons and restores read what
// each phase of the cluster was moved to: the head in an applied phase, the
// revision before it in an unreached one, neither in an interrupted one.
func TestReadsFollowTheDeploymentTargetPhaseByPhase(t *testing.T) {
	v1, v2, v3 := syntheticID("a", "1"), syntheticID("a", "2"), syntheticID("a", "3")
	step := func(target *rendered.DeploymentTarget, release, phase string, applied bool, later ...string) {
		target.Advance(rendered.TargetWrite{Phase: phase, Later: later, Applied: applied, Pins: rendered.ReceiptPins{EffectiveTree: syntheticID("b", release)}})
	}
	begin := func(commit string, previous *rendered.DeploymentTarget) *rendered.DeploymentTarget {
		return &rendered.DeploymentTarget{Commit: commit, AcceptedCommit: v1, Previous: previous, Unreached: []string{"config", "web", "worker"}}
	}
	accepted := &rendered.DeploymentTarget{Commit: v1}
	target := begin(v2, accepted)
	source := releasesSource(t)
	source.Target = func() (*rendered.DeploymentTarget, error) { return target, nil }
	source = source.WithCache()
	check := func(name, settings, web, worker string) {
		t.Helper()
		got := []string{releaseOf(t, source, target.Commit, settingsRef), releaseOf(t, source, target.Commit, webRef), releaseOf(t, source, target.Commit, workerRef)}
		if want := []string{settings, web, worker}; !slices.Equal(got, want) {
			t.Fatalf("%s: settings, web, worker read releases %v, want %v", name, got, want)
		}
	}
	step(target, "2", "", false, "config", "web", "worker")
	check("only a hook ran", "1", "1", "1")
	step(target, "2", "config", false, "web", "worker")
	check("crash between record and apply", "refused", "1", "1")
	step(target, "2", "config", true, "web", "worker")
	step(target, "2", "web", false, "worker")
	check("web apply interrupted", "2", "refused", "1")
	step(target, "2", "web", true, "worker")
	check("worker unreached", "2", "2", "1")
	objects, err := source.AcceptedManifests(context.Background(), v2, "app")
	if err != nil || !slices.Equal(refs(objects), []string{"ConfigMap/app/settings", "Deployment/app/web", "Deployment/app/worker", "ConfigMap/app/only-1", "Secret/app/token", "Secret/app/token-1"}) {
		t.Fatalf("desired objects = %v, %v; the unreached phase keeps the previous revision's", refs(objects), err)
	}

	// A push over the partial head: its unreached phases keep what they run.
	partial := *target
	target = begin(v3, &partial)
	step(target, "3", "config", true, "web", "worker")
	check("a push over a partial head", "3", "2", "1")

	target = &partial
	step(target, "2", "config", false, "web", "worker")
	check("the same head retried", "refused", "2", "1")
	step(target, "2", "config", true, "web", "worker")
	step(target, "2", "web", true, "worker")
	step(target, "2", "worker", true)
	check("fully applied", "2", "2", "2")
	if target.Previous != nil {
		t.Fatal("a whole apply still names the revision before it")
	}
	if got := releaseOf(t, source, v1, webRef); got != "1" {
		t.Fatalf("a read of another revision gave release %s, want its own", got)
	}
	target = nil
	if got := releaseOf(t, source, v1, webRef); got != "1" {
		t.Fatalf("without a target release %s, want 1", got)
	}
}

// The target's own images are desired state; the accepted pins are not.
func TestTheTargetsImagesAreDesiredState(t *testing.T) {
	source := releasesSource(t)
	pins := func(release, image string) rendered.ReceiptPins {
		return rendered.ReceiptPins{EffectiveTree: syntheticID("b", release), Images: []rendered.DeployedImage{{Ref: webRef, Name: "c", Applied: "web:" + image}}}
	}
	source.Pins = pins("1", "accepted-digest")
	source.Target = func() (*rendered.DeploymentTarget, error) {
		return &rendered.DeploymentTarget{Commit: syntheticID("a", "2"), Pins: pins("2", "target-digest")}, nil
	}
	if got := releaseOf(t, source, syntheticID("a", "2"), webRef); got != "target-digest" {
		t.Fatalf("target image = %s", got)
	}
	if got := releaseOf(t, source, syntheticID("a", "1"), webRef); got != "accepted-digest" {
		t.Fatalf("accepted image = %s", got)
	}
	source.Target = func() (*rendered.DeploymentTarget, error) { return nil, errors.New("unreadable") }
	if _, err := source.AcceptedManifest(context.Background(), syntheticID("a", "2"), webRef); err == nil {
		t.Fatal("an unreadable target fell back to the accepted revision")
	}
}
