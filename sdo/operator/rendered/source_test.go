package rendered_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/kustomize"
	"sdo.dev/operator/render/raw"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

const (
	syntheticCommit = "0123456789abcdef0123456789abcdef01234567"
	syntheticTree   = "89abcdef0123456789abcdef0123456789abcdef"
)

func catalog(t *testing.T) *render.Catalog {
	t.Helper()
	c := render.NewCatalog()
	if err := raw.Register(c); err != nil {
		t.Fatal(err)
	}
	if err := kustomize.Register(c); err != nil {
		t.Fatal(err)
	}
	return c
}

func rawSource(t *testing.T, paths ...string) *contracts.DeploymentSource {
	t.Helper()
	src, err := rendered.LegacyRawSource(paths)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func kustomizeSource(t *testing.T, dir string) *contracts.DeploymentSource {
	t.Helper()
	config, err := anypb.New(&contracts.KustomizeRenderConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.DeploymentSource{RendererId: kustomize.ID, Config: config}
}

// treeGit is a synthetic object store holding one commit whose tree is files:
// it answers the Git reads the accepted-source reader makes (rev-parse of the
// commit's tree, the tree type check, the long listing and blob reads).
func treeGit(files rendertest.MapFS) rendered.GitFunc {
	return func(_ context.Context, _ string, args ...string) ([]byte, error) {
		switch {
		case len(args) == 4 && args[0] == "rev-parse" && args[3] == syntheticCommit+"^{tree}":
			return []byte(syntheticTree + "\n"), nil
		case len(args) == 3 && args[0] == "cat-file" && args[1] == "-t" && args[2] == syntheticTree:
			return []byte("tree\n"), nil
		case len(args) == 5 && args[0] == "ls-tree" && args[3] == syntheticTree:
			// Effective lists the direct children of .sdo/ directories.
			dir := args[4]
			seen := map[string]bool{}
			var out strings.Builder
			for name := range files {
				rest, ok := strings.CutPrefix(name, dir)
				if !ok {
					continue
				}
				child, _, nested := strings.Cut(rest, "/")
				if seen[child] {
					continue
				}
				seen[child] = true
				kind, mode := "blob", "100644"
				if nested {
					kind, mode = "tree", "040000"
				}
				fmt.Fprintf(&out, "%s %s %s\t%s%s\x00", mode, kind, strings.Repeat("e", 40), dir, child)
			}
			return []byte(out.String()), nil
		case len(args) >= 1 && args[0] == "ls-tree" && args[len(args)-1] == syntheticTree:
			names := make([]string, 0, len(files))
			for name := range files {
				names = append(names, name)
			}
			sort.Strings(names)
			var out strings.Builder
			for _, name := range names {
				blob, _ := files.Blob(name)
				fmt.Fprintf(&out, "100644 blob %s %d\t%s\x00", blob, len(files[name]), name)
			}
			return []byte(out.String()), nil
		case len(args) == 3 && args[0] == "cat-file" && args[1] == "blob":
			for name, content := range files {
				if blob, _ := files.Blob(name); blob == args[2] {
					return []byte(content), nil
				}
			}
		}
		return nil, fmt.Errorf("synthetic repository has no object for git %v", args)
	}
}

const webDeployment = "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec: {replicas: 2, template: {spec: {containers: [{name: c, image: 'example/web:1'}]}}}\n"

var rawTree = rendertest.MapFS{
	"k8s/web.yaml":            webDeployment,
	"k8s/sub/svc.yml":         "apiVersion: v1\nkind: Service\nmetadata: {name: web, namespace: app}\nspec: {ports: [{port: 80}]}\n",
	"k8s/sub/other.yml":       "apiVersion: v1\nkind: Service\nmetadata: {name: elsewhere, namespace: other}\n",
	"k8s/sub/notes.txt":       "ignored",
	"k8s/creds.yaml":          "apiVersion: v1\nkind: Secret\nmetadata: {name: creds, annotations: {k: v}}\nstringData: {password: hunter2}\n",
	"k8s/multi.yaml":          "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: a}\ndata: {n: '1'}\n---\nkind: Nameless\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: b}\n",
	"k8s/ns.json":             `{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"app"}}`,
	"loose.yaml":              webDeployment,
	"bad/broken.yaml":         "kind: [unterminated\n",
	"dup/a.yaml":              webDeployment,
	"dup/b.yaml":              webDeployment,
	"kust/kustomization.yaml": "resources: [d.yaml]\n",
	"kust/d.yaml":             webDeployment,
	"imageless/d.yaml":        "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: empty}\nspec: {template: {spec: {containers: []}}}\n",
}

func refs(objects []*unstructured.Unstructured) []string {
	out := make([]string, 0, len(objects))
	for _, o := range objects {
		out = append(out, o.GetKind()+"/"+o.GetNamespace()+"/"+o.GetName())
	}
	return out
}

// TestSourceReadsRawSelections pins the raw selection rules as reads see them
// through the catalog: sorted files, nameless documents skipped, namespaces
// defaulted, cluster scope cleared, foreign objects out of the unqualified
// view, Secrets reduced to identity, and every failure classified.
func TestSourceReadsRawSelections(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		paths []string
		want  []string
		kind  error
	}{
		"directory":         {paths: []string{"k8s"}, want: []string{"Secret/app/creds", "ConfigMap/app/a", "ConfigMap/app/b", "Namespace//app", "Service/app/web", "Deployment/app/web"}},
		"nested directory":  {paths: []string{"k8s/sub"}, want: []string{"Service/app/web"}},
		"file and nested":   {paths: []string{"loose.yaml", "k8s/sub"}, want: []string{"Service/app/web", "Deployment/app/web"}},
		"json":              {paths: []string{"k8s/ns.json"}, want: []string{"Namespace//app"}},
		"no paths":          {kind: rendered.ErrSourceUnconfigured},
		"decode failure":    {paths: []string{"bad"}, kind: rendered.ErrSourceInvalid},
		"missing":           {paths: []string{"absent"}, kind: rendered.ErrSourceInvalid},
		"selected twice":    {paths: []string{"k8s/sub", "k8s/sub/svc.yml"}, kind: rendered.ErrSourceInvalid},
		"escape":            {paths: []string{"../x"}, kind: rendered.ErrSourceInvalid},
		"no manifests":      {paths: []string{"k8s/sub/notes.txt"}, kind: rendered.ErrSourceInvalid},
		"duplicate objects": {paths: []string{"dup"}, kind: rendered.ErrSourceInvalid},
		// Transitional (S3-11c): a path list naming one kustomize root reads
		// through kustomize, as the old reader did; a mix stays rejected (D3).
		"legacy kustomize root":     {paths: []string{"kust"}, want: []string{"Deployment/app/web"}},
		"legacy kustomization file": {paths: []string{"kust/kustomization.yaml"}, want: []string{"Deployment/app/web"}},
		"kustomize root mixed":      {paths: []string{"kust", "k8s/sub"}, kind: rendered.ErrSourceInvalid},
		"imageless workload":        {paths: []string{"imageless"}, kind: rendered.ErrSourceInvalid},
	} {
		t.Run(name, func(t *testing.T) {
			source := rendered.Source{Repository: "/synthetic", Deployment: rawSource(t, tc.paths...), Catalog: catalog(t), Git: treeGit(rawTree)}
			objects, err := source.RawManifests(ctx, syntheticCommit, "app")
			if tc.kind != nil {
				if !errors.Is(err, tc.kind) {
					t.Fatalf("err = %v, want %v", err, tc.kind)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := refs(objects); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("objects = %v, want %v", got, tc.want)
			}
			for _, o := range objects {
				if o.GetKind() == "Secret" && !reflect.DeepEqual(o.Object, map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "creds", "namespace": "app"}}) {
					t.Fatalf("Secret read is not identity-only: %v", o.Object)
				}
			}
		})
	}
	source := rendered.Source{Repository: "/synthetic", Deployment: rawSource(t, "k8s"), Catalog: catalog(t), Git: treeGit(rawTree)}
	peer, err := source.AcceptedScopedManifest(ctx, syntheticCommit, "app", sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "other", Name: "elsewhere"})
	if err != nil || peer.GetNamespace() != "other" {
		t.Fatalf("qualified view lost the foreign object: %v %v", peer, err)
	}
}

// TestSourceRendersKustomizeSourceFromTheAcceptedTree: a kustomize
// deployment source read through Git renders exactly what the renderer
// renders over the same files (namespace defaulted by core), and Secret
// values never leave the read.
func TestSourceRendersKustomizeSourceFromTheAcceptedTree(t *testing.T) {
	const deployment = "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec:\n  selector: {matchLabels: {app: web}}\n  template:\n    metadata: {labels: {app: web}}\n    spec: {containers: [{name: web, image: web:1}]}\n"
	fixtures := map[string]struct {
		dir   string
		files rendertest.MapFS
	}{
		"overlay with base, prefix and generators": {dir: "app", files: rendertest.MapFS{
			"app/kustomization.yaml": "namePrefix: p-\nresources: [../base, svc.yaml, secret.yaml]\n" +
				"configMapGenerator:\n- {name: cfg, files: [conf/init.sh], literals: [a=b]}\n" +
				"secretGenerator:\n- {name: gen, files: [private/pw], literals: [x=y]}\n" +
				"generatorOptions: {disableNameSuffixHash: true}\n",
			"app/svc.yaml":            "apiVersion: v1\nkind: Service\nmetadata: {name: web}\nspec: {ports: [{port: 80}]}\n",
			"app/secret.yaml":         "apiVersion: v1\nkind: Secret\nmetadata: {name: raw, annotations: {k: v}}\nstringData: {token: hunter2}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: beside}\n",
			"app/conf/init.sh":        "echo hi\n",
			"app/private/pw":          "pw",
			"base/kustomization.yaml": "resources: [deploy.yaml]\ncommonLabels: {tier: web}\n",
			"base/deploy.yaml":        deployment,
		}},
		"secretGenerator merge and replace on a base Secret": {dir: "app", files: rendertest.MapFS{
			"app/kustomization.yaml": "resources: [../base]\nsecretGenerator:\n- {name: m, behavior: merge, literals: [c=d]}\n- {name: r, behavior: replace, literals: [e=f]}\n" +
				"generatorOptions: {disableNameSuffixHash: true}\n",
			"base/kustomization.yaml": "resources: [secret.yaml]\n",
			"base/secret.yaml":        "apiVersion: v1\nkind: Secret\nmetadata: {name: m}\nstringData: {a: b}\n---\napiVersion: v1\nkind: Secret\nmetadata: {name: r}\nstringData: {a: b}\n",
		}},
		"namespace and images transformers": {dir: "k", files: rendertest.MapFS{
			"k/kustomization.yaml": "namespace: demo\nresources: [d.yaml]\nimages: [{name: web, newTag: \"2\"}]\nreplicas: [{name: web, count: 3}]\n",
			"k/d.yaml":             deployment,
		}},
	}
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			root := commitFiles(t, fixture.files)
			rev := strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD"))
			src := kustomizeSource(t, fixture.dir)
			got, err := (rendered.Source{Repository: root, Deployment: src, Catalog: catalog(t)}).RawManifests(context.Background(), rev, "demo")
			if err != nil {
				t.Fatal(err)
			}
			res, err := catalog(t).Render(context.Background(), fixture.files, render.Env{Namespace: "demo"}, src.GetRendererId(), src.GetConfig())
			if err != nil {
				t.Fatal(err)
			}
			want := res.Desired()
			if len(got) != len(want) {
				t.Fatalf("Source read %d objects, renderer %d", len(got), len(want))
			}
			for i := range want {
				if want[i].GetNamespace() == "" && !rendered.ClusterScopedKind(want[i].GetKind()) {
					want[i].SetNamespace("demo")
				}
				a, _ := json.Marshal(got[i].Object)
				b, _ := json.Marshal(want[i].Object)
				if string(a) != string(b) {
					t.Errorf("object %d differs\nsource:   %s\nrenderer: %s", i, a, b)
				}
				if strings.Contains(string(a), "hunter2") || strings.Contains(string(a), "x=y") {
					t.Errorf("Secret value reached a read: %s", a)
				}
			}
		})
	}
}

func commitFiles(t *testing.T, files rendertest.MapFS) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "repo")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, root)
	for file, body := range files {
		full := filepath.Join(root, file)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "synthetic")
	return root
}

func TestDesiredRejectsDuplicatesAcrossPhasesAndMarksForeignObjects(t *testing.T) {
	c := render.NewCatalog()
	object := func(kind, namespace, name string) *unstructured.Unstructured {
		o := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": kind, "metadata": map[string]any{"name": name}}}
		if namespace != "" {
			o.SetNamespace(namespace)
		}
		return o
	}
	var phases []render.Phase
	if err := render.Register(c, "toy", render.RendererFunc[*contracts.RawRenderConfig](func(context.Context, render.FS, render.Env, *contracts.RawRenderConfig) (render.Result, error) {
		return render.Result{Phases: phases}, nil
	})); err != nil {
		t.Fatal(err)
	}
	config, _ := anypb.New(&contracts.RawRenderConfig{})
	src := &contracts.DeploymentSource{RendererId: "toy", Config: config}
	env := render.Env{Namespace: "app"}

	phases = []render.Phase{
		{Name: "setup", Objects: []*unstructured.Unstructured{object("ConfigMap", "", "a")}},
		{Name: "default", Objects: []*unstructured.Unstructured{object("ConfigMap", "app", "a")}},
	}
	if _, err := rendered.Desired(context.Background(), rendertest.MapFS{}, env, c, src); !errors.Is(err, rendered.ErrSourceInvalid) {
		t.Fatalf("duplicate across phases = %v, want invalid", err)
	}
	phases = []render.Phase{{Name: "default", Objects: []*unstructured.Unstructured{
		object("ConfigMap", "", "a"), object("Service", "other", "peer"), object("ClusterRole", "app", "reader"),
	}}}
	set, err := rendered.Desired(context.Background(), rendertest.MapFS{}, env, c, src)
	if err != nil {
		t.Fatal(err)
	}
	var foreign []string
	for _, o := range set.Objects() {
		if o.Foreign {
			foreign = append(foreign, o.Ref.Name)
		}
	}
	if !reflect.DeepEqual(foreign, []string{"peer"}) {
		t.Fatalf("foreign = %v, want [peer]", foreign)
	}
	if _, err := rendered.Desired(context.Background(), rendertest.MapFS{}, env, c, nil); !errors.Is(err, rendered.ErrSourceUnconfigured) {
		t.Fatalf("nil source = %v", err)
	}
	unknown := proto.Clone(src).(*contracts.DeploymentSource)
	unknown.RendererId = "absent"
	if _, err := rendered.Desired(context.Background(), rendertest.MapFS{}, env, c, unknown); !errors.Is(err, rendered.ErrSourceInvalid) {
		t.Fatalf("unknown renderer = %v", err)
	}
}

// TestRenderDigestIgnoresSecretModeAndTracksObjects: deploy (Include) and
// reads (Omit) of one source agree on the render digest; any object change
// moves it. The env digest ignores SecretMode only.
func TestRenderDigestIgnoresSecretModeAndTracksObjects(t *testing.T) {
	files := rendertest.MapFS{
		"k8s/s.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: s}\nstringData: {k: v}\n",
		"k8s/d.yaml": webDeployment,
	}
	digest := func(files rendertest.MapFS, env render.Env) rendered.DesiredSet {
		t.Helper()
		set, err := rendered.Desired(context.Background(), files, env, catalog(t), rawSource(t, "k8s"))
		if err != nil {
			t.Fatal(err)
		}
		return set
	}
	omit := digest(files, render.Env{Namespace: "app", Secrets: render.SecretsOmit})
	include := digest(files, render.Env{Namespace: "app", Secrets: render.SecretsInclude})
	if omit.RenderDigest != include.RenderDigest || omit.EnvDigest != include.EnvDigest {
		t.Fatalf("Omit and Include digests differ: %+v %+v", omit, include)
	}
	changed := rendertest.MapFS{"k8s/s.yaml": files["k8s/s.yaml"], "k8s/d.yaml": strings.Replace(webDeployment, "replicas: 2", "replicas: 3", 1)}
	if digest(changed, render.Env{Namespace: "app"}).RenderDigest == omit.RenderDigest {
		t.Fatal("an object change kept the render digest")
	}
	if digest(files, render.Env{Namespace: "other"}).EnvDigest == omit.EnvDigest {
		t.Fatal("a namespace change kept the env digest")
	}
}

// TestRenderDigestAgreesAcrossModesForGeneratedSecrets: kustomize drops a
// generated Secret's type in SecretsOmit but keeps it in SecretsInclude; the
// digest covers Secret identity only, so deploy and reads still agree.
func TestRenderDigestAgreesAcrossModesForGeneratedSecrets(t *testing.T) {
	files := rendertest.MapFS{
		"app/kustomization.yaml": "secretGenerator:\n- {name: gen, literals: [x=y]}\ngeneratorOptions: {disableNameSuffixHash: true}\n",
	}
	set := func(mode render.SecretMode) rendered.DesiredSet {
		t.Helper()
		out, err := rendered.Desired(context.Background(), files, render.Env{Namespace: "app", Secrets: mode}, catalog(t), kustomizeSource(t, "app"))
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if omit, include := set(render.SecretsOmit), set(render.SecretsInclude); omit.RenderDigest != include.RenderDigest {
		t.Fatalf("generated Secret digests differ between modes: %s vs %s", omit.RenderDigest, include.RenderDigest)
	}
}

// TestRendererReadFailureStaysUnavailable: a renderer whose Git-backed read
// fails mid-render is transient, not an invalid source.
func TestRendererReadFailureStaysUnavailable(t *testing.T) {
	gone := treeGit(rendertest.MapFS{"k8s/d.yaml": webDeployment})
	failingBlobs := func(ctx context.Context, dir string, args ...string) ([]byte, error) {
		if args[0] == "cat-file" && args[1] == "blob" {
			return nil, errors.New("object store went away")
		}
		return gone(ctx, dir, args...)
	}
	_, err := (rendered.Source{Repository: "/synthetic", Deployment: rawSource(t, "k8s"), Catalog: catalog(t), Git: failingBlobs}).RawManifests(context.Background(), syntheticCommit, "app")
	if !errors.Is(err, rendered.ErrSourceUnavailable) {
		t.Fatalf("mid-read failure = %v, want unavailable", err)
	}
}

// A read failure inside a kustomize render keeps its cause: a transient Git
// failure on a referenced resource or generator input stays Unavailable.
func TestKustomizeReadFailureKeepsItsKind(t *testing.T) {
	files := rendertest.MapFS{
		"app/kustomization.yaml": "resources: [svc.yaml]\nconfigMapGenerator:\n- {name: c, files: [conf/init.sh]}\n",
		"app/svc.yaml":           "apiVersion: v1\nkind: Service\nmetadata: {name: web}\n",
		"app/conf/init.sh":       "echo hi\n",
	}
	store := treeGit(files)
	for _, failing := range []string{"app/svc.yaml", "app/conf/init.sh"} {
		t.Run(failing, func(t *testing.T) {
			blob, _ := files.Blob(failing)
			git := func(ctx context.Context, dir string, args ...string) ([]byte, error) {
				if len(args) == 3 && args[0] == "cat-file" && args[1] == "blob" && args[2] == blob {
					return nil, errors.New("git down")
				}
				return store(ctx, dir, args...)
			}
			_, err := (rendered.Source{Repository: "/synthetic", Deployment: kustomizeSource(t, "app"), Catalog: catalog(t), Git: git}).RawManifests(context.Background(), syntheticCommit, "app")
			if !errors.Is(err, rendered.ErrSourceUnavailable) {
				t.Fatalf("transient read failure = %v, want unavailable", err)
			}
		})
	}
}

func TestReadsKeepTheSourceImageTagWithoutPins(t *testing.T) {
	source := rendered.Source{Repository: "/synthetic", Deployment: rawSource(t, "loose.yaml"), Catalog: catalog(t), Git: treeGit(rawTree)}
	if image := webImage(t, source); image != "example/web:1" {
		t.Fatalf("read image = %v, want the source tag example/web:1", image)
	}
}

var webRef = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}

func webImage(t *testing.T, source rendered.Source) any {
	t.Helper()
	object, err := source.AcceptedManifest(context.Background(), syntheticCommit, webRef)
	if err != nil {
		t.Fatal(err)
	}
	containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
	return containers[0].(map[string]any)["image"]
}

type failingPins struct{}

func (failingPins) AcceptedImagePins(context.Context) (rendered.ReceiptPins, error) {
	return rendered.ReceiptPins{}, errors.New("ledger unreadable")
}

// Desired state includes the deployed image: with the pins of the same
// effective tree, every reader of the source sees the applied digest for the
// pinned container; pins of another tree, container or role change nothing;
// the render digest is the same either way; an unreadable pin ledger fails the
// read instead of falling back to the tag (which would report false drift).
func TestReadsReturnTheReceiptPinsForTheSameEffectiveTree(t *testing.T) {
	const digest = "example/web@sha256:" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	read := func(pins rendered.PinProvider) rendered.Source {
		return rendered.Source{Repository: "/synthetic", Deployment: rawSource(t, "loose.yaml"), Catalog: catalog(t), Git: treeGit(rawTree), Pins: pins}
	}
	object, err := read(nil).AcceptedManifest(context.Background(), syntheticCommit, webRef)
	if err != nil {
		t.Fatal(err)
	}
	containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
	name, _ := containers[0].(map[string]any)["name"].(string)
	first := webImage(t, read(nil))
	pin := func(container string, init bool) []rendered.DeployedImage {
		return []rendered.DeployedImage{{Ref: webRef, Name: container, Init: init, Applied: digest, Digest: digest}}
	}
	if got := webImage(t, read(rendered.ReceiptPins{EffectiveTree: syntheticTree, Images: pin(name, false)})); got != digest {
		t.Fatalf("pinned read image = %v, want %s", got, digest)
	}
	// The cache-backed source every controller reader shares sees it too.
	if got := webImage(t, read(rendered.ReceiptPins{EffectiveTree: syntheticTree, Images: pin(name, false)}).WithCache()); got != digest {
		t.Fatalf("cached pinned read image = %v, want %s", got, digest)
	}
	for label, pins := range map[string]rendered.ReceiptPins{
		"another tree":      {EffectiveTree: "ffffffffffffffffffffffffffffffffffffffff", Images: pin(name, false)},
		"another container": {EffectiveTree: syntheticTree, Images: pin("sidecar", false)},
		"init role":         {EffectiveTree: syntheticTree, Images: pin(name, true)},
		"no tree":           {Images: pin(name, false)},
	} {
		if got := webImage(t, read(pins)); got != first {
			t.Errorf("%s: image = %v, want the source image %v", label, got, first)
		}
	}
	if _, err := read(failingPins{}).AcceptedManifest(context.Background(), syntheticCommit, webRef); !errors.Is(err, rendered.ErrSourceUnavailable) {
		t.Fatalf("pin ledger failure = %v, want unavailable", err)
	}
	pinned := read(rendered.ReceiptPins{EffectiveTree: syntheticTree, Images: pin(name, false)})
	a, errA := pinned.ShapeDigest(context.Background(), syntheticCommit, "app")
	b, errB := read(nil).ShapeDigest(context.Background(), syntheticCommit, "app")
	if errA != nil || errB != nil || a != b {
		t.Fatalf("shape digest moved with pins: %q %q (%v %v)", a, b, errA, errB)
	}
}
