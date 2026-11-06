package deployment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm"
	"sdo.dev/operator/render/kustomize"
	"sdo.dev/operator/render/raw"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
)

// clusterFake is a faithful Applier: it records what it is asked to apply and
// observes workloads from the documents it received (images in
// init-then-regular order, revision from the source-tree stamp).
type clusterFake struct {
	events   []string
	applied  []map[string]any
	live     map[string]WorkloadObservation
	applyErr error
	notReady map[string]bool
	// jobsInFlight is what each Observe was told about unfinished Jobs.
	jobsInFlight map[string][]string
}

func (c *clusterFake) Apply(_ context.Context, documents []byte) error {
	if c.applyErr != nil {
		return c.applyErr
	}
	decoder := yaml.NewDecoder(bytes.NewReader(documents))
	var names []string
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		c.applied = append(c.applied, document)
		meta, _ := document["metadata"].(map[string]any)
		kind, _ := document["kind"].(string)
		name, _ := meta["name"].(string)
		names = append(names, kind+"/"+name)
		if rendered.WorkloadKind(kind) {
			namespace, _ := meta["namespace"].(string)
			id := kind + "/" + namespace + "/" + name
			images, _ := Stamp(document, "", func(site ImageSite) string { return site.Image })
			annotations, _ := meta["annotations"].(map[string]any)
			revision, _ := annotations["sdo.dev/source-tree"].(string)
			if c.live == nil {
				c.live = map[string]WorkloadObservation{}
			}
			c.live[id] = WorkloadObservation{ResourceID: id, Revision: revision, Images: images, Ready: !c.notReady[id]}
		}
	}
	c.events = append(c.events, "apply "+strings.Join(names, ","))
	return nil
}

func (c *clusterFake) Observe(ctx context.Context, id string) (WorkloadObservation, error) {
	c.events = append(c.events, "observe "+id)
	if c.jobsInFlight == nil {
		c.jobsInFlight = map[string][]string{}
	}
	c.jobsInFlight[id], _ = ctx.Value(jobsInFlightKey{}).([]string)
	observed, ok := c.live[id]
	if !ok {
		return WorkloadObservation{}, fmt.Errorf("workload %s not found", id)
	}
	return observed, nil
}

func depCommit(t *testing.T, files rendertest.MapFS) string {
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

func depRenderers(t *testing.T) *render.Catalog {
	t.Helper()
	c := render.NewCatalog()
	for _, register := range []func(*render.Catalog) error{raw.Register, kustomize.Register, registerHelm, registerPhased} {
		if err := register(c); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

const depImages = "prebuilt"

func depImageCatalog(t *testing.T) (*imagesrc.Catalog, *imagesrctest.Fake) {
	t.Helper()
	fake := &imagesrctest.Fake{}
	c := imagesrc.NewCatalog()
	if err := imagesrc.Register[*emptypb.Empty](c, depImages, fake, imagesrc.Options{}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetDefault(depImages); err != nil {
		t.Fatal(err)
	}
	return c, fake
}

func depAny(t *testing.T, m interface {
	ProtoReflect() protoreflect.Message
}) *anypb.Any {
	t.Helper()
	a, err := anypb.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func depSource(t *testing.T, id string, config proto.Message) *contracts.DeploymentSource {
	t.Helper()
	return &contracts.DeploymentSource{RendererId: id, Config: depAny(t, config)}
}

const depDeployment = `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec:
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      initContainers: [{name: init, image: "busybox:1"}]
      containers: [{name: web, image: "web:1"}, {name: side, image: "web:1"}]
`

// registerPhased registers a renderer whose first phase is not gated.
func registerPhased(c *render.Catalog) error {
	object := func(kind, name, image string) *unstructured.Unstructured {
		o := map[string]any{"apiVersion": "v1", "kind": kind, "metadata": map[string]any{"name": name}}
		if kind == "Deployment" {
			o["apiVersion"] = "apps/v1"
			o["spec"] = map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"app": name}}, "template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": name}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": name, "image": image}}},
			}}
		}
		return &unstructured.Unstructured{Object: o}
	}
	workload := func(kind, name string) *unstructured.Unstructured {
		o := object("Deployment", name, name+":1")
		o.SetKind(kind)
		if kind == "Job" {
			o.SetAPIVersion("batch/v1")
			o.Object["spec"] = map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": name, "image": name + ":1"}}}}}
		}
		return o
	}
	if err := render.Register[*wrapperspb.Int32Value](c, "jobphased", render.RendererFunc[*wrapperspb.Int32Value](
		func(context.Context, render.FS, render.Env, *wrapperspb.Int32Value) (render.Result, error) {
			return render.Result{Phases: []render.Phase{
				{Name: "first", Gate: render.GateNone, Objects: []*unstructured.Unstructured{workload("Job", "migrate"), workload("Deployment", "a")}},
				{Name: "second", Gate: render.GateReady, Objects: []*unstructured.Unstructured{workload("Deployment", "b")}},
				{Name: "third", Gate: render.GateReady, Objects: []*unstructured.Unstructured{workload("Job", "late")}},
			}}, nil
		})); err != nil {
		return err
	}
	return render.Register[*wrapperspb.StringValue](c, "phased", render.RendererFunc[*wrapperspb.StringValue](
		func(context.Context, render.FS, render.Env, *wrapperspb.StringValue) (render.Result, error) {
			return render.Result{Phases: []render.Phase{
				{Name: "first", Gate: render.GateNone, Objects: []*unstructured.Unstructured{object("ConfigMap", "cfg", ""), object("Deployment", "a", "a:1")}},
				{Name: "second", Gate: render.GateReady, Objects: []*unstructured.Unstructured{object("Deployment", "b", "b:1")}},
			}}, nil
		}))
}

type depFixture struct {
	name   string
	files  rendertest.MapFS
	source func(*testing.T) *contracts.DeploymentSource
	// beyondReads lists Kind/name of objects deploy applies that reads do not
	// show: ephemeral phases (not desired state) and foreign-namespace
	// objects (projected out of unqualified reads, D5).
	beyondReads []string
}

func depHelmTree(t *testing.T, name, prefix string, extra map[string]string) rendertest.MapFS {
	t.Helper()
	out := rendertest.MapFS{}
	root := filepath.Join("..", "render", "helm", "testdata", name)
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(p)
		rel, _ := filepath.Rel(root, p)
		out[prefix+"/"+filepath.ToSlash(rel)] = string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func depFixtures(t *testing.T) []depFixture {
	rawFiles := rendertest.MapFS{
		"k8s/web.yaml":   depDeployment,
		"k8s/svc.yaml":   "apiVersion: v1\nkind: Service\nmetadata: {name: web, namespace: other}\nspec: {ports: [{port: 80}]}\n",
		"k8s/creds.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: creds}\nstringData: {password: hunter2}\ndata: {token: eA==}\n",
		"k8s/ns.yaml":    "apiVersion: v1\nkind: Namespace\nmetadata: {name: extra, namespace: ignored}\n",
		"k8s/cm.json":    `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"a"},"data":{"n":1}}`,
		"k8s/notes.txt":  "not a manifest",
		"k8s/multi.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: m1}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: m2}\n",
	}
	kustFiles := rendertest.MapFS{
		"app/kustomization.yaml": "namePrefix: p-\nresources: [../base, extra.yaml]\n" +
			"configMapGenerator:\n- {name: cfg, files: [conf/init.sh]}\n" +
			"secretGenerator:\n- {name: gen, literals: [x=y]}\n" +
			"generatorOptions: {disableNameSuffixHash: true}\n",
		"app/extra.yaml":          "apiVersion: v1\nkind: ServiceAccount\nmetadata: {name: sa}\n",
		"app/conf/init.sh":        "echo hi\n",
		"base/kustomization.yaml": "resources: [deploy.yaml]\ncommonLabels: {tier: web}\n",
		"base/deploy.yaml":        depDeployment,
	}
	hookWeb := map[string]string{"h/templates/web.yaml": depDeployment}
	return []depFixture{
		{name: "raw", files: rawFiles, beyondReads: []string{"Service/web"}, source: func(t *testing.T) *contracts.DeploymentSource {
			return depSource(t, raw.ID, &contracts.RawRenderConfig{Paths: []string{"k8s"}})
		}},
		{name: "kustomize", files: kustFiles, source: func(t *testing.T) *contracts.DeploymentSource {
			return depSource(t, kustomize.ID, &contracts.KustomizeRenderConfig{Path: "app"})
		}},
		{name: "helm", files: depHelmTree(t, "app", "chart", nil), source: func(t *testing.T) *contracts.DeploymentSource {
			return depSource(t, helm.ID, &contracts.HelmRenderConfig{ChartPath: "chart"})
		}},
		{name: "helm hooks", files: depHelmTree(t, "hooks", "h", hookWeb), beyondReads: []string{"Job/migrate", "ConfigMap/seed", "Job/smoke"},
			source: func(t *testing.T) *contracts.DeploymentSource {
				return depSource(t, helm.ID, &contracts.HelmRenderConfig{ChartPath: "h"})
			}},
	}
}

const depNamespace = "demo"

func depDeployer(t *testing.T, root string, cluster *clusterFake) (Deployer, *imagesrctest.Fake) {
	t.Helper()
	images, fake := depImageCatalog(t)
	return Deployer{
		Namespace: depNamespace, Renderers: depRenderers(t), Images: images, Applier: cluster,
		TreeDigest: broker.ProposalTreeDigest, AcceptedRoot: root,
	}, fake
}

func depHead(t *testing.T, root string) string {
	return testutil.Git(t, root, "rev-parse", "HEAD")
}

func depSourceReader(root string, src *contracts.DeploymentSource, environment *contracts.RenderEnvironment, c *render.Catalog) rendered.Source {
	return rendered.Source{Repository: root, Deployment: src, Catalog: c, Environment: environment}
}

func depKey(o map[string]any) string {
	meta, _ := o["metadata"].(map[string]any)
	kind, _ := o["kind"].(string)
	namespace, _ := meta["namespace"].(string)
	name, _ := meta["name"].(string)
	return kind + " " + namespace + "/" + name
}

// depCanonical renders an object comparable: Secrets reduced to identity (reads
// omit their values), JSON-encoded with sorted keys.
func depCanonical(t *testing.T, o map[string]any) string {
	t.Helper()
	data, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	var copy map[string]any
	if err := json.Unmarshal(data, &copy); err != nil {
		t.Fatal(err)
	}
	if copy["kind"] == "Secret" {
		// Reads carry Secret identity only; renderers differ on what else
		// SecretsOmit drops (kustomize drops type).
		meta, _ := copy["metadata"].(map[string]any)
		copy = map[string]any{"apiVersion": copy["apiVersion"], "kind": "Secret", "metadata": map[string]any{"name": meta["name"], "namespace": meta["namespace"]}}
	}
	out, err := json.Marshal(copy)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// depUnstamp reverses Stamp on an applied document: the source-tree
// annotations and the substituted images.
func depUnstamp(o map[string]any, original map[string]string) {
	drop := func(meta map[string]any) {
		annotations, _ := meta["annotations"].(map[string]any)
		delete(annotations, "sdo.dev/source-tree")
		if len(annotations) == 0 {
			delete(meta, "annotations")
		}
	}
	if meta, ok := o["metadata"].(map[string]any); ok {
		drop(meta)
	}
	kind, _ := o["kind"].(string)
	if !rendered.WorkloadKind(kind) {
		return
	}
	template := o["spec"].(map[string]any)["template"].(map[string]any)
	if meta, ok := template["metadata"].(map[string]any); ok {
		drop(meta)
		if len(meta) == 0 {
			delete(template, "metadata")
		}
	}
	pod := template["spec"].(map[string]any)
	for _, field := range []string{"initContainers", "containers"} {
		containers, _ := pod[field].([]any)
		for _, raw := range containers {
			container := raw.(map[string]any)
			if image, ok := original[container["image"].(string)]; ok {
				container["image"] = image
			}
		}
	}
}

// TestDeployPathObjectsEqualReadObjects is the property the design replaced by
// a single Desired: for every fixture (raw, kustomize, helm, helm with hook
// phases) the objects the deployer applies, before Stamp, are exactly the
// objects rendered.Source reads, plus ephemeral and foreign-namespace objects
// that reads project out.
func TestDeployPathObjectsEqualReadObjects(t *testing.T) {
	// nil is an environment never discovered (renderer defaults); the other is
	// a discovered one. Deploy and reads must agree under both.
	for name, environment := range map[string]*contracts.RenderEnvironment{
		"discovered": {KubeVersion: "v1.29.4", ApiVersions: []string{"apps/v1", "apps/v1/Deployment", "v1", "v1/Service"}},
		"unknown":    nil,
	} {
		for _, fixture := range depFixtures(t) {
			t.Run(fixture.name+"/env="+name, func(t *testing.T) {
				ctx := context.Background()
				root := depCommit(t, fixture.files)
				source := fixture.source(t)
				catalog := depRenderers(t)
				reads, err := depSourceReader(root, source, environment, catalog).RawManifests(ctx, depHead(t, root), depNamespace)
				if err != nil {
					t.Fatal(err)
				}
				want := map[string]string{}
				original := map[string]string{}
				var expected []string
				for _, o := range reads {
					want[depKey(o.Object)] = depCanonical(t, o.Object)
					if rendered.WorkloadKind(o.GetKind()) {
						expected = append(expected, o.GetKind()+"/"+o.GetNamespace()+"/"+o.GetName())
						_, _ = Stamp(o.DeepCopy().Object, "", func(site ImageSite) string {
							original[imagesrctest.Repository(site.Image)+"@"+imagesrctest.Digest(site.Image)] = site.Image
							return site.Image
						})
					}
				}
				cluster := &clusterFake{}
				deployer, _ := depDeployer(t, root, cluster)
				deployer.Environment = environment
				receipt, err := deployer.ApplyAndVerify(ctx, root, &contracts.DeploymentPlan{Source: source, ExpectedWorkloadIds: expected})
				if err != nil {
					t.Fatal(err)
				}
				// The Include render's digest is the Omit render's digest, so a
				// restart can verify what deploy recorded.
				if _, err := deployer.VerifyAccepted(ctx, depHead(t, root), depNamespace, receipt); err != nil {
					t.Fatalf("verify after deploy: %v", err)
				}
				got := map[string]string{}
				var extra []string
				for _, document := range cluster.applied {
					depUnstamp(document, original)
					key := depKey(document)
					if _, read := want[key]; read {
						got[key] = depCanonical(t, document)
						continue
					}
					meta := document["metadata"].(map[string]any)
					extra = append(extra, fmt.Sprint(document["kind"], "/", meta["name"]))
				}
				if len(got) != len(want) {
					t.Fatalf("deployed %d read objects, reads have %d\ngot %v\nwant %v", len(got), len(want), sortedKeys(got), sortedKeys(want))
				}
				for key, object := range want {
					if got[key] != object {
						t.Errorf("%s differs\ndeploy: %s\nread:   %s", key, got[key], object)
					}
				}
				sort.Strings(extra)
				wantExtra := append([]string(nil), fixture.beyondReads...)
				sort.Strings(wantExtra)
				if strings.Join(extra, ",") != strings.Join(wantExtra, ",") {
					t.Errorf("objects applied beyond the reads = %v, want the ephemeral and foreign-namespace set %v", extra, wantExtra)
				}
			})
		}
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func depPlan(t *testing.T, source *contracts.DeploymentSource, workloads ...string) *contracts.DeploymentPlan {
	return &contracts.DeploymentPlan{Source: source, ExpectedWorkloadIds: workloads}
}

const depRawWeb = "Deployment/demo/web"

func depRawFixture(t *testing.T) (string, *contracts.DeploymentSource) {
	root := depCommit(t, rendertest.MapFS{"k8s/web.yaml": depDeployment, "k8s/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\n"})
	return root, depSource(t, raw.ID, &contracts.RawRenderConfig{Paths: []string{"k8s"}})
}

func TestReceiptRecordsResolvedImagesAndEvidence(t *testing.T) {
	root, source := depRawFixture(t)
	cluster := &clusterFake{}
	deployer, fake := depDeployer(t, root, cluster)
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	tree := depHead(t, root)
	// No memory and no patches: the effective tree is the base tree, and the
	// workloads carry its ID as their source stamp.
	if tree = testutil.Git(t, root, "rev-parse", "HEAD^{tree}"); receipt.BaseTree != tree || receipt.EffectiveTree != tree || receipt.ObservedRevision != tree || receipt.PatchesDigest == "" {
		t.Fatalf("receipt tree %q / effective %q / %q, want %q", receipt.BaseTree, receipt.EffectiveTree, receipt.ObservedRevision, tree)
	}
	if !receipt.Ready || len(receipt.ResourceIDs) != 1 || receipt.ResourceIDs[0] != depRawWeb {
		t.Fatalf("receipt = %+v", receipt)
	}
	pinned := "web@" + imagesrctest.Digest("web:1")
	busybox := "busybox@" + imagesrctest.Digest("busybox:1")
	if strings.Join(receipt.Images, ",") != strings.Join([]string{busybox, pinned, pinned}, ",") {
		t.Fatalf("images = %v", receipt.Images)
	}
	if calls := fake.Calls(); strings.Join(calls, ",") != "busybox:1,web:1" {
		t.Fatalf("each distinct image resolves once in order, calls = %v", calls)
	}
	if receipt.RenderDigest == "" || receipt.EnvDigest == "" || receipt.Source == nil || receipt.Source == source || !proto.Equal(receipt.Source, source) {
		t.Fatalf("receipt source/digests = %v %q %q (source must be an equal clone)", receipt.Source, receipt.RenderDigest, receipt.EnvDigest)
	}
	if len(receipt.ContainerImages) != 0 {
		t.Fatalf("the fake reports no per-container identities, receipt must not invent them: %+v", receipt.ContainerImages)
	}
}

func TestDeployerRejectsPlansBeforeApplying(t *testing.T) {
	root, source := depRawFixture(t)
	for name, plan := range map[string]*contracts.DeploymentPlan{
		"no source":             {ExpectedWorkloadIds: []string{depRawWeb}},
		"no expected workloads": {Source: source},
		"unexpected workload":   depPlan(t, source, depRawWeb, "Deployment/demo/ghost"),
		"missing workload":      depPlan(t, source, "Deployment/demo/ghost"),
		"duplicate expected":    depPlan(t, source, depRawWeb, depRawWeb),
		"image declared but unused": {Source: source, ExpectedWorkloadIds: []string{depRawWeb}, Images: []*contracts.ImageSource{
			{Image: "never:1", SourceId: depImages, Config: depAny(t, &emptypb.Empty{})}}},
		"unknown renderer": {Source: &contracts.DeploymentSource{RendererId: "nope"}, ExpectedWorkloadIds: []string{depRawWeb}},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := &clusterFake{}
			deployer, _ := depDeployer(t, root, cluster)
			if _, err := deployer.ApplyAndVerify(context.Background(), root, plan); err == nil {
				t.Fatal("plan was accepted")
			}
			if len(cluster.events) != 0 {
				t.Fatalf("cluster touched before the plan was validated: %v", cluster.events)
			}
		})
	}
}

func TestImageResolutionFailureIsClassifiedAndNothingIsApplied(t *testing.T) {
	root, source := depRawFixture(t)
	cluster := &clusterFake{}
	deployer, fake := depDeployer(t, root, cluster)
	fake.Hook = func(context.Context, imagesrc.Request) error {
		return &imagesrc.ResolveError{Kind: imagesrc.ResolveTransient, Err: errors.New("rate limited")}
	}
	_, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	var resolve *imagesrc.ResolveError
	if !errors.As(err, &resolve) || resolve.Kind != imagesrc.ResolveTransient {
		t.Fatalf("err = %v, want the classified resolve error preserved", err)
	}
	if len(cluster.events) != 0 {
		t.Fatalf("applied: %v", cluster.events)
	}
}

func TestPhasesApplyInOrderAndGateReadyObservesBeforeTheNextPhase(t *testing.T) {
	root := depCommit(t, rendertest.MapFS{"x": "x"})
	source := depSource(t, "phased", &wrapperspb.StringValue{})
	cluster := &clusterFake{}
	deployer, _ := depDeployer(t, root, cluster)
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/a", "Deployment/demo/b"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"apply ConfigMap/cfg,Deployment/a", // GateNone: no observation here
		"apply Deployment/b",
		"observe Deployment/demo/b", // GateReady
		"observe Deployment/demo/a", // ungated workloads are observed before the receipt
	}
	if strings.Join(cluster.events, "|") != strings.Join(want, "|") {
		t.Fatalf("events:\n%s\nwant:\n%s", strings.Join(cluster.events, "\n"), strings.Join(want, "\n"))
	}
	if strings.Join(receipt.ResourceIDs, ",") != "Deployment/demo/a,Deployment/demo/b" {
		t.Fatalf("ResourceIDs = %v", receipt.ResourceIDs)
	}
}

func TestEphemeralPhasesGateButStayOutOfTheReceipt(t *testing.T) {
	fixtures := depFixtures(t)
	fixture := fixtures[3]
	root := depCommit(t, fixture.files)
	cluster := &clusterFake{}
	deployer, _ := depDeployer(t, root, cluster)
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, fixture.source(t), "Deployment/demo/web"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"apply ConfigMap/seed,Job/migrate", "observe Job/demo/migrate",
		"apply ConfigMap/main-cm,Deployment/web", "observe Deployment/demo/web",
		"apply Job/smoke", "observe Job/demo/smoke",
	}
	if strings.Join(cluster.events, "|") != strings.Join(want, "|") {
		t.Fatalf("events:\n%s\nwant:\n%s", strings.Join(cluster.events, "\n"), strings.Join(want, "\n"))
	}
	if strings.Join(receipt.ResourceIDs, ",") != "Deployment/demo/web" {
		t.Fatalf("ResourceIDs = %v: ephemeral workloads are not desired state", receipt.ResourceIDs)
	}
}

func TestGateReadyFailureStopsLaterPhases(t *testing.T) {
	root := depCommit(t, rendertest.MapFS{"x": "x"})
	source := depSource(t, "phased", &wrapperspb.StringValue{})
	cluster := &clusterFake{notReady: map[string]bool{"Deployment/demo/b": true}}
	deployer, _ := depDeployer(t, root, cluster)
	_, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/a", "Deployment/demo/b"))
	if err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("err = %v, want not ready", err)
	}
}

func TestApplyFailureNamesThePhase(t *testing.T) {
	root, source := depRawFixture(t)
	cluster := &clusterFake{applyErr: errors.New("boom")}
	deployer, _ := depDeployer(t, root, cluster)
	_, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err == nil || !strings.Contains(err.Error(), `phase "default"`) || !errors.Is(err, cluster.applyErr) {
		t.Fatalf("err = %v", err)
	}
}

func TestVerifyAcceptedChecksDigestsAndLiveWorkloads(t *testing.T) {
	root, source := depRawFixture(t)
	cluster := &clusterFake{}
	deployer, _ := depDeployer(t, root, cluster)
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	commit := depHead(t, root)
	ctx := context.Background()
	if got, err := deployer.VerifyAccepted(ctx, commit, depNamespace, receipt); err != nil || got.BaseTree != receipt.BaseTree {
		t.Fatalf("verify = %+v, %v", got, err)
	}

	for name, mutate := range map[string]func(*lifecycle.DeploymentReceipt){
		"render digest": func(r *lifecycle.DeploymentReceipt) { r.RenderDigest = strings.Repeat("0", 64) },
		"env digest":    func(r *lifecycle.DeploymentReceipt) { r.EnvDigest = strings.Repeat("0", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := receipt
			mutate(&changed)
			_, err := deployer.VerifyAccepted(ctx, commit, depNamespace, changed)
			var renderer *RendererChangedError
			if !errors.As(err, &renderer) || renderer.Want == renderer.Got {
				t.Fatalf("err = %v, want RendererChangedError", err)
			}
		})
	}

	t.Run("renderer environment changed", func(t *testing.T) {
		changed := receipt
		changed.Environment = &contracts.RenderEnvironment{KubeVersion: "v1.20.0"}
		_, err := deployer.VerifyAccepted(ctx, commit, depNamespace, changed)
		var renderer *RendererChangedError
		if !errors.As(err, &renderer) || renderer.What != "env" {
			t.Fatalf("err = %v, want RendererChangedError on env", err)
		}
	})
	t.Run("workload no longer at the accepted revision", func(t *testing.T) {
		cluster.live[depRawWeb] = WorkloadObservation{ResourceID: depRawWeb, Revision: "stale", Images: cluster.live[depRawWeb].Images, Ready: true}
		defer func() { cluster.live[depRawWeb] = WorkloadObservation{} }()
		if _, err := deployer.VerifyAccepted(ctx, commit, depNamespace, receipt); err == nil || !strings.Contains(err.Error(), "no longer matches") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("wrong namespace", func(t *testing.T) {
		if _, err := deployer.VerifyAccepted(ctx, commit, "elsewhere", receipt); err == nil {
			t.Fatal("namespace mismatch accepted")
		}
	})
	t.Run("evidence of another tree", func(t *testing.T) {
		changed := receipt
		changed.BaseTree = strings.Repeat("1", 40)
		if _, err := deployer.VerifyAccepted(ctx, commit, depNamespace, changed); err == nil {
			t.Fatal("foreign evidence accepted")
		}
	})
	t.Run("commit not in the accepted branch", func(t *testing.T) {
		if _, err := deployer.VerifyAccepted(ctx, strings.Repeat("2", 40), depNamespace, receipt); err == nil {
			t.Fatal("unknown commit accepted")
		}
	})
}

func registerHelm(c *render.Catalog) error { return helm.Register(c, helm.InProcess) }

// A workload's init containers may wait for a Job of the same or an earlier
// phase, so observing it tells the backend which of those Jobs are unfinished;
// a Job of a later phase is not a dependency.
func TestObserveTellsTheBackendWhichEarlierJobsAreUnfinished(t *testing.T) {
	root := depCommit(t, rendertest.MapFS{"x": "x"})
	source := depSource(t, "jobphased", &wrapperspb.Int32Value{})
	cluster := &clusterFake{}
	deployer, _ := depDeployer(t, root, cluster)
	if _, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, "Deployment/demo/a", "Deployment/demo/b", "Job/demo/late", "Job/demo/migrate")); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cluster.jobsInFlight["Deployment/demo/b"], ","); got != "Job/demo/migrate" {
		t.Fatalf("Deployment b observed with jobs %q, want only the earlier-phase Job", got)
	}
	if got := cluster.jobsInFlight["Job/demo/migrate"]; len(got) != 0 {
		t.Fatalf("a Job is never its own dependency: %v", got)
	}
}
