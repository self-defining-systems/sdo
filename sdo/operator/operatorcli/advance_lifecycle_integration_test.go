//go:build integration

package operatorcli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"gopkg.in/yaml.v3"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/upstream"
)

// The upstream-advance end-to-end check without a cluster: a synthetic chart
// whose Job is named after a hash of its images is onboarded from a provided
// plan, upstream then releases a revision that only bumps image tags (and so
// renames the Job), and the production Advancer (real broker with its
// validation subprocess, real helm render, real Deployer over a fake cluster)
// accepts it with no agent task. The contract's common case (operational
// contract plan, section 5) is zero agent turns for an image-only release.
//
// Scripted, as in the onboarding check: the health judge of the onboarding
// itself, the live checks and the independent health verdict. The plan is the
// one the lane builds: Go proposes its release-independent entries
// (rendered.SummarizeWorkloads) from the baseline render.

const advChartNamespace = "onb-chart"

var advChartApp = func() onbApp {
	app := onbToy
	app.name, app.namespace = "onb-chart", advChartNamespace
	app.workloads = []string{"Deployment/" + advChartNamespace + "/web"}
	return app
}()

func advChartFiles(webTag, migrateTag string) map[string]string {
	return map[string]string{
		"chart/Chart.yaml":  "apiVersion: v2\nname: synthetic\nversion: 0.1.0\n",
		"chart/values.yaml": fmt.Sprintf("webTag: %q\nmigrateTag: %q\n", webTag, migrateTag),
		"chart/templates/app.yaml": `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec:
  replicas: 1
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      containers:
        - name: web
          image: registry.synthetic.invalid/web:{{ .Values.webTag }}
          ports: [{containerPort: 8080}]
---
apiVersion: v1
kind: Service
metadata: {name: web}
spec:
  selector: {app: web}
  ports: [{port: 8080}]
---
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ printf "migrate-%s" (printf "%s-%s" .Values.webTag .Values.migrateTag | sha256sum | trunc 7) }}
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: registry.synthetic.invalid/migrate:{{ .Values.migrateTag }}
`,
	}
}

func advWrite(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// advCluster is the Applier of a cluster that observes every workload it was
// given: ready, at the stamped revision and the applied images.
type advCluster struct {
	live map[string]deployment.WorkloadObservation
}

func (c *advCluster) Apply(_ context.Context, documents []byte) error {
	decoder := yaml.NewDecoder(bytes.NewReader(documents))
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		kind, _ := document["kind"].(string)
		if !rendered.WorkloadKind(kind) {
			continue
		}
		meta, _ := document["metadata"].(map[string]any)
		namespace, _ := meta["namespace"].(string)
		name, _ := meta["name"].(string)
		id := kind + "/" + namespace + "/" + name
		images, err := deployment.Stamp(document, "", func(site deployment.ImageSite) string { return site.Image })
		if err != nil {
			return err
		}
		annotations, _ := meta["annotations"].(map[string]any)
		revision, _ := annotations["sdo.dev/source-tree"].(string)
		if c.live == nil {
			c.live = map[string]deployment.WorkloadObservation{}
		}
		c.live[id] = deployment.WorkloadObservation{ResourceID: id, Revision: revision, Images: images, Ready: true}
	}
}

func (c *advCluster) Observe(_ context.Context, id string) (deployment.WorkloadObservation, error) {
	observed, ok := c.live[id]
	if !ok {
		return deployment.WorkloadObservation{}, fmt.Errorf("workload %s not found", id)
	}
	return observed, nil
}

// advReadiness is the real Deployer (offline checks, render, image resolution,
// apply, observation) over the fake cluster, with scripted live checks.
type advReadiness struct{ deployment.Deployer }

func (advReadiness) CheckLive(context.Context, string, *contracts.DeploymentPlan, lifecycle.DeploymentReceipt, error) ([]operability.Check, error) {
	return onbReadiness{}.CheckLive(context.Background(), "", nil, lifecycle.DeploymentReceipt{}, nil)
}

type advMaintenance struct{}

func (advMaintenance) Pause(context.Context, string) error        { return nil }
func (advMaintenance) Resume(context.Context) error               { return nil }
func (advMaintenance) Busy(context.Context) (string, bool, error) { return "", false, nil }

// advRegistry is the prebuilt source's registry: it pins any tag to a digest.
type advRegistry struct{ fake *imagesrctest.Fake }

func (r advRegistry) Resolve(ctx context.Context, req imagesrc.Request, _ *contracts.PrebuiltImageConfig) (imagesrc.Resolved, error) {
	return r.fake.Resolve(ctx, req, &emptypb.Empty{})
}

func advImages(t *testing.T) *imagesrc.Catalog {
	t.Helper()
	catalog := imagesrc.NewCatalog()
	if err := imagesrc.Register[*contracts.PrebuiltImageConfig](catalog, "prebuilt", advRegistry{&imagesrctest.Fake{}}, imagesrc.Options{}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetDefault("prebuilt"); err != nil {
		t.Fatal(err)
	}
	return catalog
}

// advPlan is the provided plan Go proposes for the baseline render: stable
// workload identities and image repositories, never this release's Job name or
// tags.
func advPlan(t *testing.T, root string, source *contracts.DeploymentSource) *contracts.DeploymentPlan {
	t.Helper()
	reader := rendered.Source{Repository: root, Deployment: source, Catalog: renderers.Catalog()}
	inventory, err := reader.AcceptedInventory(context.Background(), strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD")), advChartNamespace)
	if err != nil {
		t.Fatal(err)
	}
	summary := rendered.SummarizeWorkloads(inventory.Objects, advChartNamespace)
	plan := &contracts.DeploymentPlan{Source: source, ExpectedWorkloadIds: summary.Identities}
	prebuilt, err := anypb.New(&contracts.PrebuiltImageConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, repository := range summary.Repositories {
		plan.Images = append(plan.Images, &contracts.ImageSource{Image: repository, SourceId: "prebuilt", Config: prebuilt})
	}
	return plan
}

func agentJobs(t *testing.T, o onbOutcome) []batchv1.Job {
	t.Helper()
	jobs, err := o.client.BatchV1().Jobs(onbControl).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return jobs.Items
}

func TestAdvanceOfAnImageOnlyReleaseIsAcceptedWithoutAgentTurns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	upstreamDir := filepath.Join(t.TempDir(), "upstream")
	if err := os.MkdirAll(upstreamDir, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, upstreamDir)
	testutil.Git(t, upstreamDir, "checkout", "-q", "-B", "main")
	advWrite(t, upstreamDir, advChartFiles("1", "1"))
	testutil.Git(t, upstreamDir, "add", ".")
	testutil.Git(t, upstreamDir, "commit", "-qm", "release 1")
	release1 := strings.TrimSpace(testutil.Git(t, upstreamDir, "rev-parse", "HEAD"))

	config, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "chart"})
	if err != nil {
		t.Fatal(err)
	}
	source := &contracts.DeploymentSource{RendererId: "helm", Config: config}
	plan := advPlan(t, upstreamDir, source)
	if !strings.HasPrefix(strings.Join(plan.GetExpectedWorkloadIds(), ","), "Deployment/"+advChartNamespace+"/web,Job/"+advChartNamespace+"/migrate-*") {
		t.Fatalf("the proposed plan pins release-specific workload names: %v", plan.GetExpectedWorkloadIds())
	}
	for _, image := range plan.GetImages() {
		if imagesrc.Repository(image.GetImage()) != image.GetImage() {
			t.Fatalf("the proposed plan pins the tag of %s", image.GetImage())
		}
	}

	cluster := &advCluster{}
	var deployer deployment.Deployer
	readiness := func(root string) lifecycle.SourceReadiness {
		deployer = deployment.Deployer{
			Namespace: advChartNamespace, Renderers: renderers.Catalog(), Images: advImages(t), Applier: cluster,
			TreeDigest: broker.ProposalTreeDigest, AcceptedRoot: root,
		}
		return advReadiness{deployer}
	}
	var accepted lifecycle.SourceReadiness
	o := onboardWith(t, advChartApp, mustBuiltin(t, telemetrysources.NoneProfileName, onbControl), onbOptions{
		prepare: func(t *testing.T, root string) {
			testutil.Git(t, filepath.Dir(root), "clone", "-q", upstreamDir, root)
			testutil.ConfigureGit(t, root)
			testutil.Git(t, root, "remote", "remove", "origin")
			accepted = readiness(root)
		},
		plan:      plan,
		readiness: lazyReadiness{&accepted},
		sources:   []layout.Source{{Path: ".", Kind: "git", Config: map[string]any{"repository": upstreamDir, "ref": "main"}}},
	}, advChartApp.goodProposal())
	if o.err != nil {
		t.Fatalf("onboarding the chart from its provided plan: %v", o.err)
	}
	onboardingJobs := len(agentJobs(t, o))

	// Upstream releases a revision that bumps both image tags only.
	advWrite(t, upstreamDir, advChartFiles("2", "2"))
	testutil.Git(t, upstreamDir, "commit", "-qam", "release 2: images only")
	release2 := strings.TrimSpace(testutil.Git(t, upstreamDir, "rev-parse", "HEAD"))

	catalog := upstreams.Catalog()
	sources, err := memory.ReadSources(o.accepted, catalog)
	if err != nil || len(sources) != 1 {
		t.Fatalf("accepted sources = %v, %v", sources, err)
	}
	prev, found, err := o.repository.LastUpstream(ctx)
	if err != nil || !found || prev != release1 {
		t.Fatalf("accepted upstream = %q found %v err %v, want %s", prev, found, err, release1)
	}
	latest, err := catalog.Latest(ctx, sources[0].Kind, sources[0].Config)
	if err != nil || latest.ID != release2 {
		t.Fatalf("latest = %+v, %v; want %s", latest, err, release2)
	}
	advancer := lifecycle.Advancer{
		Upstreams: catalog, Broker: o.repository, Readiness: advReadiness{deployer},
		Verifier: onbVerifier{},
		Shape: func(ctx context.Context, root, revision string, source *contracts.DeploymentSource) (string, error) {
			return rendered.Source{Deployment: source, Catalog: renderers.Catalog(), Repository: root}.ShapeDigest(ctx, revision, advChartNamespace)
		},
		Maintenance: advMaintenance{}, Activity: advMaintenance{},
		Wait: func(context.Context, time.Duration) error { return nil },
	}
	result, err := advancer.Advance(ctx, lifecycle.AdvanceConfig{
		Namespace: advChartNamespace, HealthObjective: "every synthetic workload stays available", GitDir: filepath.Join(o.accepted, ".git"),
		PendingEvaluations: 1, InfrastructureTries: 1,
	}, upstream.Request{Source: sources[0], Revision: latest, Prev: prev})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Outcome != lifecycle.AdvanceAccepted {
		t.Fatalf("advance = %s hold %s (cause %s owner %s items %v): %s", result.Outcome, result.Hold, result.Cause, result.Owner, result.Items, result.Detail)
	}
	if result.Owed != nil {
		t.Errorf("an image-only release owes the designer a refresh: %+v", result.Owed)
	}
	if got := len(agentJobs(t, o)); got != onboardingJobs {
		t.Errorf("agent Jobs after the advance = %d, after onboarding %d: an image-only release must not start an agent", got, onboardingJobs)
	}
	head := strings.TrimSpace(testutil.Git(t, o.accepted, "rev-parse", "HEAD"))
	if head != result.Receipt.Commit {
		t.Errorf("accepted HEAD %s is not the advance commit %s", head, result.Receipt.Commit)
	}
	// The cluster runs release 2: the renamed Job and the bumped images.
	if len(cluster.live) < 2 {
		t.Fatalf("observed workloads = %v", cluster.live)
	}
	for id, observed := range cluster.live {
		if strings.HasPrefix(id, "Job/") {
			for _, image := range observed.Images {
				if !strings.Contains(image, ":2") && !strings.Contains(image, "@sha256:") {
					t.Errorf("%s runs %v, not release 2", id, observed.Images)
				}
			}
		}
	}
	if last, _, err := o.repository.LastUpstream(ctx); err != nil || last != release2 {
		t.Errorf("accepted upstream after the advance = %q, %v; want %s", last, err, release2)
	}
}

// lazyReadiness defers to the Deployer built once the repository exists.
type lazyReadiness struct{ target *lifecycle.SourceReadiness }

func (l lazyReadiness) ApplyAndVerify(ctx context.Context, checkout string, plan *contracts.DeploymentPlan) (lifecycle.DeploymentReceipt, error) {
	return (*l.target).ApplyAndVerify(ctx, checkout, plan)
}

func (l lazyReadiness) VerifyAccepted(ctx context.Context, commit, namespace string, prior lifecycle.DeploymentReceipt) (lifecycle.DeploymentReceipt, error) {
	return (*l.target).VerifyAccepted(ctx, commit, namespace, prior)
}

func (l lazyReadiness) CheckLive(ctx context.Context, checkout string, plan *contracts.DeploymentPlan, receipt lifecycle.DeploymentReceipt, outcome error) ([]operability.Check, error) {
	return (*l.target).CheckLive(ctx, checkout, plan, receipt, outcome)
}

func (l lazyReadiness) CheckOffline(ctx context.Context, checkout string, plan *contracts.DeploymentPlan) (lifecycle.OfflineChecks, error) {
	return (*l.target).(lifecycle.OfflineChecker).CheckOffline(ctx, checkout, plan)
}
