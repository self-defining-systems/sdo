//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sdo.dev/operator/contracts/settings"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"gopkg.in/yaml.v3"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/upstream"
)

// The immutable-field advance end to end without a cluster: a synthetic chart
// with a fixed-name Job (it completes) and a Deployment is onboarded, then
// upstream releases a revision that changes the Job's image (its template is
// immutable) and the Deployment's selector labels (immutable). The production
// Advancer and Deployer must accept it with no agent task: the finished Job is
// recreated, the Deployment is deleted with orphan propagation and recreated,
// and the orphaned pods are replaced by the new rollout.

func recreateChartFiles(seedTag, track string) map[string]string {
	return map[string]string{
		"chart/Chart.yaml":  "apiVersion: v2\nname: synthetic\nversion: 0.1.0\n",
		"chart/values.yaml": fmt.Sprintf("seedTag: %q\ntrack: %q\n", seedTag, track),
		"chart/templates/app.yaml": `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec:
  replicas: 1
  selector: {matchLabels: {app: web, track: {{ .Values.track }}}}
  template:
    metadata: {labels: {app: web, track: {{ .Values.track }}}}
    spec:
      containers:
        - name: web
          image: registry.synthetic.invalid/web:1
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
metadata: {name: seed}
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: seed
          image: registry.synthetic.invalid/seed:{{ .Values.seedTag }}
`,
	}
}

// immutableCluster is an API server and the controllers the scenario needs,
// in memory. Apply refuses an immutable change the way the API server does
// (a Job's spec.template, a workload's spec.selector); a Job completes as
// soon as it is created; the Deployment controller adopts ReplicaSets its
// selector matches, creates one per pod template and scales the others down.
type immutableCluster struct {
	seq     int
	objects map[string]*immutableObject // Kind/name, one namespace
	events  []string
}

type immutableObject struct {
	kind, name, uid string
	doc             map[string]any
	labels          map[string]string
	controller      string
	orphan          *deployment.OrphanStamp
	observation     deployment.WorkloadObservation
}

func (c *immutableCluster) uid() string { c.seq++; return fmt.Sprintf("uid-%d", c.seq) }

func decodeDocuments(documents []byte) ([]map[string]any, error) {
	var out []map[string]any
	decoder := yaml.NewDecoder(bytes.NewReader(documents))
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, err
		}
		out = append(out, document)
	}
}

func docKey(document map[string]any) (string, string, string) {
	kind, _ := document["kind"].(string)
	meta, _ := document["metadata"].(map[string]any)
	name, _ := meta["name"].(string)
	return kind + "/" + name, kind, name
}

func specField(document map[string]any, field string) any {
	spec, _ := document["spec"].(map[string]any)
	return spec[field]
}

// immutable returns the immutable field an update of live to document changes.
func immutable(live *immutableObject, document map[string]any) string {
	switch live.kind {
	case "Job":
		if !reflect.DeepEqual(specField(live.doc, "template"), specField(document, "template")) {
			return "spec.template"
		}
	case "Deployment":
		if !reflect.DeepEqual(specField(live.doc, "selector"), specField(document, "selector")) {
			return "spec.selector"
		}
	}
	return ""
}

func (c *immutableCluster) refusals(documents []map[string]any) []string {
	var out []string
	for _, document := range documents {
		key, kind, name := docKey(document)
		if live := c.objects[key]; live != nil {
			if field := immutable(live, document); field != "" {
				out = append(out, fmt.Sprintf(`Error from server (Invalid): %s "%s" is invalid: %s: Invalid value: {}: field is immutable`, kind, name, field))
			}
		}
	}
	return out
}

func (c *immutableCluster) Apply(_ context.Context, documents []byte) error {
	decoded, err := decodeDocuments(documents)
	if err != nil {
		return err
	}
	if refused := c.refusals(decoded); len(refused) > 0 {
		return fmt.Errorf("apply Kubernetes manifests: exit status 1: %s", strings.Join(refused, "\n"))
	}
	if c.objects == nil {
		c.objects = map[string]*immutableObject{}
	}
	for _, document := range decoded {
		key, kind, name := docKey(document)
		live := c.objects[key]
		if live == nil {
			live = &immutableObject{kind: kind, name: name, uid: c.uid()}
			c.objects[key] = live
			c.events = append(c.events, "create "+key)
		}
		live.doc = document
		if !rendered.WorkloadKind(kind) {
			continue
		}
		images, err := deployment.Stamp(document, "", func(site deployment.ImageSite) string { return site.Image })
		if err != nil {
			return err
		}
		meta, _ := document["metadata"].(map[string]any)
		revision := rendered.StampedTree(meta)
		live.observation = deployment.WorkloadObservation{ResourceID: kind + "/" + advChartNamespace + "/" + name, Revision: revision, Images: images, Ready: true}
		if kind == "Deployment" {
			c.reconcileDeployment(live)
		}
	}
	return nil
}

func templateLabels(document map[string]any) map[string]string {
	template, _ := specField(document, "template").(map[string]any)
	meta, _ := template["metadata"].(map[string]any)
	raw, _ := meta["labels"].(map[string]any)
	out := map[string]string{}
	for key, value := range raw {
		out[key] = fmt.Sprint(value)
	}
	return out
}

func (c *immutableCluster) reconcileDeployment(d *immutableObject) {
	selector, _ := specField(d.doc, "selector").(map[string]any)
	matchLabels, _ := selector["matchLabels"].(map[string]any)
	matches := func(labels map[string]string) bool {
		for key, value := range matchLabels {
			if labels[key] != fmt.Sprint(value) {
				return false
			}
		}
		return true
	}
	encoded, _ := json.Marshal(specField(d.doc, "template"))
	sum := sha256.Sum256(encoded)
	current := d.name + "-" + hex.EncodeToString(sum[:])[:6]
	for _, object := range c.objects {
		if object.kind == "ReplicaSet" && object.controller == "" && matches(object.labels) {
			object.controller = d.uid // adoption
		}
	}
	if c.objects["ReplicaSet/"+current] == nil {
		labels := templateLabels(d.doc)
		rs := &immutableObject{kind: "ReplicaSet", name: current, uid: c.uid(), labels: labels, controller: d.uid}
		c.objects["ReplicaSet/"+current] = rs
		pod := &immutableObject{kind: "Pod", name: current + "-0", uid: c.uid(), labels: labels, controller: rs.uid}
		c.objects["Pod/"+pod.name] = pod
		c.events = append(c.events, "create ReplicaSet/"+current)
	}
	for key, object := range c.objects {
		if object.kind == "ReplicaSet" && object.controller == d.uid && object.name != current {
			c.deleteTree(key) // rollout: the old ReplicaSet scales down
		}
	}
}

func (c *immutableCluster) deleteTree(key string) {
	object := c.objects[key]
	delete(c.objects, key)
	for child, dependent := range c.objects {
		if dependent.controller == object.uid {
			c.deleteTree(child)
		}
	}
}

func (c *immutableCluster) Observe(_ context.Context, id string) (deployment.WorkloadObservation, error) {
	parts := strings.Split(id, "/")
	live := c.objects[parts[0]+"/"+parts[2]]
	if live == nil {
		return deployment.WorkloadObservation{}, fmt.Errorf("workload %s not found", id)
	}
	return live.observation, nil
}

func (c *immutableCluster) ImmutableConflicts(_ context.Context, documents []byte) ([]deployment.ImmutableConflict, error) {
	decoded, err := decodeDocuments(documents)
	if err != nil {
		return nil, err
	}
	return deployment.ParseImmutableConflicts(strings.Join(c.refusals(decoded), "\n")), nil
}

func (c *immutableCluster) live(object *immutableObject) deployment.LiveObject {
	return deployment.LiveObject{
		Ref: sdk.ObjectRef{Kind: object.kind, Namespace: advChartNamespace, Name: object.name}, UID: object.uid,
		Labels: object.labels, Controlled: object.controller != "", Finished: object.kind == "Job",
		AppliedByDeployer: true, Orphan: object.orphan,
	}
}

func (c *immutableCluster) Get(_ context.Context, ref sdk.ObjectRef) (deployment.LiveObject, bool, error) {
	object := c.objects[ref.Kind+"/"+ref.Name]
	if object == nil {
		return deployment.LiveObject{}, false, nil
	}
	return c.live(object), true, nil
}

func (c *immutableCluster) Dependents(_ context.Context, _ sdk.ObjectRef, uid string) ([]deployment.LiveObject, error) {
	var out []deployment.LiveObject
	for _, object := range c.objects {
		if object.controller == uid {
			out = append(out, c.live(object))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref.Name < out[j].Ref.Name })
	return out, nil
}

var _ deployment.Recreator = (*immutableCluster)(nil)

func (c *immutableCluster) MarkOrphan(_ context.Context, ref sdk.ObjectRef, stamp deployment.OrphanStamp) error {
	object := c.objects[ref.Kind+"/"+ref.Name]
	if object == nil {
		return fmt.Errorf("mark %s/%s: not found", ref.Kind, ref.Name)
	}
	object.orphan = &stamp
	return nil
}

func (c *immutableCluster) Orphaned(context.Context, string) ([]deployment.LiveObject, error) {
	var out []deployment.LiveObject
	for _, object := range c.objects {
		if object.orphan != nil {
			out = append(out, c.live(object))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref.Name < out[j].Ref.Name })
	return out, nil
}

func (c *immutableCluster) Delete(_ context.Context, ref sdk.ObjectRef, propagation deployment.Propagation) error {
	key := ref.Kind + "/" + ref.Name
	object := c.objects[key]
	if object == nil {
		return nil
	}
	c.events = append(c.events, "delete "+key+" "+string(propagation))
	if propagation == deployment.PropagationOrphan {
		delete(c.objects, key)
		for _, dependent := range c.objects {
			if dependent.controller == object.uid {
				dependent.controller = ""
			}
		}
		return nil
	}
	c.deleteTree(key)
	return nil
}

func TestAdvanceRecreatesAFinishedJobAndAWorkloadWithAChangedSelector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	upstreamDir := filepath.Join(t.TempDir(), "upstream")
	if err := os.MkdirAll(upstreamDir, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, upstreamDir)
	testutil.Git(t, upstreamDir, "checkout", "-q", "-B", "main")
	advWrite(t, upstreamDir, recreateChartFiles("1", "one"))
	testutil.Git(t, upstreamDir, "add", ".")
	testutil.Git(t, upstreamDir, "commit", "-qm", "release 1")

	config, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "chart"})
	if err != nil {
		t.Fatal(err)
	}
	source := &contracts.DeploymentSource{RendererId: "helm", Config: config}
	plan := advPlan(t, upstreamDir, source)

	cluster := &immutableCluster{}
	var deployer deployment.Deployer
	var accepted lifecycle.SourceReadiness
	o := onboardWith(t, advChartApp, mustBuiltin(t, telemetrysources.NoneProfileName, onbControl), onbOptions{
		prepare: func(t *testing.T, root string) {
			testutil.Git(t, filepath.Dir(root), "clone", "-q", upstreamDir, root)
			testutil.ConfigureGit(t, root)
			testutil.Git(t, root, "remote", "remove", "origin")
			deployer = deployment.Deployer{
				Namespace: advChartNamespace, Renderers: renderers.Catalog(), Images: advImages(t), Applier: cluster,
				TreeDigest: broker.ProposalTreeDigest, AcceptedRoot: root,
			}
			accepted = advReadiness{deployer}
		},
		plan:      plan,
		readiness: lazyReadiness{&accepted},
		sources:   []layout.Source{{Path: ".", Kind: "git", Config: map[string]any{"repository": upstreamDir, "ref": "main"}}},
	}, advChartApp.goodProposal())
	if o.err != nil {
		t.Fatalf("onboarding the chart: %v", o.err)
	}
	onboardingJobs := len(agentJobs(t, o))
	oldDeployment, oldJob := cluster.objects["Deployment/web"].uid, cluster.objects["Job/seed"].uid
	var oldPods []string
	for key, object := range cluster.objects {
		if object.kind == "Pod" {
			oldPods = append(oldPods, key)
		}
	}

	// Upstream changes the Job's image and the Deployment's selector labels.
	advWrite(t, upstreamDir, recreateChartFiles("2", "two"))
	testutil.Git(t, upstreamDir, "commit", "-qam", "release 2: new seed image, new selector")
	release2 := strings.TrimSpace(testutil.Git(t, upstreamDir, "rev-parse", "HEAD"))

	catalog := upstreams.Catalog()
	sources, err := memory.ReadSources(o.accepted, catalog)
	if err != nil || len(sources) != 1 {
		t.Fatalf("accepted sources = %v, %v", sources, err)
	}
	prev, _, err := o.repository.LastUpstream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := catalog.Latest(ctx, sources[0].Kind, sources[0].Config)
	if err != nil || latest.ID != release2 {
		t.Fatalf("latest = %+v, %v", latest, err)
	}
	advClock := &advTestClock{at: time.Unix(1_800_000_000, 0)}
	advancer := lifecycle.Advancer{
		Upstreams: catalog, Broker: o.repository, Readiness: advReadiness{deployer},
		Verifier: onbVerifier{},
		Shape: func(ctx context.Context, root, revision string, source *contracts.DeploymentSource) (string, error) {
			return rendered.Source{Deployment: source, Catalog: renderers.Catalog(), Repository: root}.ShapeDigest(ctx, revision, advChartNamespace)
		},
		Maintenance: advMaintenance{}, Activity: advMaintenance{},
		Wait: advClock.wait, Now: advClock.now,
	}
	result, err := advancer.Advance(ctx, lifecycle.AdvanceConfig{SettingsDigest: settings.Default().DetectionDigest(),
		Namespace: advChartNamespace, HealthObjective: "every synthetic workload stays available", GitDir: filepath.Join(o.accepted, ".git"),
		PendingEvaluations: 1, InfrastructureTries: 1,
	}, upstream.Request{Source: sources[0], Revision: latest, Prev: prev})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Outcome != lifecycle.AdvanceAccepted {
		t.Fatalf("advance = %s hold %s (cause %s owner %s items %v): %s", result.Outcome, result.Hold, result.Cause, result.Owner, result.Items, result.Detail)
	}
	if got := len(agentJobs(t, o)); got != onboardingJobs {
		t.Errorf("agent Jobs after the advance = %d, after onboarding %d: a recreate needs no agent", got, onboardingJobs)
	}

	// The Job and the Deployment are new objects at release 2.
	job, web := cluster.objects["Job/seed"], cluster.objects["Deployment/web"]
	if job == nil || job.uid == oldJob || !strings.Contains(strings.Join(job.observation.Images, ","), "seed") {
		t.Fatalf("Job/seed was not recreated: %+v", job)
	}
	if web == nil || web.uid == oldDeployment {
		t.Fatalf("Deployment/web was not recreated: %+v", web)
	}
	// Every running pod belongs to the recreated Deployment's rollout; the
	// orphaned release-1 pods are gone.
	for _, key := range oldPods {
		if cluster.objects[key] != nil {
			t.Errorf("orphaned pod %s still runs", key)
		}
	}
	pods := 0
	for _, object := range cluster.objects {
		if object.kind != "Pod" {
			continue
		}
		pods++
		owner := ""
		for _, rs := range cluster.objects {
			if rs.kind == "ReplicaSet" && rs.uid == object.controller {
				owner = rs.controller
			}
		}
		if owner != web.uid || object.labels["track"] != "two" {
			t.Errorf("pod %s (labels %v) is not part of the recreated Deployment's rollout", object.name, object.labels)
		}
	}
	if pods == 0 {
		t.Fatal("the recreated Deployment runs no pods")
	}
	for _, want := range []string{"delete Job/seed Background", "delete Deployment/web Orphan"} {
		if !strings.Contains(strings.Join(cluster.events, "\n"), want) {
			t.Errorf("events lack %q:\n%s", want, strings.Join(cluster.events, "\n"))
		}
	}
	// The accepted deployment's evidence records both recreates.
	evidence, found, err := o.repository.AcceptedSourceEvidence(ctx)
	if err != nil || !found {
		t.Fatalf("accepted evidence: %v found %v", err, found)
	}
	var recreated []string
	for _, rec := range evidence.Evidence.Recreated {
		recreated = append(recreated, rec.Ref.Kind+"/"+rec.Ref.Name+":"+rec.Reason+":"+strings.Join(rec.Replaced, ","))
	}
	sort.Strings(recreated)
	if len(recreated) != 2 || !strings.HasPrefix(recreated[0], "Deployment/web:workload-orphan-recreate:ReplicaSet/") || recreated[1] != "Job/seed:finished-job:" {
		t.Fatalf("evidence recreated = %v", recreated)
	}
}
