//go:build integration

package operatorcli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/internal/rehearsal"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/renderenv"
	sdoruntime "sdo.dev/operator/runtime"
	"sdo.dev/operator/runtime/envtestio"
	"sdo.dev/operator/upstream"
)

// The replay rehearsal: SDO's real upstream-follow path over a sequence of
// chart releases, with no cluster and no model, reporting every blocker of
// every release in one run (it continues past a failing release, as the
// follower polls the next revision). What is real: the API server (envtest:
// immutability, defaulting, server-side apply, generation, RBAC), the helm
// render with the recorded render environment, the broker with its
// validation subprocess, the Coordinator's onboarding, the Advancer, the
// Deployer and its kubectl backend, the operability reports, and the
// controller's accepted-source inventory check over the live namespace.
// What stands in: the kubelet and workload controllers (internal/rehearsal),
// image resolution (any tag pins to a digest), the health judge (scripted),
// the independent health verdict (scripted: readiness plus an optional
// warm-up) and the deployment agent and detection designer (stubs that only
// record their dispatch).

// rehearsalRelease is one upstream release: the whole followed tree, from a
// directory (vendored the way the lane does) or from files.
type rehearsalRelease struct {
	Name  string            `json:"name"`
	Dir   string            `json:"dir,omitempty"`
	Files map[string]string `json:"-"`
}

type rehearsalInput struct {
	Name      string
	Namespace string
	Source    *contracts.DeploymentSource
	Releases  []rehearsalRelease
	Behaviors []rehearsal.Behavior
	// WarmUp makes the scripted health verdict fire while a pod has been
	// ready for less than WarmUp (cold caches); zero is always clear once
	// every workload is ready.
	WarmUp time.Duration
	// Environment is the recorded render environment; nil discovers it from
	// the envtest API server (the lanes' Kubernetes version).
	Environment *contracts.RenderEnvironment
	// App is the scripted health judge's application; nil derives one from
	// the baseline render.
	App *onbApp
	// Plan builds the provided plan from the baseline; nil is the plan Go
	// proposes (stable identities, image repositories), as the lane builds it.
	Plan func(t *testing.T, upstreamDir string, source *contracts.DeploymentSource, environment *contracts.RenderEnvironment, namespace string) *contracts.DeploymentPlan
}

// rehearsalPlan is the plan the lane provides: Go's proposal from the
// baseline render.
func rehearsalPlan(t *testing.T, upstreamDir string, source *contracts.DeploymentSource, environment *contracts.RenderEnvironment, namespace string) *contracts.DeploymentPlan {
	t.Helper()
	reader := rendered.Source{Repository: upstreamDir, Deployment: source, Catalog: renderers.Catalog(), Environment: environment}
	inventory, err := reader.AcceptedInventory(context.Background(), strings.TrimSpace(testutil.Git(t, upstreamDir, "rev-parse", "HEAD")), namespace)
	if err != nil {
		t.Fatalf("render the baseline: %v", err)
	}
	summary := rendered.SummarizeWorkloads(inventory.Objects, namespace)
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

// rehearsalApp derives the scripted judge's application from the baseline
// render: one component per long-running workload, its Services by selector,
// and one read operation on the first Service with a port.
func rehearsalApp(name, namespace string, objects []*unstructured.Unstructured) onbApp {
	app := onbApp{name: name, namespace: namespace}
	var services []*unstructured.Unstructured
	for _, o := range objects {
		if o.GetKind() == "Service" {
			services = append(services, o)
		}
	}
	var components, operations strings.Builder
	first := ""
	for _, o := range objects {
		kind := o.GetKind()
		if kind != "Deployment" && kind != "StatefulSet" && kind != "DaemonSet" {
			continue
		}
		app.workloads = append(app.workloads, kind+"/"+namespace+"/"+o.GetName())
		labels, _, _ := unstructured.NestedStringMap(o.Object, "spec", "template", "metadata", "labels")
		var matched []string
		for _, s := range services {
			selector, _, _ := unstructured.NestedStringMap(s.Object, "spec", "selector")
			if len(selector) == 0 {
				continue
			}
			ok := true
			for k, v := range selector {
				if labels[k] != v {
					ok = false
				}
			}
			if !ok {
				continue
			}
			matched = append(matched, s.GetName())
			ports, _, _ := unstructured.NestedSlice(s.Object, "spec", "ports")
			if first == "" && len(ports) > 0 {
				port, _ := ports[0].(map[string]any)
				number, _ := port["port"].(int64)
				if number == 0 {
					if f, ok := port["port"].(float64); ok {
						number = int64(f)
					}
				}
				if number > 0 {
					first = o.GetName()
					fmt.Fprintf(&operations, "  - id: %s-read\n    writes: false\n    entry: {service: %s, port: %d, method: GET, path: /}\n    path: [%s]\n", o.GetName(), s.GetName(), number, o.GetName())
					app.generators = append(app.generators, onbScenario{o.GetName() + "-read", s.GetName(), "/", "", int(number)})
				}
			}
		}
		fmt.Fprintf(&components, "  - {name: %s, workload: %s/%s, services: [%s], holds_data: %v}\n", o.GetName(), kind, o.GetName(), strings.Join(matched, ", "), kind == "StatefulSet")
	}
	app.mapYAML = "version: 1\ncomponents:\n" + components.String() + "edges: []\noperations:\n" + operations.String()
	if first != "" {
		app.mapYAML += "probes:\n  - {scenario: " + first + "-read, kind: call, operations: [" + first + "-read]}\n"
		app.probes = map[string][]string{"probe-call": {first + "-read"}}
	}
	return app
}

// writeRelease replaces the followed tree with the release and commits it.
func writeRelease(t *testing.T, dir string, release rehearsalRelease, index int) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != ".git" {
			if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	if release.Dir != "" {
		if err := os.CopyFS(dir, os.DirFS(release.Dir)); err != nil {
			t.Fatalf("copy release %s: %v", release.Name, err)
		}
	}
	advWrite(t, dir, release.Files)
	testutil.Git(t, dir, "add", "-A")
	testutil.Git(t, dir, "commit", "-qm", fmt.Sprintf("release %d: %s", index, release.Name))
	return strings.TrimSpace(testutil.Git(t, dir, "rev-parse", "HEAD"))
}

// rehearsalVerifier is the scripted independent health verdict: every
// workload of the namespace ready (the workload-health floor), and, with a
// warm-up, the probe detector firing while any pod has been ready for less
// than it.
type rehearsalVerifier struct {
	lifecycle.HealthVerifier
	client     kubernetes.Interface
	namespace  string
	now        func() time.Time
	warmUp     time.Duration
	detector   string
	readySince map[string]time.Time
	mu         sync.Mutex
}

func (v *rehearsalVerifier) VerifyCandidate(ctx context.Context, _, _, _, _ string, _ lifecycle.DeploymentReceipt) (lifecycle.HealthVerdict, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	pods, err := v.client.CoreV1().Pods(v.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return lifecycle.HealthVerdict{}, err
	}
	if v.readySince == nil {
		v.readySince = map[string]time.Time{}
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		key := string(pod.UID)
		ready := false
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if !ready {
			return lifecycle.HealthVerdict{Outcome: lifecycle.VerificationSourceFault, Summary: "pod " + pod.Name + " not ready",
				Detectors: []lifecycle.DetectorState{{DetectorID: "workload-health", Status: "firing", Reason: "pod " + pod.Name + " not ready"}}}, nil
		}
		if _, ok := v.readySince[key]; !ok {
			v.readySince[key] = v.now()
		}
		if v.warmUp > 0 && v.now().Sub(v.readySince[key]) < v.warmUp {
			return lifecycle.HealthVerdict{Outcome: lifecycle.VerificationSourceFault, Summary: "probe timeouts on a cold pod",
				Detectors: []lifecycle.DetectorState{{DetectorID: v.detector, Status: "firing", Reason: fmt.Sprintf("pod %s ready for %s of a %s warm-up", pod.Name, v.now().Sub(v.readySince[key]), v.warmUp)}}}, nil
		}
	}
	return lifecycle.HealthVerdict{Outcome: lifecycle.VerificationClear, Summary: "every workload ready"}, nil
}

// recordingCorrector and recordingDesigner stand in for the deployment agent
// and the detection designer: they record the dispatch and change nothing.
type recordingCorrector struct{ dispatches *[]rehearsal.Dispatch }

func (r recordingCorrector) CorrectDeployment(_ context.Context, c lifecycle.DeploymentCorrection) (*contracts.DeploymentPlan, error) {
	*r.dispatches = append(*r.dispatches, rehearsal.Dispatch{Role: "deployer", Reason: c.Feedback})
	return nil, fmt.Errorf("rehearsal: the deployment agent is a stub that changes nothing")
}

type recordingDesigner struct{ dispatches *[]rehearsal.Dispatch }

func (r recordingDesigner) RefreshDetection(_ context.Context, change lifecycle.ShapeChange) error {
	*r.dispatches = append(*r.dispatches, rehearsal.Dispatch{Role: "designer", Reason: fmt.Sprintf("shape change %+v", change)})
	return nil
}

// inventoryResources are the namespaced kinds the controller's informers
// feed the accepted-source inventory check (Secrets are never read).
var inventoryResources = []schema.GroupVersionResource{
	{Version: "v1", Resource: "configmaps"}, {Version: "v1", Resource: "services"}, {Version: "v1", Resource: "serviceaccounts"},
	{Version: "v1", Resource: "pods"}, {Version: "v1", Resource: "persistentvolumeclaims"},
	{Group: "apps", Version: "v1", Resource: "deployments"}, {Group: "apps", Version: "v1", Resource: "statefulsets"},
	{Group: "apps", Version: "v1", Resource: "daemonsets"}, {Group: "apps", Version: "v1", Resource: "replicasets"},
	{Group: "batch", Version: "v1", Resource: "jobs"}, {Group: "batch", Version: "v1", Resource: "cronjobs"},
	{Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}, {Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"},
}

func liveObjects(ctx context.Context, client dynamic.Interface, namespace string) ([]*unstructured.Unstructured, error) {
	var out []*unstructured.Unstructured
	for _, gvr := range inventoryResources {
		list, err := client.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", gvr.Resource, err)
		}
		for i := range list.Items {
			item := list.Items[i]
			item.SetAPIVersion(gvr.GroupVersion().String())
			out = append(out, &item)
		}
	}
	return out, nil
}

func objectID(o *unstructured.Unstructured) string {
	return o.GetKind() + "/" + o.GetNamespace() + "/" + o.GetName()
}

var workloadKinds = map[string]bool{"Deployment": true, "StatefulSet": true, "DaemonSet": true, "Job": true}

func liveSpecs(objects []*unstructured.Unstructured) map[string]any {
	out := map[string]any{}
	for _, o := range objects {
		if workloadKinds[o.GetKind()] {
			out[objectID(o)] = o.Object["spec"]
		}
	}
	return out
}

func generations(objects []*unstructured.Unstructured) map[string]int64 {
	out := map[string]int64{}
	for _, o := range objects {
		if workloadKinds[o.GetKind()] {
			out[objectID(o)] = o.GetGeneration()
		}
	}
	return out
}

// renderedSpecs is what a revision renders for each workload (before SDO
// stamps or pins it): a workload whose rendered spec did not change must
// keep its generation.
func renderedSpecs(t *testing.T, source rendered.Source, commit, namespace string) map[string]string {
	t.Helper()
	inventory, err := source.AcceptedInventory(context.Background(), commit, namespace)
	if err != nil {
		return nil
	}
	out := map[string]string{}
	for _, o := range inventory.Objects {
		if !workloadKinds[o.GetKind()] {
			continue
		}
		data, _ := json.Marshal(map[string]any{"spec": o.Object["spec"], "labels": o.GetLabels(), "annotations": o.GetAnnotations()})
		ns := o.GetNamespace()
		if ns == "" {
			ns = namespace
		}
		out[o.GetKind()+"/"+ns+"/"+o.GetName()] = string(data)
	}
	return out
}

func rehearse(t *testing.T, in rehearsalInput) rehearsal.Report {
	t.Helper()
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	report := rehearsal.Report{Name: in.Name}

	env := envtestio.Start(t)
	env.CreateNamespace(t, in.Namespace)
	env.CreateNamespace(t, onbControl)
	// The fake kubelet lists every kind each step; client-go's default
	// 5 QPS throttle would make one step cost seconds.
	config := rest.CopyConfig(env.Config)
	config.QPS, config.Burst = 1000, 2000
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	environment := in.Environment
	if environment == nil {
		if environment, err = renderenv.Discover(client.Discovery()); err != nil {
			t.Fatalf("discover the render environment: %v", err)
		}
	}
	clock := rehearsal.NewClock(time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC))
	kubelet := &rehearsal.Kubelet{Client: client, Namespace: in.Namespace, Behaviors: in.Behaviors, Now: clock.Now}
	world := &rehearsal.World{Clock: clock, Kubelet: kubelet, Kubeconfig: env.Kubeconfig}
	gcCtx, stopGC := context.WithCancel(ctx)
	defer stopGC()
	go kubelet.RunFinalizers(gcCtx, 50*time.Millisecond)

	upstreamDir := filepath.Join(t.TempDir(), "upstream")
	if err := os.MkdirAll(upstreamDir, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, upstreamDir)
	testutil.Git(t, upstreamDir, "checkout", "-q", "-B", "main")
	commits := []string{writeRelease(t, upstreamDir, in.Releases[0], 0)}
	upstreamSource := rendered.Source{Repository: upstreamDir, Deployment: in.Source, Catalog: renderers.Catalog(), Environment: environment}

	planOf := in.Plan
	if planOf == nil {
		planOf = rehearsalPlan
	}
	plan := planOf(t, upstreamDir, in.Source, environment, in.Namespace)
	app := in.App
	if app == nil {
		baseline, err := upstreamSource.AcceptedInventory(ctx, commits[0], in.Namespace)
		if err != nil {
			t.Fatalf("render the baseline: %v", err)
		}
		derived := rehearsalApp(in.Name, in.Namespace, baseline.Objects)
		app = &derived
	}

	backend := deployment.CommandBackend{Run: world.Kubectl, Wait: world.Wait, Now: clock.Now}
	var deployer deployment.Deployer
	var accepted lifecycle.SourceReadiness
	releaseStart := time.Now()
	injectedStart := clock.Now()
	o := onboardWith(t, *app, mustBuiltin(t, telemetrysources.NoneProfileName, onbControl), onbOptions{
		prepare: func(t *testing.T, root string) {
			testutil.Git(t, filepath.Dir(root), "clone", "-q", upstreamDir, root)
			testutil.ConfigureGit(t, root)
			testutil.Git(t, root, "remote", "remove", "origin")
			deployer = deployment.Deployer{
				Namespace: in.Namespace, Renderers: renderers.Catalog(), Images: advImages(t), Applier: backend,
				TreeDigest: broker.ProposalTreeDigest, AcceptedRoot: root, Environment: environment, Sleep: world.Wait,
			}
			accepted = deployer
		},
		plan:      plan,
		readiness: lazyReadiness{&accepted},
		sources:   []layout.Source{{Path: ".", Kind: "git", Config: map[string]any{"repository": upstreamDir, "ref": "main"}}},
	}, app.goodProposal())
	baseline := rehearsal.Release{Index: 0, Name: in.Releases[0].Name, Adopted: o.err == nil, Outcome: "onboarded"}
	if o.err != nil {
		baseline.Outcome, baseline.Error = "onboarding_failed", o.err.Error()
	}
	if o.client != nil {
		for _, job := range agentJobs(t, o) {
			baseline.Dispatches = append(baseline.Dispatches, rehearsal.Dispatch{Role: "agent", ID: job.Name})
		}
	}
	onboardingJobs := len(baseline.Dispatches)
	var acknowledgement *contracts.NamespaceOnboarding
	if o.err == nil {
		head := strings.TrimSpace(testutil.Git(t, o.accepted, "rev-parse", "HEAD"))
		source := rendered.Source{Repository: o.accepted, Deployment: in.Source, Catalog: renderers.Catalog(), Environment: environment}
		if objects, err := source.RawManifests(ctx, head, in.Namespace); err == nil {
			if err := onboarding.Stage(ctx, client, onbControl, in.Namespace, head, objects, clock.Now()); err == nil {
				acknowledgement, _ = onboarding.Pending(ctx, client, onbControl, in.Namespace)
			} else {
				baseline.KubeletErrors = append(baseline.KubeletErrors, "stage namespace onboarding: "+err.Error())
			}
		}
	}
	var specsBefore map[string]any
	observe := func(r *rehearsal.Release, before map[string]int64, beforeSpecs, afterSpecs map[string]string) {
		world.Settle(ctx, 30*time.Second, 5*time.Second)
		live, err := liveObjects(ctx, dyn, in.Namespace)
		if err != nil {
			r.KubeletErrors = append(r.KubeletErrors, err.Error())
			return
		}
		after := generations(live)
		specsAfter := liveSpecs(live)
		for id, gen := range after {
			if old, ok := before[id]; ok && old != gen {
				r.Generations = append(r.Generations, rehearsal.GenerationChange{Workload: id, Before: old, After: gen, RenderedChange: beforeSpecs[id] != afterSpecs[id], Changed: rehearsal.SpecDiff(specsBefore[id], specsAfter[id], 6)})
			}
		}
		sort.Slice(r.Generations, func(i, j int) bool { return r.Generations[i].Workload < r.Generations[j].Workload })
		if o.err != nil {
			return
		}
		head := strings.TrimSpace(testutil.Git(t, o.accepted, "rev-parse", "HEAD"))
		source := rendered.Source{Repository: o.accepted, Deployment: in.Source, Catalog: renderers.Catalog(), Environment: environment}
		inventory, err := source.AcceptedInventory(ctx, head, in.Namespace)
		if err != nil {
			r.KubeletErrors = append(r.KubeletErrors, "accepted inventory: "+err.Error())
			return
		}
		declared := map[string]bool{}
		for _, object := range append(append([]*unstructured.Unstructured{}, inventory.Objects...), inventory.Hooks...) {
			ns := object.GetNamespace()
			if ns == "" {
				ns = in.Namespace
			}
			declared[object.GetKind()+"/"+ns+"/"+object.GetName()] = true
		}
		for _, object := range live {
			if len(object.GetOwnerReferences()) == 0 && (object.GetAnnotations()[rendered.SourceTreeKey] != "" || object.GetLabels()[rendered.SourceTreeKey] != "") && !declared[objectID(object)] {
				r.Leftover = append(r.Leftover, objectID(object))
			}
		}
		sort.Strings(r.Leftover)
		// Adoption claims ownership of what the deployer applied and changes
		// nothing: the first pass writes each object's acceptance record
		// (reported, not asserted: a Deployment's annotation write moves its
		// generation), and a second pass over the recorded namespace must be a
		// no-op: no error, no skipped spec change, no generation moved.
		// A held release leaves the live namespace ahead of the accepted
		// commit on purpose; there is nothing to adopt for it.
		if r.Adopted {
		pass := sdoruntime.AdoptionPass{
			Namespace: in.Namespace, SourceCommit: head, Client: client, Snapshot: sdktest.Snapshot{NamespaceName: in.Namespace},
			Source: rendered.Source{Repository: o.accepted, Deployment: in.Source, Catalog: renderers.Catalog(), Environment: environment, Pins: o.repository},
		}
		beforeAdopt := generations(live)
		for round := 1; round <= 2; round++ {
			status, err := pass.Run(ctx, clock.Now())
			if err != nil {
				t.Errorf("release %s: adoption pass %d: %v", r.Name, round, err)
			}
			if len(status.SpecChangeSkipped) > 0 {
				t.Errorf("release %s: adoption pass %d skipped objects whose adoption would change their spec: %v", r.Name, round, status.SpecChangeSkipped)
			}
			afterLive, err := liveObjects(ctx, dyn, in.Namespace)
			if err != nil {
				t.Fatal(err)
			}
			moved := []string{}
			for id, gen := range generations(afterLive) {
				if old, ok := beforeAdopt[id]; ok && old != gen {
					moved = append(moved, fmt.Sprintf("%s %d->%d", id, old, gen))
				}
			}
			sort.Strings(moved)
			if round == 1 && len(moved) > 0 {
				t.Logf("release %s: adoption pass 1 (acceptance records) moved generations: %v", r.Name, moved)
			}
			if round == 2 && len(moved) > 0 {
				t.Errorf("release %s: a second adoption pass moved generations: %v", r.Name, moved)
			}
			beforeAdopt = generations(afterLive)
		}
		}
		index := sdoruntime.NewSourceInventoryFrom(in.Namespace, head, inventory)
		if err := index.SetNamespaceOnboarding(acknowledgement); err != nil {
			r.KubeletErrors = append(r.KubeletErrors, "namespace onboarding record: "+err.Error())
		}
		for _, f := range index.Findings(live, clock.Now()) {
			resource := f.PrimaryResource.Kind + "/" + f.PrimaryResource.Namespace + "/" + f.PrimaryResource.Name
			r.Drift = append(r.Drift, rehearsal.Finding{Detector: f.DetectorID, Rule: f.RuleID, Resource: resource, Summary: f.Summary})
			r.Incidents = append(r.Incidents, f.DetectorID+" "+resource)
		}
		sort.Slice(r.Drift, func(i, j int) bool { return r.Drift[i].Resource < r.Drift[j].Resource })
	}
	observe(&baseline, map[string]int64{}, nil, nil)
	kubeletErrors := func(r *rehearsal.Release, from int) int {
		r.KubeletErrors = append(r.KubeletErrors, kubelet.Errors[from:]...)
		return len(kubelet.Errors)
	}
	seen := kubeletErrors(&baseline, 0)
	baseline.InjectedSeconds, baseline.Seconds = clock.Now().Sub(injectedStart).Seconds(), time.Since(releaseStart).Seconds()
	report.Releases = append(report.Releases, baseline)
	if o.err != nil {
		report.Seconds = time.Since(started).Seconds()
		return report
	}

	catalog := upstreams.Catalog()
	sources, err := memory.ReadSources(o.accepted, catalog)
	if err != nil || len(sources) != 1 {
		t.Fatalf("accepted sources = %v, %v", sources, err)
	}
	for i := 1; i < len(in.Releases); i++ {
		release := in.Releases[i]
		releaseStart, injectedStart = time.Now(), clock.Now()
		kubelet.SetRelease(i)
		r := rehearsal.Release{Index: i, Name: release.Name}
		live, err := liveObjects(ctx, dyn, in.Namespace)
		if err != nil {
			t.Fatal(err)
		}
		before := generations(live)
		specsBefore = liveSpecs(live)
		commit := writeRelease(t, upstreamDir, release, i)
		beforeSpecs := renderedSpecs(t, upstreamSource, commits[len(commits)-1], in.Namespace)
		afterSpecs := renderedSpecs(t, upstreamSource, commit, in.Namespace)
		commits = append(commits, commit)
		prev, _, err := o.repository.LastUpstream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		latest, err := catalog.Latest(ctx, sources[0].Kind, sources[0].Config)
		if err != nil {
			t.Fatal(err)
		}
		advancer := lifecycle.Advancer{
			Upstreams: catalog, Broker: o.repository, Readiness: deployer,
			Verifier: &rehearsalVerifier{client: client, namespace: in.Namespace, now: clock.Now, warmUp: in.WarmUp, detector: firstTrafficDetector(*app)},
			Shape: func(ctx context.Context, root, revision string, source *contracts.DeploymentSource) (string, error) {
				return rendered.Source{Deployment: source, Catalog: renderers.Catalog(), Repository: root, Environment: environment}.ShapeDigest(ctx, revision, in.Namespace)
			},
			Designer: recordingDesigner{&r.Dispatches}, Deployer: recordingCorrector{&r.Dispatches},
			Maintenance: advMaintenance{}, Activity: advMaintenance{},
			Wait: world.Wait, Now: clock.Now,
		}
		result, err := advancer.Advance(ctx, lifecycle.AdvanceConfig{
			Namespace: in.Namespace, HealthObjective: "every workload stays available", GitDir: filepath.Join(o.accepted, ".git"),
		}, upstream.Request{Source: sources[0], Revision: latest, Prev: prev})
		r.Outcome = string(result.Outcome)
		if err != nil {
			r.Outcome, r.Error = "error", err.Error()
		}
		r.Adopted = err == nil && result.Outcome == lifecycle.AdvanceAccepted
		r.Hold, r.Cause, r.Owner, r.Detail = string(result.Hold), string(result.Cause), string(result.Owner), result.Detail
		for _, item := range result.Items {
			r.Items = append(r.Items, string(item))
		}
		if result.Owner == operability.OwnerResponder {
			r.Incidents = append(r.Incidents, "responder owns hold "+string(result.Hold)+": "+result.Detail)
			r.Dispatches = append(r.Dispatches, rehearsal.Dispatch{Role: "responder", Reason: "hold " + string(result.Hold)})
		}
		if r.Adopted {
			if evidence, found, err := o.repository.AcceptedSourceEvidence(ctx); err == nil && found {
				for _, rec := range evidence.Evidence.Recreated {
					r.Recreated = append(r.Recreated, fmt.Sprintf("%s/%s (%s: %s)", rec.Ref.Kind, rec.Ref.Name, rec.Propagation, strings.Join(rec.Fields, ",")))
				}
			}
			check := &rehearsal.OperabilityCheck{Valid: true}
			report, found, err := o.repository.OperabilityReport(ctx, lifecycle.AdvanceID(commit), operability.StageAdvance)
			switch {
			case err != nil:
				check.Valid, check.Error = false, err.Error()
			case !found:
				check.Valid, check.Error = false, "no advance operability report for the accepted commit"
			default:
				counts := map[string]int{}
				for _, item := range report.Items {
					for _, c := range item.Checks {
						check.Checks++
						counts[string(c.ID)]++
					}
				}
				for id, n := range counts {
					if n > 1 {
						check.Duplicates = append(check.Duplicates, id)
					}
				}
				sort.Strings(check.Duplicates)
				if err := report.Validate(); err != nil {
					check.Valid, check.Error = false, err.Error()
				}
			}
			r.Operability = check
		}
		if got := len(agentJobs(t, o)); got != onboardingJobs {
			r.Dispatches = append(r.Dispatches, rehearsal.Dispatch{Role: "agent", Reason: fmt.Sprintf("%d agent Job(s) started", got-onboardingJobs)})
			onboardingJobs = got
		}
		observe(&r, before, beforeSpecs, afterSpecs)
		seen = kubeletErrors(&r, seen)
		r.InjectedSeconds, r.Seconds = clock.Now().Sub(injectedStart).Seconds(), time.Since(releaseStart).Seconds()
		t.Logf("release %s: %.1fs wall, harness cost so far %+v", r.Name, r.Seconds, world.Stats)
		report.Releases = append(report.Releases, r)
	}
	report.Seconds = time.Since(started).Seconds()
	t.Logf("harness cost: %+v", world.Stats)
	return report
}

func firstTrafficDetector(app onbApp) string {
	if ids := app.trafficDetectors(); len(ids) > 0 {
		return ids[0]
	}
	return "workload-health"
}

// writeRehearsal writes report.json and summary.txt when SDO_REHEARSAL_OUT
// names a directory, and logs the summary.
func writeRehearsal(t *testing.T, report rehearsal.Report) {
	t.Helper()
	t.Log("\n" + report.Summary())
	out := os.Getenv("SDO_REHEARSAL_OUT")
	if out == "" {
		return
	}
	data, err := report.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "rehearsal.json"), append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "rehearsal.txt"), []byte(report.Summary()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// rehearsalFile is the input a benchmark driver writes for
// TestReplayRehearsalFromInput (a benchmark lane's driver writes it): the
// deployment source as ProtoJSON and each release's
// followed tree as a directory.
type rehearsalFile struct {
	Name              string               `json:"name"`
	Namespace         string               `json:"namespace"`
	DeploymentSource  json.RawMessage      `json:"deployment_source"`
	RenderEnvironment string               `json:"render_environment,omitempty"`
	Releases          []rehearsalRelease   `json:"releases"`
	Behaviors         []rehearsal.Behavior `json:"behaviors,omitempty"`
	WarmUp            rehearsal.Duration   `json:"warm_up,omitempty"`
}

// TestReplayRehearsalFromInput rehearses the releases SDO_REHEARSAL_INPUT
// lists (nightly/manual; skipped without it). It fails when any release has a
// blocker, after writing the full report.
func TestReplayRehearsalFromInput(t *testing.T) {
	path := os.Getenv("SDO_REHEARSAL_INPUT")
	if path == "" {
		t.Skip("SDO_REHEARSAL_INPUT names no rehearsal input")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var file rehearsalFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	source := &contracts.DeploymentSource{}
	if err := protojson.Unmarshal(file.DeploymentSource, source); err != nil {
		t.Fatalf("deployment_source: %v", err)
	}
	if len(file.Releases) < 2 {
		t.Fatalf("%s lists %d releases; a rehearsal needs a baseline and at least one release", path, len(file.Releases))
	}
	var environment *contracts.RenderEnvironment
	if file.RenderEnvironment != "" {
		if environment, err = renderenv.Parse(file.RenderEnvironment); err != nil {
			t.Fatalf("render_environment: %v", err)
		}
	}
	report := rehearse(t, rehearsalInput{
		Name: file.Name, Namespace: file.Namespace, Source: source, Releases: file.Releases,
		Behaviors: file.Behaviors, WarmUp: time.Duration(file.WarmUp), Environment: environment,
	})
	writeRehearsal(t, report)
	for _, release := range report.Releases {
		if blockers := release.Blockers(); len(blockers) > 0 {
			t.Errorf("release %s: %d blocker(s)", release.Name, len(blockers))
		}
	}
}
