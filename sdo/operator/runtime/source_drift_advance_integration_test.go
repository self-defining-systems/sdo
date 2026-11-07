//go:build integration

package runtime_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/runtime"
	"sdo.dev/operator/runtime/envtestio"
)

const driftChainNS = "drift-chain"

// driftChainChart is a chart shaped like a Helm release with install and upgrade hooks: a workload, a Job named after
// a hash of its images, and a pre-install,pre-upgrade hook phase (ConfigMap,
// ServiceAccount, Role, RoleBinding, hash-named Job) that every release
// re-applies in place with a new chart label and new data.
func driftChainChart(release int) map[string]string {
	return map[string]string{
		"chart/Chart.yaml":  "apiVersion: v2\nname: synthetic\nversion: 0.1.0\n",
		"chart/values.yaml": fmt.Sprintf("release: %q\n", fmt.Sprint(release)),
		"chart/templates/app.yaml": `apiVersion: apps/v1
kind: Deployment
metadata: {name: web, labels: {chart: "synthetic-{{ .Values.release }}"}}
spec:
  replicas: 1
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      containers:
        - name: web
          image: registry.synthetic.invalid/web:{{ .Values.release }}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ printf "migrate-%s" (.Values.release | sha256sum | trunc 7) }}
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: registry.synthetic.invalid/migrate:{{ .Values.release }}
`,
		"chart/templates/hooks.yaml": `{{- $hook := dict "helm.sh/hook" "pre-install,pre-upgrade" "helm.sh/hook-weight" "-5" "helm.sh/hook-delete-policy" "before-hook-creation" }}
apiVersion: v1
kind: ServiceAccount
metadata: {name: secrets, labels: {chart: "secrets-{{ .Values.release }}"}, annotations: {{ $hook | toJson }}}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: secrets, labels: {chart: "secrets-{{ .Values.release }}"}, annotations: {{ $hook | toJson }}}
data: {script: "release {{ .Values.release }}"}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: secrets, labels: {chart: "secrets-{{ .Values.release }}"}, annotations: {{ $hook | toJson }}}
rules: [{apiGroups: [""], resources: [secrets], verbs: [get]}]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: secrets, labels: {chart: "secrets-{{ .Values.release }}"}, annotations: {{ $hook | toJson }}}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: secrets}
subjects: [{kind: ServiceAccount, name: secrets}]
---
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ printf "secrets-%s" (.Values.release | sha256sum | trunc 7) }}
  annotations: {{ $hook | toJson }}
spec:
  template:
    spec:
      serviceAccountName: secrets
      restartPolicy: Never
      containers:
        - name: secrets
          image: registry.synthetic.invalid/secrets:{{ .Values.release }}
`,
	}
}

// Every accepted advance leaves the live namespace explained by the accepted
// revision's inventory, with the install environment record taken at the first
// install: hook objects the deployer re-applies in place (new labels, stamp and
// data) are never "absent from accepted source", and the hash-named Jobs a
// release renames are pruned instead of left to accumulate. Real API server
// (envtest), real helm render, real Deployer over kubectl; the stand-in for
// kube-controller-manager completes Jobs and reports workloads ready. HUNT1
// predicted six, eight, ten and twelve findings over four advances of one real chart.
//
// Faults it catches that no cheaper test does (each makes it fail): indexing
// the accepted inventory without its hook objects (NewSourceInventory instead
// of NewSourceInventoryFrom), and the deployer not pruning superseded Jobs.
// About 17 s (an API server, five kubectl deploys): nightly runs it.
func TestAcceptedAdvancesLeaveNoUnexplainedDrift(t *testing.T) {
	if testing.Short() {
		t.Skip("starts an API server and deploys five releases with kubectl (about 17 s); nightly CI runs it without -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := envtestio.Start(t)
	t.Setenv("KUBECONFIG", env.Kubeconfig)
	env.CreateNamespace(t, driftChainNS)
	env.CreateNamespace(t, "sdo-ctl")
	cs := env.Client(t)
	dyn, err := dynamic.NewForConfig(env.Config)
	if err != nil {
		t.Fatal(err)
	}
	go driftChainControllers(ctx, cs)

	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, repo)
	testutil.Git(t, repo, "checkout", "-q", "-B", "main")
	release := func(n int) string {
		for name, content := range driftChainChart(n) {
			path := filepath.Join(repo, filepath.FromSlash(name))
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		testutil.Git(t, repo, "add", "-A")
		testutil.Git(t, repo, "commit", "-qm", fmt.Sprintf("release %d", n))
		return strings.TrimSpace(testutil.Git(t, repo, "rev-parse", "HEAD"))
	}
	config, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "chart"})
	if err != nil {
		t.Fatal(err)
	}
	source := &contracts.DeploymentSource{RendererId: "helm", Config: config}
	inventoryAt := func(commit string) rendered.Inventory {
		inventory, err := rendered.Source{Repository: repo, Deployment: source, Catalog: renderers.Catalog()}.AcceptedInventory(ctx, commit, driftChainNS)
		if err != nil {
			t.Fatal(err)
		}
		return inventory
	}

	images := imagesrc.NewCatalog()
	if err := imagesrc.Register[*contracts.PrebuiltImageConfig](images, "prebuilt", driftChainRegistry{&imagesrctest.Fake{}}, imagesrc.Options{}); err != nil {
		t.Fatal(err)
	}
	if err := images.SetDefault("prebuilt"); err != nil {
		t.Fatal(err)
	}
	deployer := deployment.Deployer{
		Namespace: driftChainNS, Renderers: renderers.Catalog(), Images: images, AcceptedRoot: repo,
		Applier: deployment.CommandBackend{Wait: func(context.Context, time.Duration) error { return nil }},
		TreeDigest: func(_ context.Context, checkout string) (string, error) {
			return strings.TrimSpace(testutil.Git(t, checkout, "rev-parse", "HEAD^{tree}")), nil
		},
		Sleep: func(context.Context, time.Duration) error { return nil },
	}

	first := release(1)
	summary := rendered.SummarizeWorkloads(inventoryAt(first).Objects, driftChainNS)
	plan := &contracts.DeploymentPlan{Source: source, ExpectedWorkloadIds: summary.Identities}
	prebuilt, err := anypb.New(&contracts.PrebuiltImageConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, repository := range summary.Repositories {
		plan.Images = append(plan.Images, &contracts.ImageSource{Image: repository, SourceId: "prebuilt", Config: prebuilt})
	}

	var record *contracts.NamespaceOnboarding
	for n := 1; n <= 5; n++ {
		commit := first
		if n > 1 {
			commit = release(n)
		}
		if _, err := deployer.ApplyAndVerify(ctx, repo, plan); err != nil {
			t.Fatalf("release %d: deploy: %v", n, err)
		}
		inventory := inventoryAt(commit)
		if n == 1 {
			// The installer's snapshot of what the first install left behind.
			if err := onboarding.Stage(ctx, cs, "sdo-ctl", driftChainNS, commit, inventory.Objects, time.Now()); err != nil {
				t.Fatalf("stage onboarding: %v", err)
			}
			if record, err = onboarding.Pending(ctx, cs, "sdo-ctl", driftChainNS); err != nil || record == nil {
				t.Fatalf("onboarding record %v: %v", record, err)
			}
		}
		index := runtime.NewSourceInventoryFrom(driftChainNS, commit, inventory)
		if err := index.SetNamespaceOnboarding(record); err != nil {
			t.Fatal(err)
		}
		live := driftChainLive(ctx, t, dyn)
		for _, finding := range index.Findings(live, time.Now()) {
			t.Errorf("after release %d: %s", n, finding.Summary)
		}
	}
	jobs, err := cs.BatchV1().Jobs(driftChainNS).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 2 {
		var names []string
		for _, j := range jobs.Items {
			names = append(names, j.Name)
		}
		t.Errorf("jobs after five releases = %v, want the current migrate and secrets Jobs only", names)
	}
}

type driftChainRegistry struct{ fake *imagesrctest.Fake }

func (r driftChainRegistry) Resolve(ctx context.Context, req imagesrc.Request, _ *contracts.PrebuiltImageConfig) (imagesrc.Resolved, error) {
	return r.fake.Resolve(ctx, req, nil)
}

func driftChainLive(ctx context.Context, t *testing.T, dyn dynamic.Interface) []*unstructured.Unstructured {
	t.Helper()
	var live []*unstructured.Unstructured
	for _, gvr := range []schema.GroupVersionResource{
		{Version: "v1", Resource: "configmaps"}, {Version: "v1", Resource: "services"}, {Version: "v1", Resource: "serviceaccounts"},
		{Group: "apps", Version: "v1", Resource: "deployments"}, {Group: "batch", Version: "v1", Resource: "jobs"},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}, {Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"},
	} {
		list, err := dyn.Resource(gvr).Namespace(driftChainNS).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("list %s: %v", gvr.Resource, err)
		}
		for i := range list.Items {
			live = append(live, &list.Items[i])
		}
	}
	return live
}

// driftChainControllers stands in for kube-controller-manager: Deployments
// become ready, Jobs complete.
func driftChainControllers(ctx context.Context, cs kubernetes.Interface) {
	merge := types.MergePatchType
	for ctx.Err() == nil {
		deps, _ := cs.AppsV1().Deployments(driftChainNS).List(ctx, metav1.ListOptions{})
		for _, d := range deps.Items {
			if d.Status.ObservedGeneration == d.Generation && d.Status.ReadyReplicas == 1 {
				continue
			}
			patch := fmt.Sprintf(`{"status":{"observedGeneration":%d,"replicas":1,"updatedReplicas":1,"readyReplicas":1,"availableReplicas":1}}`, d.Generation)
			if _, err := cs.AppsV1().Deployments(driftChainNS).Patch(ctx, d.Name, merge, []byte(patch), metav1.PatchOptions{}, "status"); err != nil && !errors.IsNotFound(err) && ctx.Err() == nil {
				fmt.Fprintln(os.Stderr, "deployment status:", err)
			}
		}
		jobs, _ := cs.BatchV1().Jobs(driftChainNS).List(ctx, metav1.ListOptions{})
		for _, j := range jobs.Items {
			if j.Status.Succeeded > 0 {
				continue
			}
			now := time.Now().UTC().Format(time.RFC3339)
			patch := fmt.Sprintf(`{"status":{"startTime":%q,"completionTime":%q,"succeeded":1,"ready":0,"terminating":0,"conditions":[{"type":"SuccessCriteriaMet","status":"True","lastProbeTime":%q,"lastTransitionTime":%q},{"type":"Complete","status":"True","lastProbeTime":%q,"lastTransitionTime":%q}]}}`, now, now, now, now, now, now)
			if _, err := cs.BatchV1().Jobs(driftChainNS).Patch(ctx, j.Name, merge, []byte(patch), metav1.PatchOptions{}, "status"); err != nil && !errors.IsNotFound(err) && ctx.Err() == nil {
				fmt.Fprintln(os.Stderr, "job status:", err)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
