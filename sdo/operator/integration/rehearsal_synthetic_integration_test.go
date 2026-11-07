//go:build integration

package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/rehearsal"
)

// The PR-tier rehearsal: a small synthetic chart with the shapes the live
// release replays hit, over four releases. It runs in about 30 s (envtest start, one
// onboarding with ~500 broker git processes, three advances with the
// validation subprocess); it is the cheap tier that must show a replay
// blocker before a live lane spends an hour on it.
//
// The chart: a Deployment that changes every release, a Deployment that
// never changes (Kubernetes bumps a Deployment's generation for any
// annotation change, so SDO's bookkeeping must stay off its annotations),
// and a StatefulSet with Services; an autoscaler targeting the first
// Deployment (the fake kubelet runs the HorizontalPodAutoscaler controller and
// the Deployment controller's revision annotation, so live fields no render
// holds change under every release); a Job named after
// a hash of its images (renamed by every image bump); a fixed-name bucket Job
// (its pod template is immutable); and a pre-install/pre-upgrade hook under
// templates/shared-secrets/ (a secrets-named upstream path) with its
// ConfigMap, ServiceAccount, Role and RoleBinding.
type syntheticRelease struct {
	web, migrate, buckets, store, secrets string
}

func (r syntheticRelease) files() map[string]string {
	return map[string]string{
		"chart/Chart.yaml":  "apiVersion: v2\nname: rehearsal\nversion: 0.1.0\n",
		"chart/values.yaml": fmt.Sprintf("web: %q\nmigrate: %q\nbuckets: %q\nstore: %q\nsecrets: %q\n", r.web, r.migrate, r.buckets, r.store, r.secrets),
		"chart/templates/app.yaml": `apiVersion: apps/v1
kind: Deployment
metadata: {name: web, annotations: {synthetic.invalid/owner: web-team}}
spec:
  replicas: 1
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      containers:
        - name: web
          image: registry.synthetic.invalid/web:{{ .Values.web }}
          ports: [{containerPort: 8080}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: static, annotations: {synthetic.invalid/owner: static-team}}
spec:
  replicas: 1
  selector: {matchLabels: {app: static}}
  template:
    metadata: {labels: {app: static}}
    spec:
      containers:
        - name: static
          image: registry.synthetic.invalid/static:1
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: web}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: web}
  minReplicas: 2
  maxReplicas: 4
---
apiVersion: v1
kind: Service
metadata: {name: web}
spec:
  selector: {app: web}
  ports: [{port: 8080}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: store}
spec:
  replicas: 1
  serviceName: store
  selector: {matchLabels: {app: store}}
  template:
    metadata: {labels: {app: store}}
    spec:
      containers:
        - name: store
          image: registry.synthetic.invalid/store:{{ .Values.store }}
          ports: [{containerPort: 5432}]
---
apiVersion: v1
kind: Service
metadata: {name: store}
spec:
  selector: {app: store}
  ports: [{port: 5432}]
---
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ printf "migrate-%s" (printf "%s-%s" .Values.web .Values.migrate | sha256sum | trunc 7) }}
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: registry.synthetic.invalid/migrate:{{ .Values.migrate }}
---
apiVersion: batch/v1
kind: Job
metadata: {name: buckets}
spec:
  backoffLimit: 6
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: buckets
          image: registry.synthetic.invalid/buckets:{{ .Values.buckets }}
`,
		// The vendored chart ships new template text each release.
		"chart/templates/shared-secrets/job.yaml": "# upstream template revision " + r.secrets + "\n" + `apiVersion: v1
kind: ServiceAccount
metadata:
  name: shared-secrets
  annotations: {"helm.sh/hook": "pre-install,pre-upgrade", "helm.sh/hook-weight": "-5"}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: shared-secrets
  annotations: {"helm.sh/hook": "pre-install,pre-upgrade", "helm.sh/hook-weight": "-5"}
rules:
  - apiGroups: [""]
    resources: [secrets]
    verbs: [get, list, create, patch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: shared-secrets
  annotations: {"helm.sh/hook": "pre-install,pre-upgrade", "helm.sh/hook-weight": "-5"}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: shared-secrets}
subjects:
  - {kind: ServiceAccount, name: shared-secrets}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: shared-secrets
  annotations: {"helm.sh/hook": "pre-install,pre-upgrade", "helm.sh/hook-weight": "-5"}
data:
  generate.sh: "echo generate {{ .Values.secrets }}"
---
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ printf "shared-secrets-%s" (.Values.secrets | sha256sum | trunc 7) }}
  annotations: {"helm.sh/hook": "pre-install,pre-upgrade", "helm.sh/hook-weight": "-3"}
spec:
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: shared-secrets
      containers:
        - name: generate
          image: registry.synthetic.invalid/kubectl:1
`,
	}
}

// syntheticRehearsal is the PR-tier input: the baseline, an image release
// that renames the hash Job, recreates the bucket Job and touches the
// shared-secrets hook, a release whose recreated bucket Job fails every
// attempt, and a release that recovers. Pods take a 30 s warm-up before the
// scripted probe clears.
func syntheticRehearsal(t *testing.T) rehearsalInput {
	t.Helper()
	config, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "chart"})
	if err != nil {
		t.Fatal(err)
	}
	releases := []syntheticRelease{
		{"1", "1", "1", "1", "a"},
		{"2", "2", "2", "1", "b"},
		{"3", "3", "3", "1", "b"},
		{"4", "3", "4", "1", "b"},
	}
	in := rehearsalInput{
		Name: "synthetic", Namespace: "rehearsal",
		Source: &contracts.DeploymentSource{RendererId: "helm", Config: config},
		Behaviors: []rehearsal.Behavior{
			{Kind: "Job", Name: "buckets", JobFailures: -1, FromRelease: 2, UntilRelease: 2},
			// A rolled pod is unready for 20 s: its Service has no endpoints
			// while the advance rolls it.
			{Kind: "Deployment", Name: "web", ReadyAfter: rehearsal.Duration(20 * time.Second)},
			{Kind: "StatefulSet", Name: "store", ReadyAfter: rehearsal.Duration(20 * time.Second)},
		},
		WarmUp: 30 * time.Second,
	}
	for i, r := range releases {
		in.Releases = append(in.Releases, rehearsalRelease{Name: fmt.Sprintf("r%d", i), Files: r.files()})
	}
	return in
}

func TestReplayRehearsalOfASyntheticChartReportsEveryRelease(t *testing.T) {
	report := rehearse(t, syntheticRehearsal(t))
	writeRehearsal(t, report)
	if len(report.Releases) != 4 {
		t.Fatalf("rehearsed %d of 4 releases", len(report.Releases))
	}
	byName := map[string]rehearsal.Release{}
	for _, r := range report.Releases {
		byName[r.Name] = r
		if len(r.KubeletErrors) > 0 {
			t.Errorf("%s: the fake kubelet hit API errors (a harness gap): %v", r.Name, r.KubeletErrors)
		}
	}
	if !byName["r0"].Adopted {
		t.Fatalf("baseline not onboarded: %s", byName["r0"].Error)
	}
	// r1: the hash-named Job is renamed (stable identity), the finished
	// bucket Job is recreated, the shared-secrets path validates, the
	// untouched StatefulSet and Deployment keep their generation, and the cold pods settle.
	r1 := byName["r1"]
	if !r1.Adopted {
		t.Errorf("r1 not adopted: %v", r1.Blockers())
	}
	if !strings.Contains(strings.Join(r1.Recreated, " "), "Job/buckets") {
		t.Errorf("r1 recreated %v, want Job/buckets", r1.Recreated)
	}
	for _, g := range r1.Generations {
		if !g.RenderedChange {
			t.Errorf("r1: %s generation %d -> %d although its rendered spec did not change", g.Workload, g.Before, g.After)
		}
	}
	if r1.Operability == nil || !r1.Operability.Valid {
		t.Errorf("r1 operability report: %+v", r1.Operability)
	}
	// r2: the recreated bucket Job fails every attempt: a typed rollout hold
	// the responder owns, naming the Job, within the injected deadline.
	r2 := byName["r2"]
	if r2.Adopted || r2.Hold != "rollout" || r2.Owner != "responder" || !strings.Contains(r2.Detail, "buckets") {
		t.Errorf("r2 = adopted %v hold %q owner %q: %s", r2.Adopted, r2.Hold, r2.Owner, r2.Detail)
	}
	// r3: the rehearsal continued past the hold, and the next release adopts.
	if r3 := byName["r3"]; !r3.Adopted {
		t.Errorf("r3 not adopted after the r2 hold: %v", r3.Blockers())
	}
	for _, r := range report.Releases {
		// An adopted release leaves nothing the controller would call
		// unexplained: hook objects are accepted, superseded hash-named
		// Jobs are pruned (live 8 opened an incident on both).
		if r.Adopted && len(r.Drift) > 0 {
			t.Errorf("%s adopted with drift findings: %+v", r.Name, r.Drift)
		}
		// The advance's rollout is a maintenance window: disruption it
		// causes (the StatefulSet's Service without endpoints while it
		// rolls) opens no incident (live 9 opened one on 3 of 4 releases).
		if r.Adopted && r.Index > 0 && len(r.ControllerIncidents) > 0 {
			t.Errorf("%s: an accepted advance opened incidents: %v", r.Name, r.ControllerIncidents)
		}
		for _, d := range r.Dispatches {
			if d.Role == "deployer" || d.Role == "designer" {
				t.Errorf("%s dispatched the %s for an image-only release: %s", r.Name, d.Role, d.Reason)
			}
		}
	}
}
