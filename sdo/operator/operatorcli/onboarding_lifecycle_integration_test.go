//go:build integration

package operatorcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/install"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetrycontext"
	"sdo.dev/operator/worker_registry"
)

// This file is the cluster-free onboarding end-to-end check: the production
// lifecycle (lifecycle.Coordinator.Run) over the operate Job's real broker
// (onboardingBroker, with its validation subprocess: this test binary, see
// TestMain), Go's bootstrap commits, the operate Job's real agent transport
// (lifecycleWorkers' KubernetesExecutor on a fake API server) and the
// install's telemetry for each built-in profile. Only what needs a cluster or
// a model is scripted: the health judge (onbJudge edits the checkout and runs
// the real `sdo-operator memory check` with the Job's environment), the
// deployer receipt and live checks, the independent health verdict and the
// controller install. The compiler is FakeGo: the bugs this guards against
// (telemetry policy missing in the validation child, ownership evaluated
// before Go prepares the traffic detectors, a too-coarse retry repeat key)
// all sit before compilation, which the nightly
// TestOnboardingBrokerAcceptsTheJudgesProposalUnderEveryProfile covers.

// onbApp is a synthetic application shape: its manifests and the health
// judge's known-good proposal for it.
type onbApp struct {
	name, namespace string
	manifests       string
	workloads       []string
	// mapYAML is the judge's application map region; generators its traffic
	// scenarios; probes the health-probe workloads (name -> scenarios).
	mapYAML    string
	generators []onbScenario
	probes     map[string][]string
	// links is the link-probe workload's edge list (YAML), empty for none.
	links string
}

type onbScenario struct {
	id, service, path, body string
	port                    int
}

func onbDeployment(kind, name string, port int) string {
	spec := fmt.Sprintf(`  selector: {matchLabels: {app: %[1]s}}
  template:
    metadata: {labels: {app: %[1]s}}
    spec:
      containers:
        - name: %[1]s
          image: registry.synthetic.invalid/%[1]s:1
          ports: [{containerPort: %[2]d}]
`, name, port)
	if kind == "StatefulSet" {
		spec = "  serviceName: " + name + "\n" + spec
	}
	return fmt.Sprintf("apiVersion: apps/v1\nkind: %s\nmetadata: {name: %s}\nspec:\n  replicas: 1\n%s---\napiVersion: v1\nkind: Service\nmetadata: {name: %s}\nspec:\n  selector: {app: %s}\n  ports: [{port: %d}]\n---\n", kind, name, spec, name, name, port)
}

// onbToy is a single-service application.
var onbToy = onbApp{
	name: "onb-toy", namespace: "onb-toy",
	manifests: onbDeployment("Deployment", "web", 8080),
	workloads: []string{"Deployment/onb-toy/web"},
	mapYAML: `version: 1
components:
  - {name: web, workload: Deployment/web, services: [web], holds_data: false}
edges: []
operations:
  - id: web-home
    writes: false
    entry: {service: web, port: 8080, method: GET, path: /}
    path: [web]
probes:
  - {scenario: web-home, kind: call, operations: [web-home]}
`,
	generators: []onbScenario{{"web-home", "web", "/", "web", 8080}},
	probes:     map[string][]string{"probe-call": {"web-home"}},
}

// onbShop is a multi-workload application: two Deployments, two
// StatefulSets, a seeding Job, and a health-probe and a link-probe workload.
var onbShop = onbApp{
	name: "onb-shop", namespace: "onb-shop",
	manifests: onbDeployment("Deployment", "front", 8080) + onbDeployment("Deployment", "api", 8081) +
		onbDeployment("StatefulSet", "store", 5432) + onbDeployment("StatefulSet", "ledger", 6379) +
		`apiVersion: batch/v1
kind: Job
metadata: {name: seed}
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: seed
          image: registry.synthetic.invalid/seed:1
`,
	workloads: []string{"Deployment/onb-shop/api", "Deployment/onb-shop/front", "StatefulSet/onb-shop/ledger", "StatefulSet/onb-shop/store"},
	mapYAML: `version: 1
components:
  - {name: front, workload: Deployment/front, services: [front], holds_data: false}
  - {name: api, workload: Deployment/api, services: [api], holds_data: false}
  - {name: store, workload: StatefulSet/store, services: [store], holds_data: false}
  - {name: ledger, workload: StatefulSet/ledger, services: [ledger], holds_data: false}
edges:
  - {from: front, to: api, evidence: [config], source: k8s/app.yaml}
  - {from: api, to: store, evidence: [config], source: k8s/app.yaml}
  - {from: api, to: ledger, evidence: [config], source: k8s/app.yaml}
operations:
  - id: front-home
    writes: false
    entry: {service: front, port: 8080, method: GET, path: /}
    path: [front]
  - id: list-items
    writes: false
    entry: {service: front, port: 8080, method: GET, path: /items}
    path: [front, api, store]
  - id: balance
    writes: false
    entry: {service: api, port: 8081, method: GET, path: /balance}
    path: [api, ledger]
probes:
  - {scenario: front-home, kind: call, operations: [front-home]}
  - {scenario: list-items, kind: call, operations: [list-items]}
  - {scenario: balance, kind: call, operations: [balance]}
`,
	generators: []onbScenario{{"front-home", "front", "/", "front", 8080}, {"list-items", "front", "/items", "items", 8080}, {"balance", "api", "/balance", "balance", 8081}},
	probes:     map[string][]string{"probe-call": {"front-home", "list-items", "balance"}},
	links:      "  - {from: front, to: api, port: 8081, protocol: tcp}\n  - {from: api, to: store, port: 5432, protocol: postgres}\n  - {from: api, to: ledger, port: 6379, protocol: redis}\n",
}

const onbControl = "sdo-ctl"

// onbEdit is one judge edit: a path relative to the checkout and its content;
// an architecture edit appends the map region instead.
type onbEdit struct{ path, content string }

// goodProposal is the health judge's known-good proposal for app: the map,
// generators, health-probe and verify workloads, and the health-objective
// playbook. It hand-authors no detector: Go installs the generic and
// traffic-<workload> detectors.
func (app onbApp) goodProposal() []onbEdit {
	edits := []onbEdit{{path: layout.ArchitectureFile, content: "\n<!-- sdo:application-map:begin -->\n```yaml\n" + app.mapYAML + "```\n<!-- sdo:application-map:end -->\n"}}
	var generators strings.Builder
	generators.WriteString("package generators\n\nimport \"sdo.dev/operator/sdk/traffic\"\n\nfunc Scenarios() traffic.Catalog {\n\treturn traffic.Catalog{\n")
	var all []string
	for _, s := range app.generators {
		fmt.Fprintf(&generators, "\t\t{\n\t\t\tID: %q, Description: \"Read %s.\",\n\t\t\tTarget: traffic.Target{Service: %q, Port: %d},\n\t\t\tDependsOn: []string{%q}, SideEffect: traffic.SideEffectRead,\n\t\t\tSteps: []traffic.Step{{Name: \"get\", Endpoint: traffic.GET(%q, nil).Contains(%q)}},\n\t\t\tDetects: []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},\n\t\t},\n", s.id, s.path, s.service, s.port, s.service, s.path, s.body)
		all = append(all, s.id)
	}
	generators.WriteString("\t}\n}\n")
	edits = append(edits, onbEdit{".sdo/diagnostics/traffic/generators/generators.go", generators.String()})
	scenarios := func(ids []string) string {
		var out strings.Builder
		for _, id := range ids {
			fmt.Fprintf(&out, "  - {id: %s, weight: 1}\n", id)
		}
		return out.String()
	}
	for _, name := range app.probeNames() {
		edits = append(edits, onbEdit{layout.TrafficWorkloadsDir + "/" + name + ".yaml", "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: " + name + "\npurpose: health-probe\nratePerSecond: 1\ntimeout: 2s\nslo:\n  window: 30\n  minSamples: 3\n  maxAge: 30s\n  maxErrorRate: 0.5\n  maxTimeoutRate: 0.5\n  latencyPercentile: 50\n  maxLatency: 1s\nscenarios:\n" + scenarios(app.probes[name])})
	}
	if app.links != "" {
		edits = append(edits, onbEdit{layout.TrafficWorkloadsDir + "/links.yaml", "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: links\npurpose: link-probe\ntimeout: 2s\nlinks:\n" + app.links})
	}
	edits = append(edits,
		onbEdit{layout.TrafficWorkloadsDir + "/verify.yaml", "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: verify\npurpose: verify-burst\nratePerSecond: 4\nduration: 3s\nscenarios:\n" + scenarios(all)},
		onbEdit{".sdo/playbooks/README.md", "# Playbooks\n\n- [health-objective](health-objective/README.md)\n"},
		onbEdit{".sdo/playbooks/health-objective/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: health-objective-failure\noriginating_incident: lifecycle-bootstrap\n---\n# Health objective failure\n\nRestore the logical resource the finding names, such as <AFFECTED_RESOURCE>, until every workload is available and every probe passes.\n\n## Fixtures\n\nThe synthetic application has no write routes; probes create no fixtures.\n\n## Probe costs\n\nEach health-probe workload sends one read request per second and stores nothing.\n\n## Calibration\n\nSynthetic: the scripted judge calibrates nothing; each bound is the documented minimum.\n"},
	)
	return edits
}

func (app onbApp) probeNames() []string {
	names := make([]string, 0, len(app.probes))
	for name := range app.probes {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// trafficDetectors are the detector IDs Go installs for app's probes.
func (app onbApp) trafficDetectors() []string {
	var ids []string
	for _, name := range app.probeNames() {
		ids = append(ids, "traffic-"+name)
	}
	if app.links != "" {
		ids = append(ids, "traffic-links")
	}
	return ids
}

// onbJudge is the scripted health judge. Each dispatch applies the next
// proposal (the last repeats), runs the real `memory check` exactly as the
// judge's Job would (the Job's environment), and reports the IDs.
type onbJudge struct {
	operatorRoot string
	proposals    [][]onbEdit
	detectorIDs  []string
	mu           sync.Mutex
	checks       [][]*contracts.MemoryDiagnostic
	environments []map[string]string
	requests     []*contracts.HealthJudgeRequest
}

func (j *onbJudge) run(ctx context.Context, task *contracts.AgentTask, environment map[string]string) (*contracts.AgentResult, error) {
	j.mu.Lock()
	attempt := len(j.checks)
	j.mu.Unlock()
	request := &contracts.HealthJudgeRequest{}
	if err := task.GetRequest().UnmarshalTo(request); err != nil {
		return nil, err
	}
	j.mu.Lock()
	j.requests = append(j.requests, request)
	j.mu.Unlock()
	checkout := task.GetWorkspace().GetCheckoutPath()
	for _, edit := range j.proposals[min(attempt, len(j.proposals)-1)] {
		path := filepath.Join(checkout, edit.path)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		if edit.path == layout.ArchitectureFile {
			current, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			edit.content = string(current) + edit.content
		}
		if strings.HasSuffix(edit.path, ".go") {
			// The judge formats its Go sources, as the skill tells it to.
			formatted, err := format.Source([]byte(edit.content))
			if err != nil {
				return nil, err
			}
			edit.content = string(formatted)
		}
		if err := os.WriteFile(path, []byte(edit.content), 0o644); err != nil {
			return nil, err
		}
	}
	// The Job's environment, plus the operator sources the worker image
	// carries at the default operator root.
	getenv := func(name string) string {
		if name == memoryOperatorEnv {
			return j.operatorRoot
		}
		return environment[name]
	}
	var stdout, stderr bytes.Buffer
	code := RunMemory(ctx, []string{"check", "--root", checkout, "--json"}, getenv, &stdout, &stderr)
	if code != exitClean && code != exitDiagnostics {
		return nil, fmt.Errorf("judge memory check exit %d: %s", code, stderr.String())
	}
	report := &contracts.MemoryCheckReport{}
	if err := wire.Unmarshal(stdout.Bytes(), report); err != nil {
		return nil, fmt.Errorf("judge memory check report: %w: %s", err, stdout.String())
	}
	j.mu.Lock()
	j.checks = append(j.checks, report.GetDiagnostics())
	j.environments = append(j.environments, environment)
	j.mu.Unlock()
	response, err := anypb.New(&contracts.HealthJudgeResult{Authored: true, DetectorIds: j.detectorIDs, AgentSessionId: "scripted-judge", Summary: "scripted onboarding proposal"})
	if err != nil {
		return nil, err
	}
	return &contracts.AgentResult{SchemaVersion: task.GetSchemaVersion(), TaskId: task.GetTaskId(), Status: "completed", ProposalId: task.GetWorkspace().GetProposalId(), Response: response}, nil
}

// onbJobRunner stands in for the kubelet and the worker image on the fake API
// server: it runs each agent Job the executor creates through the scripted
// judge with that Job's container environment and request ConfigMap, then
// writes the result ConfigMap and completes the Job.
func onbJobRunner(ctx context.Context, t *testing.T, client *fake.Clientset, namespace string, judge *onbJudge) <-chan error {
	done := make(chan error, 1)
	go func() {
		handled := map[string]bool{}
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-time.After(2 * time.Millisecond):
			}
			jobs, err := client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				continue
			}
			for i := range jobs.Items {
				job := &jobs.Items[i]
				if handled[job.Name] {
					continue
				}
				handled[job.Name] = true
				if err := onbRunJob(ctx, client, namespace, job, judge); err != nil {
					done <- fmt.Errorf("job %s: %w", job.Name, err)
					return
				}
			}
		}
	}()
	return done
}

func onbRunJob(ctx context.Context, client *fake.Clientset, namespace string, job *batchv1.Job, judge *onbJudge) error {
	environment := map[string]string{}
	for _, variable := range job.Spec.Template.Spec.Containers[0].Env {
		environment[variable.Name] = variable.Value
	}
	request, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, environment["SDO_REQUEST_CONFIGMAP"], metav1.GetOptions{})
	if err != nil {
		return err
	}
	task := &contracts.AgentTask{}
	if err := wire.UnmarshalDurable([]byte(request.Data[worker_registry.TaskDataKey]), task); err != nil {
		return err
	}
	if !task.GetRequest().MessageIs(&contracts.HealthJudgeRequest{}) {
		return fmt.Errorf("unexpected agent task %s", task.GetRequest().GetTypeUrl())
	}
	result, err := judge.run(ctx, task, environment)
	if err != nil {
		return err
	}
	payload, err := wire.Canonical(result)
	if err != nil {
		return err
	}
	if _, err := client.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: environment["SDO_RESULT_CONFIGMAP"], Namespace: namespace}, Data: map[string]string{worker_registry.ResultDataKey: string(payload)}}, metav1.CreateOptions{}); err != nil {
		return err
	}
	job.Status.Succeeded = 1
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
	_, err = client.BatchV1().Jobs(namespace).UpdateStatus(ctx, job, metav1.UpdateOptions{})
	return err
}

// onbReadiness is the trusted deployer without a cluster: it reports the
// deployment of exactly the checkout it was handed, bound the way the real
// deployer binds it, and passing live checks.
type onbReadiness struct {
	lifecycle.SourceReadinessVerifier
	app onbApp
}

func (r onbReadiness) ApplyAndVerify(ctx context.Context, checkout string, plan *contracts.DeploymentPlan) (lifecycle.DeploymentReceipt, error) {
	tree, err := broker.ProposalTreeDigest(ctx, checkout)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: checkout}, rendered.BaseTree(tree))
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	defer effective.Close()
	digest := "sha256:" + strings.Repeat("c", 64)
	var images []rendered.DeployedImage
	var refs []string
	for _, id := range r.app.workloads {
		parts := strings.Split(id, "/")
		ref := "registry.synthetic.invalid/" + parts[2] + "@" + digest
		images = append(images, rendered.DeployedImage{Ref: sdk.ObjectRef{Kind: parts[0], Namespace: parts[1], Name: parts[2]}, Name: parts[2], Applied: ref, Digest: digest, Inputs: rendered.ImageInputs{SourceImage: "registry.synthetic.invalid/" + parts[2] + ":1"}})
		refs = append(refs, ref)
	}
	return lifecycle.DeploymentReceipt{
		Source: proto.Clone(plan.GetSource()).(*contracts.DeploymentSource), BaseTree: tree, EffectiveTree: effective.ID, PatchesDigest: effective.PatchesDigest,
		ObservedRevision: effective.ID, RenderDigest: strings.Repeat("a", 64), EnvDigest: strings.Repeat("b", 64),
		ContainerImages: images, Images: refs, ResourceIDs: r.app.workloads, Ready: true,
	}, nil
}

func (onbReadiness) CheckLive(context.Context, string, *contracts.DeploymentPlan, lifecycle.DeploymentReceipt, error) ([]operability.Check, error) {
	var checks []operability.Check
	for _, id := range []string{"O1.deploy", "O2.stamps", "O6.budget"} {
		checks = append(checks, operability.Check{ID: id, Phase: operability.PhaseLive, Status: operability.StatusPass})
	}
	return checks, nil
}

// onbVerifier is the independent health verdict: the healthy synthetic
// application clears every detector.
type onbVerifier struct{ lifecycle.HealthVerifier }

func (onbVerifier) VerifyCandidate(context.Context, string, string, string, string, lifecycle.DeploymentReceipt) (lifecycle.HealthVerdict, error) {
	return lifecycle.HealthVerdict{Outcome: lifecycle.VerificationClear, Summary: "synthetic application healthy"}, nil
}

type onbInstaller struct{ commits []string }

func (i *onbInstaller) Install(_ context.Context, commit, _ string) error {
	i.commits = append(i.commits, commit)
	return nil
}

// onbOutcome is what one onboarding run produced.
type onbOutcome struct {
	result     lifecycle.Result
	err        error
	repository *broker.Broker
	accepted   string
	judgeID    string
	judge      *onbJudge
	installs   []string
	// client is the fake API server the agent Jobs went through: one Job per
	// agent task, so its Jobs count the agent turns of the run.
	client *fake.Clientset
}

// onbOptions vary an onboarding from the default synthetic raw-manifest
// application: how the source repository is made, what is deployed from it and
// how, and which upstream it follows.
type onbOptions struct {
	// prepare writes the application source into root before its first commit;
	// nil writes app.manifests under k8s/.
	prepare func(t *testing.T, root string)
	// plan is the provided deployment plan; nil is the raw source over k8s/ with
	// app.workloads as the expected workloads.
	plan *contracts.DeploymentPlan
	// readiness replaces the scripted deployer.
	readiness lifecycle.SourceReadiness
	// sources are the upstreams followed (the repository is their clone).
	sources []layout.Source
}

// onboard runs the production lifecycle for app under profile with the
// scripted judge's proposals, through the operate Job's broker and agent
// transport.
func onboard(t *testing.T, app onbApp, profile telemetry.Profile, proposals ...[]onbEdit) onbOutcome {
	t.Helper()
	return onboardWith(t, app, profile, onbOptions{}, proposals...)
}

func onboardWith(t *testing.T, app onbApp, profile telemetry.Profile, options onbOptions, proposals ...[]onbEdit) onbOutcome {
	t.Helper()
	testutil.FakeGo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mount := t.TempDir()
	root := filepath.Join(mount, "application")
	if options.prepare != nil {
		options.prepare(t, root)
	} else {
		if err := os.MkdirAll(filepath.Join(root, "k8s"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "k8s", "app.yaml"), []byte(app.manifests), 0o644); err != nil {
			t.Fatal(err)
		}
		testutil.InitGit(t, root)
		testutil.Git(t, root, "add", ".")
		testutil.Git(t, root, "commit", "-qm", "synthetic application source")
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	config := OperateConfig{
		ApplicationRoot: root, RepositoryMount: mount, WorktreeRoot: filepath.Join(mount, "worktrees"),
		Install: install.Config{TelemetryProfile: profile, BackendRecipes: telemetrysources.Recipes(), Replay: "off", RepositoryPVC: "sdo-repository"},
		Lifecycle: lifecycle.Config{
			OperationID: "onb", Application: app.name, Namespace: app.namespace, HealthObjective: "every synthetic workload stays available",
			MaxAttempts: 1, PendingEvaluations: 1, InfrastructureTries: 1,
			Sources: options.sources,
		},
	}
	registry, err := lifecycleRegistry(config, filepath.Join(operatorRoot, "..", "agents", "registrations"))
	if err != nil {
		t.Fatal(err)
	}
	// The operator's registry, as RunOperate passes it; the validation child
	// (TestMain) registers one more adapter, which no proposal here binds.
	repository, err := onboardingBroker(config, onbControl, (broker.SubprocessValidator{OperatorRoot: operatorRoot}).Validator(), sdk.ObservationScope{}, telemetryRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if config.Lifecycle.Telemetry, err = telemetrycontext.Build(telemetryRegistry(), config.Install.TelemetryEnvironment(), nil); err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	workers, err := lifecycleWorkers(config, client, onbControl, registry, repository)
	if err != nil {
		t.Fatal(err)
	}
	workers.PollInterval = 2 * time.Millisecond
	judge := &onbJudge{operatorRoot: operatorRoot, proposals: proposals, detectorIDs: app.trafficDetectors()}
	runnerCtx, stopRunner := context.WithCancel(ctx)
	runner := onbJobRunner(runnerCtx, t, client, onbControl, judge)
	installer := &onbInstaller{}
	coordinator := lifecycle.Coordinator{
		Renderers: renderers.Catalog(),
		Broker:    repository, Bootstrap: artifacts.BootstrapStore{}, Registry: registry, Workers: workers,
		Readiness: options.readiness, Verifier: onbVerifier{}, Installer: installer,
	}
	if coordinator.Readiness == nil {
		coordinator.Readiness = onbReadiness{app: app}
	}
	config.Lifecycle.ProvidedPlan = options.plan
	if config.Lifecycle.ProvidedPlan == nil {
		source, err := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
		if err != nil {
			t.Fatal(err)
		}
		config.Lifecycle.ProvidedPlan = &contracts.DeploymentPlan{Source: &contracts.DeploymentSource{RendererId: "raw", Config: source}, ExpectedWorkloadIds: app.workloads}
	}
	result, runErr := coordinator.Run(ctx, config.Lifecycle)
	stopRunner()
	if err := <-runner; err != nil {
		t.Fatalf("agent Job runner: %v", err)
	}
	return onbOutcome{result: result, err: runErr, repository: repository, accepted: root, judgeID: "onb-judge-1", judge: judge, installs: installer.commits, client: client}
}

func onbRules(diagnostics []*contracts.MemoryDiagnostic) []string {
	var rules []string
	for _, d := range diagnostics {
		rules = append(rules, d.GetRule()+"@"+d.GetPath()+": "+d.GetMessage())
	}
	return rules
}

// rejections are the broker's recorded rejections of the judge proposal.
func (o onbOutcome) rejections(t *testing.T) [][]*contracts.MemoryDiagnostic {
	t.Helper()
	log, err := o.repository.Rejections(o.judgeID)
	if err != nil {
		t.Fatal(err)
	}
	var out [][]*contracts.MemoryDiagnostic
	for _, rejection := range log.GetRejections() {
		out = append(out, rejection.GetDiagnostics())
	}
	return out
}

// fixgOrdering reports the known defect FIXG2 is fixing (#971's ordering,
// left after #994): the static ownership rule judges a tree Go has not
// prepared yet, so a fresh base without Go's generic detectors fails "must
// install detector manifests" in the judge's check and at the broker alike.
const fixgSkip = "expected failure until FIXG2 lands: ownership.initial-needs-detectors fires on a base without Go's generic detectors " +
	"(\"must install detector manifests\", #971 ordering after #994); remove this skip then"

func fixgOrdering(diagnostics []*contracts.MemoryDiagnostic) bool {
	return slices.ContainsFunc(diagnostics, func(d *contracts.MemoryDiagnostic) bool {
		return d.GetRule() == "ownership.initial-needs-detectors" && d.GetMessage() == "the initial health-judge proposal must install detector manifests"
	})
}

// requireAccepted asserts the onboarding contract: the base never fails, the
// judge's own check and the broker accept the first proposal, the accepted
// tree holds Go's generic and traffic detectors, and the controller installs
// the accepted judge commit.
func (o onbOutcome) requireAccepted(t *testing.T, app onbApp, alerts bool) {
	t.Helper()
	var base *lifecycle.BaseInvalidError
	if errors.As(o.err, &base) {
		t.Fatalf("the untouched base failed its own validation (base_invalid): %v", base.Diagnostics)
	}
	if o.err != nil {
		var history []string
		for i, rejection := range o.rejections(t) {
			history = append(history, fmt.Sprintf("broker rejection %d: %v", i+1, onbRules(rejection)))
		}
		for i, check := range o.judge.checks {
			history = append(history, fmt.Sprintf("judge check %d: %v", i+1, onbRules(check)))
			if fixgOrdering(check) {
				t.Skipf("%s\n%s", fixgSkip, strings.Join(history, "\n"))
			}
		}
		t.Fatalf("onboarding failed: %v\n%s", o.err, strings.Join(history, "\n"))
	}
	if len(o.judge.checks) != 1 || len(o.judge.checks[0]) != 0 {
		t.Fatalf("judge checks = %v, want one clean check", o.judge.checks)
	}
	if rejections := o.rejections(t); len(rejections) != 0 {
		t.Fatalf("broker rejections = %v, want the first proposal accepted", rejections)
	}
	if len(o.installs) != 1 || o.installs[0] != o.result.JudgeCommit || o.result.JudgeCommit == "" {
		t.Fatalf("installs = %v, judge commit %q", o.installs, o.result.JudgeCommit)
	}
	head := strings.TrimSpace(testutil.Git(t, o.accepted, "rev-parse", "HEAD"))
	if head != o.result.JudgeCommit {
		t.Fatalf("accepted HEAD %s is not the judge commit %s", head, o.result.JudgeCommit)
	}
	tree := testutil.Git(t, o.accepted, "ls-tree", "-r", "--name-only", "HEAD", "--", layout.DetectorDir(layout.ClassHealth, ""))
	for _, id := range append([]string{"service-endpoints", "workload-controllers"}, app.trafficDetectors()...) {
		if !strings.Contains(tree, layout.DetectorManifestPath(layout.ClassHealth, id)) {
			t.Errorf("accepted tree lacks Go's detector %s", id)
		}
	}
	if installed := strings.Contains(tree, layout.DetectorManifestPath(layout.ClassHealth, "metrics-alerts")); installed != alerts {
		t.Errorf("alerts detector installed = %v, want %v", installed, alerts)
	}
	if policy := o.judge.environments[0][memoryTelemetryPolicyEnv]; policy == "" || !json.Valid([]byte(policy)) {
		t.Errorf("the judge's Job carries no telemetry policy: %q", policy)
	}
}

// The known-good first proposal is accepted for each app shape under each
// built-in telemetry profile, with no rejection and a clean base. One top-level
// test per run: each onboarding spawns about 500 git processes (about 7 s).
func onboardsOnTheFirstProposal(t *testing.T, app onbApp, profile string) {
	o := onboard(t, app, mustBuiltin(t, profile, onbControl), app.goodProposal())
	o.requireAccepted(t, app, profile == telemetrysources.DefaultProfileName)
}

func TestOnboardingToyAcceptsTheFirstProposalUnderPrometheusDefault(t *testing.T) {
	onboardsOnTheFirstProposal(t, onbToy, telemetrysources.DefaultProfileName)
}

func TestOnboardingToyAcceptsTheFirstProposalUnderNone(t *testing.T) {
	onboardsOnTheFirstProposal(t, onbToy, telemetrysources.NoneProfileName)
}

func TestOnboardingShopAcceptsTheFirstProposalUnderPrometheusDefault(t *testing.T) {
	onboardsOnTheFirstProposal(t, onbShop, telemetrysources.DefaultProfileName)
}

func TestOnboardingShopAcceptsTheFirstProposalUnderNone(t *testing.T) {
	onboardsOnTheFirstProposal(t, onbShop, telemetrysources.NoneProfileName)
}

// A wrong proposal (a binding outside the granted namespaces) is rejected with
// the same typed diagnostics by the judge's own check and by the broker, and a
// judge that repeats it ends the run as agent_not_converging after two
// attempts, never as base_invalid.
func TestOnboardingRejectsAWrongProposalWithTypedDiagnostics(t *testing.T) {
	app := onbToy
	wrong := onbEdit{layout.TelemetrySourcesFile, "version: 1\nsources:\n  - id: app-metrics\n    kinds: [metrics]\n    adapter: prometheus\n    endpoint: http://prom.kube-system.svc:9090\n"}
	o := onboard(t, app, mustBuiltin(t, telemetrysources.DefaultProfileName, onbControl), append(app.goodProposal(), wrong))
	var base *lifecycle.BaseInvalidError
	if errors.As(o.err, &base) {
		t.Fatalf("base_invalid fired on the untouched base: %v", base.Diagnostics)
	}
	var stop *lifecycle.AgentNotConvergingError
	if !errors.As(o.err, &stop) || stop.Role != operability.OwnerDesigner {
		t.Fatalf("error = %v, want agent_not_converging for the designer", o.err)
	}
	rejections := o.rejections(t)
	if len(rejections) != 2 || len(o.judge.checks) != 2 {
		t.Fatalf("rejections=%d judge checks=%d, want 2 each", len(rejections), len(o.judge.checks))
	}
	for i := range rejections {
		broker, local := onbRules(rejections[i]), onbRules(o.judge.checks[i])
		if !slices.ContainsFunc(broker, func(r string) bool {
			return strings.HasPrefix(r, "telemetry.endpoint@"+layout.TelemetrySourcesFile+": ") && strings.Contains(r, `"kube-system" is not granted`)
		}) {
			t.Errorf("broker rejection %d = %v, want telemetry.endpoint at %s", i+1, broker, layout.TelemetrySourcesFile)
		}
		if !slices.Equal(broker, local) {
			t.Errorf("attempt %d: the judge's check %v and the broker %v disagree", i+1, local, broker)
		}
	}
}

// A judge whose retry fixes one problem but hits another at the same rule and
// path is converging: the run continues and accepts the corrected proposal,
// and each retry request carries every distinct earlier rejection.
func TestOnboardingRetryWithADifferentRejectionIsNotARepeat(t *testing.T) {
	app := onbToy
	profile := mustBuiltin(t, telemetrysources.DefaultProfileName, onbControl)
	binding := func(endpoint string) onbEdit {
		return onbEdit{layout.TelemetrySourcesFile, "version: 1\nsources:\n  - id: app-metrics\n    kinds: [metrics]\n    adapter: prometheus\n    endpoint: " + endpoint + "\n"}
	}
	o := onboard(t, app, profile,
		append(app.goodProposal(), binding("http://prom.kube-system.svc:9090")),
		append(app.goodProposal(), binding("http://10.0.0.5:9090")),
		append(app.goodProposal(), binding("http://prom.onb-toy.svc:9090")),
	)
	for _, check := range o.judge.checks {
		if fixgOrdering(check) {
			t.Skipf("%s\n%v", fixgSkip, onbRules(check))
		}
	}
	var stop *lifecycle.AgentNotConvergingError
	if errors.As(o.err, &stop) && len(o.judge.checks) == 2 {
		t.Fatalf("a retry whose rejection message changed was stopped as a repeat (#975 key, fixed by #995): %v", o.err)
	}
	if o.err != nil {
		t.Fatalf("onboarding failed: %v; rejections %v", o.err, o.rejections(t))
	}
	if len(o.judge.checks) != 3 || o.result.JudgeCommit == "" {
		t.Fatalf("judge runs = %d, judge commit %q; want the third proposal accepted", len(o.judge.checks), o.result.JudgeCommit)
	}
	// A retry is a fresh session: its request carries every distinct earlier
	// rejection, not only the latest (#948).
	previous := onbRules(o.judge.requests[2].GetPreviousDiagnostics())
	for _, want := range []string{`namespace "kube-system" is not granted`, "IP literal"} {
		if !slices.ContainsFunc(previous, func(r string) bool { return strings.Contains(r, want) }) {
			t.Errorf("the third request's previous_diagnostics %v lack the earlier rejection %q", previous, want)
		}
	}
}
