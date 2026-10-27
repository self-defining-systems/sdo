package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// hostPortConflict is the playbook reflection would author for the slot-17
// ServicePortConflict fault: the live Deployment contradicts accepted source
// while its Pod cannot schedule, and accepted source is the repair.
type hostPortConflict struct{}

func (hostPortConflict) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "service-port-conflict", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}

func (hostPortConflict) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["deployment"]
	diff, err := v.DiffAgainstSource(ref)
	if err != nil {
		return program.Verdict{}, err
	}
	if !diff.Changed {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	for _, deployment := range v.Deployments() {
		if deployment.Namespace != ref.Namespace || deployment.Name != ref.Name || deployment.Spec.Selector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		if err != nil {
			return program.Verdict{}, err
		}
		for _, pod := range v.Pods() {
			if pod.Namespace != ref.Namespace || !selector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse {
					return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "accepted source differs and the selected Pod cannot schedule", Observed: condition.Reason, Expected: "accepted source"}}}, nil
				}
			}
		}
	}
	return program.Verdict{Result: program.Unknown}, nil
}

func (p hostPortConflict) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	verdict, err := p.Diagnose(ctx, v, b)
	if err != nil || verdict.Result != program.Match {
		return program.Plan{}, err
	}
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: b.Objects["deployment"]}}}, nil
}

// unschedulable is the health detector for this fixture: any Pending Pod.
type unschedulable struct{}

func (unschedulable) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "unschedulable", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Playbooks: []string{"base"}, OriginatingCommit: "seed"}
}

func (unschedulable) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, deployment := range view.Deployments() {
		for _, container := range deployment.Spec.Template.Spec.Containers {
			for _, port := range container.Ports {
				if port.HostPort != 0 {
					ref := sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment)
					findings = append(findings, sdk.Finding{DetectorID: "unschedulable", RuleID: "host-port", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "host port blocks scheduling", PrimaryResource: ref})
				}
			}
		}
	}
	return findings, nil
}

// TestHermeticAdoptedHostPortRepairLearnsAndReplays runs the whole learning
// loop for an application adopted with install-existing (tag images, never
// deployed by SDO): adoption evidence, the controller's rendered lock, a deploy
// between acceptance and the fault, the first encounter's DiffAgainstSource
// recordings, reflection's replay gate, and a repeat that applies the learned
// plan with no responder.
func TestHermeticAdoptedHostPortRepairLearnsAndReplays(t *testing.T) {
	ctx := context.Background()
	repo := nativeRepository(t)
	manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: search}\nspec:\n  replicas: 1\n  selector: {matchLabels: {app: search}}\n  template:\n    metadata: {labels: {app: search}}\n    spec:\n      containers:\n      - name: hotel-reserv-search\n        image: example/hotelreservation:latest\n        ports:\n        - {containerPort: 8082}\n"
	if err := os.MkdirAll(filepath.Join(repo, "kubernetes"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "kubernetes/search.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, repo, "add", ".")
	nativeGit(t, repo, "commit", "-qm", "adopted hotel search")
	base := nativeGit(t, repo, "rev-parse", "HEAD")
	paths := []string{"kubernetes/search.yaml"}

	// The running application, deployed by someone other than SDO.
	one := int32(1)
	labelsOf := map[string]string{"app": "search"}
	live := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "search", Namespace: "app", Generation: 1, UID: "search-uid", ResourceVersion: "1"},
		Spec:       appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labelsOf}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labelsOf}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "hotel-reserv-search", Image: "example/hotelreservation:latest", Ports: []corev1.ContainerPort{{ContainerPort: 8082, Protocol: corev1.ProtocolTCP}}}}}}},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "search-1", Namespace: "app", Labels: labelsOf}, Spec: live.Spec.Template.Spec, Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "hotel-reserv-search", Image: "example/hotelreservation:latest", Ready: true}}}}
	client := fake.NewSimpleClientset(live, pod)
	sourceClient := bridgeFake(t, client)

	workspace := t.TempDir()
	authority, err := broker.New(repo, workspace, func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	// Controller start: publish the first rendered lock.
	source := GitPlanSource{Repository: repo, ManifestPaths: paths}
	locked, err := (RenderedLockReconciler{Source: source, Client: client, Broker: authority}).Reconcile(ctx, base, "app")
	if err != nil {
		t.Fatal(err)
	}

	// Ownership adoption: the controller takes the declared fields and records
	// what the accepted object looked like, the point additions count from.
	controllerConfig := testControllerConfig()
	controllerConfig.Namespace, controllerConfig.SourceCommit = "app", locked
	controller, err := NewController(controllerConfig, nil, fastloopObjects{client: client}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Source: source, Client: sourceClient, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: locked, Client: sourceClient}
	}})
	if err := controller.adoptOwnership(ctx, time.Unix(11, 0).UTC(), sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatalf("ownership adoption: %v", err)
	}
	adopted, err := client.AppsV1().Deployments("app").Get(ctx, "search", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}

	// A deploy between acceptance and the fault: the deployer applies the
	// rendered accepted source, which never carries the acceptance record.
	rendered, err := source.AcceptedManifest(ctx, locked, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyAcceptedSource(ctx, sourceClient, ref, rendered, false); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	adopted, err = client.AppsV1().Deployments("app").Get(ctx, "search", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if record, ok := adopted.Annotations[acceptedPathsAnnotation]; !ok || record == "" {
		t.Fatal("deploy lost the acceptance record")
	}
	live = adopted.DeepCopy()

	// First encounter: ServicePortConflict injects a hostPort; the Pod pends.
	faulted := live.DeepCopy()
	faulted.Spec.Template.Spec.Containers[0].Ports[0].HostPort = 9100
	if _, err := client.AppsV1().Deployments("app").Update(ctx, faulted, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pending := pod.DeepCopy()
	pending.Spec = faulted.Spec.Template.Spec
	pending.Status = corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable"}}}
	if _, err := client.CoreV1().Pods("app").Update(ctx, pending, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	provider := fastloopObjects{client: client}
	capture := func() sdktest.Snapshot {
		t.Helper()
		snapshot, err := provider.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		captured, err := sdktest.CaptureSnapshot(snapshot, "app")
		if err != nil {
			t.Fatal(err)
		}
		return captured
	}
	open := capture()
	diff, err := (LiveReadView{DetectionContext: open, Source: source, SourceCommit: locked, Client: sourceClient}).DiffAgainstSource(ref)
	if err != nil {
		t.Fatalf("first encounter cannot compare adopted Deployment with accepted source: %v", err)
	}
	if !diff.Changed {
		t.Fatalf("added hostPort not reported as accepted-source drift: %+v", diff)
	}

	// The responder's manual repair returns the live object to source.
	if _, err := client.AppsV1().Deployments("app").Update(ctx, live.DeepCopy(), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	fixed := capture()

	// Reflection: the learned playbook must pass the replay gate on the
	// recorded accepted-source reads before the broker can publish it.
	p := hostPortConflict{}
	corpus := sdktest.CorpusCase{Open: open, Fixed: fixed, Healthy: fixed, Bindings: program.Bindings{Objects: program.ObjectBindings{"deployment": ref}}}
	for phase, snapshot := range map[string]sdktest.Snapshot{"open": open, "fixed": fixed} {
		recorder := &sdktest.RecordingReadView{ReadView: LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: locked, Client: sourceClient}}
		if _, err := recorder.DiffAgainstSource(ref); err != nil {
			t.Fatal(err)
		}
		for _, call := range recorder.Calls {
			call.Phase = phase
			corpus.Recordings = append(corpus.Recordings, call)
		}
	}
	if err := sdktest.CheckMitigationRepairs(p, p, corpus); err != nil {
		t.Fatalf("reflection replay gate rejects the learned playbook: %v", err)
	}

	// Repeat: the same fault; the learned plan runs with no responder.
	if _, err := client.AppsV1().Deployments("app").Update(ctx, faulted.DeepCopy(), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	repeat := capture()
	view := LiveReadView{DetectionContext: repeat, Source: source, SourceCommit: locked, Client: sourceClient}
	bindings := program.Bindings{Objects: program.ObjectBindings{"deployment": ref}}
	plan, err := p.Mitigate(ctx, view, bindings)
	if err != nil || len(plan.Actions) != 1 {
		t.Fatalf("learned playbook did not plan on the repeat: %+v %v", plan, err)
	}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: source, Rollouts: &planFakeRollout{}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{unschedulable{}}, Provider: provider}}
	result, err := executor.Apply(ctx, IncidentRequest{IncidentID: "repeat", Namespace: "app", SourceCommit: locked, RepairPolicy: "recorded-actions"}, plan, false)
	if err != nil || !result.Applied || !result.VerificationPassed {
		t.Fatalf("learned plan did not repair the repeat: %+v %v", result, err)
	}
	after, err := client.AppsV1().Deployments("app").Get(ctx, "search", metav1.GetOptions{})
	if err != nil || after.Spec.Template.Spec.Containers[0].Ports[0].HostPort != 0 || after.Spec.Template.Spec.Containers[0].Image != "example/hotelreservation:latest" {
		t.Fatalf("repeat did not restore accepted source: %+v %v", after, err)
	}
	verdict, err := p.Diagnose(ctx, LiveReadView{DetectionContext: capture(), Source: source, SourceCommit: locked, Client: sourceClient}, bindings)
	if err != nil || verdict.Result != program.NoMatch {
		t.Fatalf("source contradiction remains after the repeat: %+v %v", verdict, err)
	}
}
