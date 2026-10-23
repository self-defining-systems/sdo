package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

func TestScopedAvailabilitySeededDenialRegrant(t *testing.T) {
	for seed := int64(0); seed < 16; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			targets := []sdk.ObservationTarget{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "a"}, {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "b"}}
			scope := sdk.ObservationScope{Version: 1, Targets: targets}
			one := int32(1)
			client := fake.NewSimpleClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "same", Namespace: "a"}, Spec: appsv1.DeploymentSpec{Replicas: &one}}, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "same", Namespace: "b"}, Spec: appsv1.DeploymentSpec{Replicas: &one}})
			denied := map[string]bool{}
			client.PrependReactor("list", "deployments", func(a ktesting.Action) (bool, kruntime.Object, error) {
				if denied[a.GetNamespace()] {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "", nil)
				}
				return false, nil, nil
			})
			tracker := NewStateTracker(StateTrackerConfig{Client: client, Namespace: "app", Scope: scope, Ceiling: scope, Settle: time.Second})
			defer tracker.Stop()
			if err := tracker.Start(ctx); err != nil {
				t.Fatal(err)
			}
			clock := time.Unix(0, 0)
			cache, err := NewKubernetesCache(KubernetesCacheConfig{Now: func() time.Time { clock = clock.Add(time.Minute); return clock }, Namespace: "app", Client: client, Scope: scope, Ceiling: scope, TrackDrift: true, Generation: "commit"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			cache.scopedTracker = tracker
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			if _, err = cache.Snapshot(ctx); err != nil {
				t.Fatal(err)
			}
			tracker.Observe(now, true)
			cache.TakeObjectChanges()
			cache.TakeEvents()
			rng := rand.New(rand.NewSource(seed))
			ns := []string{"a", "b"}[rng.Intn(2)]
			denied[ns] = true
			object, err := client.AppsV1().Deployments(ns).Get(ctx, "same", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			two := int32(2)
			object.Spec.Replicas = &two
			if _, err = client.AppsV1().Deployments(ns).Update(ctx, object, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4+rng.Intn(8); i++ {
				view, err := cache.Snapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = view.(sdk.ScopedObservationSource).ScopedDeployments(ns); err == nil {
					t.Fatal("denied target appeared available")
				}
				tracker.Observe(now.Add(time.Duration(i+1)*time.Minute), true)
				changes := tracker.Changes(now)
				if len(changes.Changes) != 0 || len(changes.UnobservedKinds) != 1 {
					t.Fatalf("gap fabricated drift: %+v", changes)
				}
			}
			if len(cache.TakeObjectChanges()) != 0 {
				t.Fatal("denial emitted removal")
			}
			denied[ns] = false
			if _, err = cache.Snapshot(ctx); err != nil {
				t.Fatal(err)
			}
			changes := tracker.Changes(now.Add(time.Hour))
			if len(changes.Changes) != 1 {
				t.Fatalf("baseline promoted during gap: %+v", changes)
			}
			change := changes.Changes[0]
			if change.Namespace != ns || change.APIVersion != "apps/v1" || change.Name != "same" || change.Change != StateChangeModified {
				t.Fatalf("namespace collision: %+v", change)
			}
			encoded, _ := json.Marshal(changes)
			if strings.Contains(string(encoded), "DATABASE_URL") {
				t.Fatal("unsafe drift projection")
			}
			events := cache.TakeObjectChanges()
			if len(events) != 1 || events[0].Initial || events[0].New.GetNamespace() != ns {
				t.Fatalf("regrant did not compare retained baseline: %+v", events)
			}
		})
	}
}

func TestScopedWebhookSourceAdmissionAndRedaction(t *testing.T) {
	source, commit := foreignSource(t)
	for _, kind := range []string{"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"} {
		target := sdk.ObservationTarget{APIVersion: "admissionregistration.k8s.io/v1", Kind: kind, Name: "policy"}
		scope := sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}
		const canary = "webhook-url-credential-canary"
		url := "https://user:" + canary + "@example.org/path?key=" + canary
		scoped := sdk.ScopedSnapshot{Version: 1, Scope: scope, Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}}}
		if kind == "MutatingWebhookConfiguration" {
			scoped.Mutating = []admissionv1.MutatingWebhookConfiguration{sdk.ProjectMutatingWebhook(admissionv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "policy"}, Webhooks: []admissionv1.MutatingWebhook{{Name: "policy.example", ClientConfig: admissionv1.WebhookClientConfig{URL: &url, CABundle: []byte(canary)}}}})}
		} else {
			scoped.Validating = []admissionv1.ValidatingWebhookConfiguration{sdk.ProjectValidatingWebhook(admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "policy"}, Webhooks: []admissionv1.ValidatingWebhook{{Name: "policy.example", ClientConfig: admissionv1.WebhookClientConfig{URL: &url, CABundle: []byte(canary)}}}})}
		}
		object := scopedObjects(scoped)[target.Key()]["policy"]
		snapshot := sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: scoped}
		view := LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: commit}
		for _, initial := range []bool{true, false} {
			event, _, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{New: object, Initial: initial}, view, time.Unix(20, 0))
			if err != nil || (event != nil) == initial {
				t.Fatalf("%s initial=%v event=%+v error=%v", kind, initial, event, err)
			}
			if event != nil {
				data, _ := json.Marshal(event)
				if strings.Contains(string(data), canary) {
					t.Fatal("credential leaked into drift")
				}
			}
		}
		snapshot.ScopedSnapshot.Mutating = nil
		snapshot.ScopedSnapshot.Validating = nil
		view.DetectionContext = snapshot
		event, _, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{Old: object}, view, time.Unix(21, 0))
		if err != nil || event == nil {
			t.Fatalf("%s delete=%+v error=%v", kind, event, err)
		}
		view.SourceCommit = "missing-source-revision"
		if event, _, err = (DriftClassifier{}).Classify(context.Background(), ObjectChange{New: object}, view, time.Unix(22, 0)); err == nil || event != nil {
			t.Fatal("source error became proven absence")
		}
	}
}

type scopedIgnoringDetector struct{}

func (scopedIgnoringDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "scoped", Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "foreign"}}}
}
func (scopedIgnoringDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

type scopedIgnoringDiagnoser struct{}

func (scopedIgnoringDiagnoser) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "ignore-scoped-error"}
}
func (scopedIgnoringDiagnoser) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	_, _ = view.(sdk.ScopedObservationSource).ScopedDeployments("foreign")
	return program.Verdict{Result: program.NoMatch}, nil
}
func TestScopedWrappersKeepDenialUnknown(t *testing.T) {
	target := sdk.ObservationTarget{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "foreign"}
	scoped := sdk.ScopedSnapshot{Version: 1, Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}, Statuses: []sdk.ObservationStatus{{Target: target, State: "forbidden"}}}
	base := sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: scoped}
	live := LiveReadView{DetectionContext: base}
	recording := &sdktest.RecordingReadView{ReadView: live}
	for _, view := range []sdk.DetectionContext{base, trafficContext{DetectionContext: base, ScopedObservationSource: base}, live, recording, struct{ sdk.DetectionContext }{base}} {
		if _, err := sdk.DetectObserved(context.Background(), scopedIgnoringDetector{}, view); err == nil {
			t.Fatalf("wrapper hid unavailable dependency: %T", view)
		}
	}
	verdict := program.RunDiagnose(context.Background(), scopedIgnoringDiagnoser{}, recording, nil, time.Second)
	if verdict.Result != program.Unknown {
		t.Fatalf("ignored scoped error cleared diagnosis: %v", verdict)
	}
	replay := &sdktest.ReplayReadView{Snapshot: base, Calls: recording.Calls}
	verdict = program.RunDiagnose(context.Background(), scopedIgnoringDiagnoser{}, replay, nil, time.Second)
	if verdict.Result != program.Unknown {
		t.Fatal("scoped failure did not replay")
	}
}
func TestScopedRevocationChangesReadsAndPreservesIntersection(t *testing.T) {
	ctx := context.Background()
	targets := []sdk.ObservationTarget{{APIVersion: "v1", Kind: "Service", Namespace: "a"}, {APIVersion: "v1", Kind: "Service", Namespace: "b"}}
	ceiling := sdk.ObservationScope{Version: 1, Targets: targets}
	scope := ceiling
	revision := "one"
	client := fake.NewSimpleClientset()
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: client, Scope: scope, Ceiling: ceiling, AcceptedScope: func(context.Context) (sdk.ObservationScope, string, error) { return scope, revision, nil }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cache.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	scope = sdk.ObservationScope{Version: 1, Targets: targets[1:]}
	revision = "two"
	view, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range client.Actions() {
		if action.GetNamespace() == "a" {
			t.Fatal("revoked target was still read")
		}
	}
	if _, err = view.(sdk.ScopedObservationSource).ScopedServices("a"); err == nil {
		t.Fatal("revoked target remained available")
	}
	if len(cache.TakeObjectChanges()) != 0 {
		t.Fatal("scope edit emitted synthetic drift")
	}
}
func TestScopedCoverageGapInvalidatesSettleWithoutEvaluation(t *testing.T) {
	client := fake.NewSimpleClientset()
	target := sdk.ObservationTarget{APIVersion: "v1", Kind: "Service", Namespace: "foreign"}
	scope := sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}
	tracker := NewStateTracker(StateTrackerConfig{Client: client, Namespace: "app", Scope: scope, Ceiling: scope, Settle: time.Second})
	defer tracker.Stop()
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	scoped := sdk.ScopedSnapshot{Version: 1, Generation: "one", Scope: scope, Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}}, Services: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: "foreign"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "one"}}}}}
	now := time.Unix(100, 0)
	tracker.UpdateScoped(scoped)
	tracker.Observe(now, true)
	scoped.Services[0].Spec.Selector = map[string]string{"app": "two"}
	tracker.UpdateScoped(scoped)
	tracker.Observe(now.Add(time.Second), true)
	scoped.Statuses[0].State = "forbidden"
	tracker.UpdateScoped(scoped) // no detector evaluation and no Observe call
	scoped.Statuses[0].State = sdk.ObservationObserved
	tracker.UpdateScoped(scoped)
	tracker.Observe(now.Add(time.Hour), true)
	if len(tracker.Changes(now.Add(time.Hour)).Changes) != 1 {
		t.Fatal("settle candidate survived an unobserved interval")
	}
}
func TestScopedAggregateMissingFieldsCannotClear(t *testing.T) {
	diff := program.Diff{Changed: true, Text: `accepted: {"webhooks":[{"name":"before"}]}` + "\n" + `observed: {"webhooks":[{"name":"after"}]}`}
	if driftFieldsRestored(diff, []sdk.DriftEvent{{Fields: []sdk.DriftField{{Path: "webhooks"}}}}) {
		t.Fatal("two missing aggregate leaves established restoration")
	}
}

func TestScopedControllerAvailabilitySeeded(t *testing.T) {
	for seed := int64(0); seed < 8; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			target := sdk.ObservationTarget{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "foreign"}
			scope := sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}
			client := fake.NewSimpleClientset()
			denied := false
			client.PrependReactor("list", "deployments", func(ktesting.Action) (bool, kruntime.Object, error) {
				if denied {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "", nil)
				}
				return false, nil, nil
			})
			spec := healthSpec(sdk.WatchKind{APIVersion: target.APIVersion, Kind: target.Kind, Namespace: target.Namespace})
			spec.Interval = time.Second
			detector := scheduledDetector{spec: spec}
			clock := time.Unix(0, 0)
			cache, err := NewKubernetesCache(KubernetesCacheConfig{Now: func() time.Time { clock = clock.Add(time.Minute); return clock }, Client: client, Namespace: "app", Scope: scope, Ceiling: scope}, []sdk.Detector{detector})
			if err != nil {
				t.Fatal(err)
			}
			config := testControllerConfig()
			config.Namespace = "app"
			start := time.Unix(100, 0)
			controller, err := NewController(config, []sdk.Detector{detector}, cache, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
			if err != nil {
				t.Fatal(err)
			}
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 20; i++ {
				denied = rng.Intn(2) == 0
				if err = controller.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
					t.Fatal(err)
				}
				state, err := controller.ExportState()
				if err != nil {
					t.Fatal(err)
				}
				history := state.GetHistory()
				want := "clear"
				if denied {
					want = "error"
				}
				if len(history) == 0 || history[len(history)-1].GetStatus() != want {
					t.Fatalf("seed=%d step=%d denied=%v history=%v", seed, i, denied, history)
				}
				if i == 10 {
					restored, err := NewController(config, []sdk.Detector{detector}, cache, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
					if err != nil {
						t.Fatal(err)
					}
					if err = restored.RestoreState(state); err != nil {
						t.Fatal(err)
					}
					controller = restored
				}
			}
		})
	}
}

func TestScopedPendingWebhookDriftSurvivesDenial(t *testing.T) {
	source, commit := foreignSource(t)
	target := sdk.ObservationTarget{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "policy"}
	scope := sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), Scope: scope, Ceiling: scope, TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	observed := sdk.ScopedSnapshot{Version: 1, Scope: scope, Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}}, Mutating: []admissionv1.MutatingWebhookConfiguration{{ObjectMeta: metav1.ObjectMeta{Name: "policy"}, Webhooks: []admissionv1.MutatingWebhook{{Name: "policy.example"}}}}}
	ignore := admissionv1.Ignore
	observed.Mutating[0].Webhooks[0].FailurePolicy = &ignore
	cache.observeScoped(observed)
	cache.TakeObjectChanges()
	policy := admissionv1.Fail
	observed.Mutating[0].Webhooks[0].FailurePolicy = &policy
	cache.observeScoped(observed)
	pending := cache.TakeObjectChanges()
	if len(pending) != 1 {
		t.Fatal("no pending change")
	}
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, cache, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Events: cache, View: func(view sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: view, Source: source, SourceCommit: commit}
	}})
	denied := observed
	denied.Statuses = append([]sdk.ObservationStatus(nil), observed.Statuses...)
	denied.Statuses[0].State = "forbidden"
	controller.processDrift(context.Background(), time.Unix(101, 0), sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: denied}, pending, false)
	retained := cache.TakeObjectChanges()
	if len(retained) != 1 {
		t.Fatal("denial discarded pending drift")
	}
	cache.observeScoped(observed)
	if len(cache.TakeObjectChanges()) != 0 {
		t.Fatal("regrant itself emitted object drift")
	}
	controller.processDrift(context.Background(), time.Unix(102, 0), sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: observed}, retained, false)
	state, err := controller.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.GetDriftEvents()) != 1 {
		t.Fatal("regrant failed to admit retained drift")
	}
}
func TestScopedDeclaredSourceWebhookCannotClearWhileChanged(t *testing.T) {
	source, _ := foreignSource(t)
	path := filepath.Join(source.Repository, "app.yaml")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.WriteString("\n---\napiVersion: admissionregistration.k8s.io/v1\nkind: ValidatingWebhookConfiguration\nmetadata:\n  name: policy\nwebhooks:\n- name: policy.example\n  failurePolicy: Ignore\n  clientConfig:\n    service:\n      namespace: policy-system\n      name: backend\n\n---\napiVersion: v1\nkind: Service\nmetadata: {name: same, namespace: a}\nspec: {selector: {app: one}}\n---\napiVersion: v1\nkind: Service\nmetadata: {name: same, namespace: b}\nspec: {selector: {app: two}}\n")
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "app.yaml"}, {"commit", "-qm", "scoped source"}} {
		if out, err := exec.Command("git", append([]string{"-C", source.Repository}, args...)...).CombinedOutput(); err != nil {
			t.Fatal(string(out), err)
		}
	}
	revision, err := exec.Command("git", "-C", source.Repository, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	commit := seedRenderedLock(t, source, strings.TrimSpace(string(revision)), "app")
	ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"}
	accepted, err := source.AcceptedScopedManifest(context.Background(), commit, "app", ref)
	if err != nil || accepted.GetNamespace() != "" {
		t.Fatalf("cluster source identity: %v", err)
	}
	for _, ns := range []string{"a", "b"} {
		o, err := source.AcceptedScopedManifest(context.Background(), commit, "app", sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Name: "same", Namespace: ns})
		if err != nil || o.GetNamespace() != ns {
			t.Fatalf("qualified source collision: %v", err)
		}
	}
	policy := admissionv1.Fail
	target := sdk.ObservationTarget{APIVersion: ref.APIVersion, Kind: ref.Kind, Name: ref.Name}
	scoped := sdk.ScopedSnapshot{Version: 1, Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}, Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}}, Validating: []admissionv1.ValidatingWebhookConfiguration{{ObjectMeta: metav1.ObjectMeta{Name: "policy"}, Webhooks: []admissionv1.ValidatingWebhook{{Name: "policy.example", FailurePolicy: &policy, ClientConfig: admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Name: "backend", Namespace: "policy-system"}}}}}}}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: scoped}, Source: source, SourceCommit: commit}
	changed := scopedObjects(scoped)[target.Key()]["policy"]
	event, _, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{Old: accepted, New: changed}, view, time.Unix(200, 0))
	if err != nil || event == nil {
		t.Fatalf("declared webhook drift: %+v %v", event, err)
	}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || !diff.Changed || driftFieldsRestored(diff, []sdk.DriftEvent{*event}) {
		t.Fatalf("persistent webhook drift cleared: %v", err)
	}
}

func TestScopedRetryBackoffRequiresFreshRegrant(t *testing.T) {
	target := sdk.ObservationTarget{APIVersion: "v1", Kind: "Service", Namespace: "foreign"}
	scope := sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}
	client := fake.NewSimpleClientset()
	now := time.Unix(0, 0)
	denied := true
	reads := 0
	client.PrependReactor("list", "services", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetNamespace() != "foreign" {
			return false, nil, nil
		}
		reads++
		if denied {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "services"}, "", nil)
		}
		return false, nil, nil
	})
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: client, Scope: scope, Ceiling: scope, Now: func() time.Time { return now }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		view, err := cache.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = view.(sdk.ScopedObservationSource).ScopedServices("foreign"); err == nil {
			t.Fatal("denial appeared observed")
		}
	}
	if reads != 1 {
		t.Fatalf("backoff issued %d reads", reads)
	}
	denied = false
	now = now.Add(time.Minute)
	view, err := cache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = view.(sdk.ScopedObservationSource).ScopedServices("foreign"); err != nil || reads != 2 {
		t.Fatalf("regrant did not require fresh list: %v reads=%d", err, reads)
	}
}

func TestScopedWebhookDefaultsChangedThenRestored(t *testing.T) {
	for _, kind := range []string{"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"} {
		t.Run(kind, func(t *testing.T) {
			source, _ := foreignSource(t)
			path := filepath.Join(source.Repository, "app.yaml")
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = fmt.Fprintf(file, "\n---\napiVersion: admissionregistration.k8s.io/v1\nkind: %s\nmetadata: {name: policy}\nwebhooks:\n- name: policy.example\n  clientConfig:\n    service: {name: backend, namespace: policy-system}\n", kind)
			_ = file.Close()
			if err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{{"add", "app.yaml"}, {"commit", "-qm", "defaulted webhook source"}} {
				if out, err := exec.Command("git", append([]string{"-C", source.Repository}, args...)...).CombinedOutput(); err != nil {
					t.Fatal(string(out), err)
				}
			}
			revision, err := exec.Command("git", "-C", source.Repository, "rev-parse", "HEAD").Output()
			if err != nil {
				t.Fatal(err)
			}
			commit := seedRenderedLock(t, source, strings.TrimSpace(string(revision)), "app")
			target := sdk.ObservationTarget{APIVersion: "admissionregistration.k8s.io/v1", Kind: kind, Name: "policy"}
			ref := sdk.ObjectRef{APIVersion: target.APIVersion, Kind: target.Kind, Name: target.Name}
			fail := admissionv1.Fail
			equivalent := admissionv1.Equivalent
			never := admissionv1.NeverReinvocationPolicy
			port := int32(443)
			snapshot := func(timeout int32) sdktest.Snapshot {
				scoped := sdk.ScopedSnapshot{Version: 1, Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}, Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}}}
				client := admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Name: "backend", Namespace: "policy-system", Port: &port}}
				if kind == "MutatingWebhookConfiguration" {
					scoped.Mutating = []admissionv1.MutatingWebhookConfiguration{{ObjectMeta: metav1.ObjectMeta{Name: "policy"}, Webhooks: []admissionv1.MutatingWebhook{{Name: "policy.example", FailurePolicy: &fail, MatchPolicy: &equivalent, TimeoutSeconds: &timeout, ReinvocationPolicy: &never, NamespaceSelector: &metav1.LabelSelector{}, ObjectSelector: &metav1.LabelSelector{}, ClientConfig: client}}}}
				} else {
					scoped.Validating = []admissionv1.ValidatingWebhookConfiguration{{ObjectMeta: metav1.ObjectMeta{Name: "policy"}, Webhooks: []admissionv1.ValidatingWebhook{{Name: "policy.example", FailurePolicy: &fail, MatchPolicy: &equivalent, TimeoutSeconds: &timeout, NamespaceSelector: &metav1.LabelSelector{}, ObjectSelector: &metav1.LabelSelector{}, ClientConfig: client}}}}
				}
				return sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: scoped}
			}
			view := LiveReadView{Source: source, SourceCommit: commit}
			view.DetectionContext = snapshot(10)
			diff, err := view.DiffAgainstSource(ref)
			if err != nil || diff.Changed {
				t.Fatalf("omitted defaults manufactured source drift: %v changed=%v", err, diff.Changed)
			}
			before := scopedObjects(snapshot(10).ScopedSnapshot)[target.Key()]["policy"]
			after := scopedObjects(snapshot(20).ScopedSnapshot)[target.Key()]["policy"]
			view.DetectionContext = snapshot(20)
			event, _, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{Old: before, New: after}, view, time.Unix(100, 0))
			if err != nil || event == nil {
				t.Fatalf("default changed: %v event=%+v", err, event)
			}
			diff, err = view.DiffAgainstSource(ref)
			if err != nil || !diff.Changed || driftFieldsRestored(diff, []sdk.DriftEvent{*event}) {
				t.Fatal("changed default appeared restored")
			}
			view.DetectionContext = snapshot(10)
			diff, err = view.DiffAgainstSource(ref)
			if err != nil || diff.Changed {
				t.Fatalf("restored default remained drift: %v", err)
			}
		})
	}
}

func TestScopedCompositionPreservesObservationClock(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	target := sdk.ObservationTarget{APIVersion: "v1", Kind: "Service", Namespace: "foreign"}
	scoped := sdk.ScopedSnapshot{Version: 1, Generation: "commit", Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}, Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}}}
	base := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &now, ScopedSnapshot: scoped}
	trafficView := trafficContext{DetectionContext: base, ScopedObservationSource: base}
	live := LiveReadView{DetectionContext: trafficView}
	recorded := &sdktest.RecordingReadView{ReadView: live}
	for _, view := range []sdk.DetectionContext{trafficView, live, recorded} {
		if !sdk.ObservationTime(view).Equal(now) {
			t.Fatalf("%T hid observation clock", view)
		}
		capture, err := sdktest.CaptureSnapshot(view, "app")
		if err != nil || !sdk.ObservationTime(capture).Equal(now) || capture.ObservationGeneration() != "commit" {
			t.Fatalf("wrapper capture changed clock/scope: %T %v", view, err)
		}
	}
}
