package replay

import (
	"context"
	"encoding/json"
	"fmt"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

type fixtureProgram struct {
	mode  string
	wrong bool
}

func (p fixtureProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: p.mode} }
func (p fixtureProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	bad := false
	switch p.mode {
	case "field":
		for _, s := range v.Services() {
			bad = bad || s.Spec.Selector["app"] != "api"
		}
	case "create":
		bad = len(v.ConfigMaps()) == 0
	case "delete":
		bad = len(v.NetworkPolicies()) != 0
	case "scale":
		for _, d := range v.Deployments() {
			bad = bad || d.Spec.Replicas == nil || *d.Spec.Replicas != 2
		}
	}
	if bad {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (p fixtureProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	var a program.Action
	switch p.mode {
	case "field":
		path := "spec.selector.app"
		if p.wrong {
			path = "spec.selector.other"
		}
		a = program.SetField{Ref: ref, Path: path, Value: program.FromSource(ref, "spec.selector.app")}
	case "create":
		ref.Kind = "ConfigMap"
		a = program.CreateFromSource{Ref: ref, Reason: "restore synthetic fixture"}
	case "delete":
		ref.Kind = "NetworkPolicy"
		ref.APIVersion = "networking.k8s.io/v1"
		a = program.DeleteObject{Ref: ref, Reason: "remove synthetic fault"}
	case "scale":
		ref.Kind = "Deployment"
		ref.APIVersion = "apps/v1"
		a = program.ScaleWorkload{Ref: ref, Replicas: program.FromSource(ref, "spec.replicas")}
	}
	return program.Plan{Actions: []program.Action{a}}, nil
}
func fixture(mode string) Case {
	meta := metav1.ObjectMeta{Name: "api", Namespace: "app"}
	open := sdktest.Snapshot{NamespaceName: "app"}
	healthy := sdktest.Snapshot{NamespaceName: "app"}
	source := []*unstructured.Unstructured{}
	switch mode {
	case "field":
		s := corev1.Service{ObjectMeta: meta, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "wrong", "untouched": "keep"}}}
		open.ServiceList = []corev1.Service{s}
		s = *s.DeepCopy()
		s.Spec.Selector["app"] = "api"
		healthy.ServiceList = []corev1.Service{s}
		source = append(source, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "app"}, "spec": map[string]any{"selector": map[string]any{"app": "api", "untouched": "keep"}}}})
	case "create":
		healthy.ConfigMapList = []corev1.ConfigMap{{ObjectMeta: meta, Data: map[string]string{"setting": "synthetic"}}}
		source = append(source, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "api", "namespace": "app"}, "data": map[string]any{"setting": "synthetic"}}})
	case "delete":
		open.NetworkPolicyList = []networkingv1.NetworkPolicy{{ObjectMeta: meta}}
	case "scale":
		zero, two := int32(0), int32(2)
		open.DeploymentList = []appsv1.Deployment{{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{Replicas: &zero}}}
		healthy.DeploymentList = []appsv1.Deployment{{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{Replicas: &two}}}
		source = append(source, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "api", "namespace": "app"}, "spec": map[string]any{"replicas": int64(2)}}})
	}
	return Case{ID: mode, Corpus: sdktest.CorpusCase{Open: open, Healthy: healthy, Fixed: healthy, RootCauseProgramIDs: []string{mode}}, Accepted: source}
}
func TestFromSourceSetFieldRepairs(t *testing.T) {
	p := fixtureProgram{mode: "field"}
	r := Check(context.Background(), p, p, nil, fixture("field"))
	if r.Status != "replayed" {
		t.Fatalf("%+v", r)
	}
}
func TestWrongPathFixCaught(t *testing.T) {
	p := fixtureProgram{mode: "field", wrong: true}
	r := Check(context.Background(), p, p, nil, fixture("field"))
	if r.Status != "failed" && r.Status != "refused" {
		t.Fatalf("%+v", r)
	}
}
func TestCreateDeleteScaleRepair(t *testing.T) {
	for _, mode := range []string{"create", "delete", "scale"} {
		t.Run(mode, func(t *testing.T) {
			p := fixtureProgram{mode: mode}
			r := Check(context.Background(), p, p, nil, fixture(mode))
			if r.Status != "replayed" {
				t.Fatalf("%+v", r)
			}
		})
	}
}

type broadDetector struct{}

func (broadDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "broad", Playbooks: []string{"field"}}
}
func (broadDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{{Status: sdk.FindingActive}}, nil
}
func TestMatrixFlagsOverbroadDetector(t *testing.T) {
	p := fixtureProgram{mode: "field"}
	reg, _ := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	rows := Matrix(context.Background(), reg, []sdk.Detector{broadDetector{}}, []Case{fixture("create")})
	if len(rows) == 0 || len(rows[0].FalsePositives) == 0 {
		t.Fatalf("%+v", rows)
	}
}

func TestMatrixEvaluatesUnlinkedDetectors(t *testing.T) {
	reg, _ := program.NewRegistry(nil, nil)
	rows := Matrix(context.Background(), reg, []sdk.Detector{broadDetector{}}, []Case{fixture("create")})
	if len(rows) == 0 || len(rows[0].FalsePositives) == 0 {
		t.Fatalf("unlinked detector silently omitted: %+v", rows)
	}
}

func TestCaseIsImmutableAcrossReplay(t *testing.T) {
	c := fixture("field")
	before := c.Corpus.Open.ServiceList[0].DeepCopy()
	p := fixtureProgram{mode: "field"}
	for i := 0; i < 2; i++ {
		r := Check(context.Background(), p, p, nil, c)
		if r.Status != "replayed" {
			t.Fatalf("%+v", r)
		}
	}
	if c.Corpus.Open.ServiceList[0].Spec.Selector["app"] != before.Spec.Selector["app"] {
		t.Fatal("open fixture changed")
	}
}

type runtimeProgram struct{ mode string }

func (p runtimeProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.mode, Params: []program.ParamSpec{{Name: "command", ValueKind: program.StringValue, Default: "/bin/true"}}}
}
func (p runtimeProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	bad := false
	switch p.mode {
	case "exec", "restart", "deletepod":
		for _, pod := range v.Pods() {
			for _, s := range pod.Status.ContainerStatuses {
				bad = bad || !s.Ready
			}
		}
		if len(v.Pods()) == 0 {
			return program.Verdict{Result: program.Unknown}, nil
		}
	case "reconcile", "rollback":
		for _, s := range v.Services() {
			bad = bad || s.Spec.Selector["app"] != "api"
		}
	}
	if bad {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (p runtimeProgram) Mitigate(_ context.Context, v program.ReadView, _ program.Bindings) (program.Plan, error) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-1"}
	var a program.Action
	switch p.mode {
	case "exec":
		a = program.Exec{Pod: ref, Container: "app", Command: []program.Value{program.FromParam("command")}}
	case "deletepod":
		a = program.DeletePod{Ref: ref}
	case "restart":
		ref.Kind = "Deployment"
		ref.APIVersion = "apps/v1"
		ref.Name = "api"
		a = program.RestartRollout{Ref: ref, Reason: "recreate stale pods"}
	case "reconcile":
		ref.Kind = "Service"
		ref.Name = "api"
		a = program.ReconcileFromSource{Ref: ref}
	case "rollback":
		ref.Kind = "Service"
		ref.Name = "api"
		a = program.Rollback{Ref: ref, ToRevision: "previous"}
	}
	return program.Plan{Actions: []program.Action{a}}, nil
}
func runtimeFixture(mode string) Case {
	if mode == "reconcile" || mode == "rollback" {
		c := fixture("field")
		c.ID = mode
		c.Corpus.RootCauseProgramIDs = []string{mode}
		c.Revisions = map[string][]*unstructured.Unstructured{"previous": c.Accepted}
		return c
	}
	c := Case{ID: mode, Corpus: sdktest.CorpusCase{RootCauseProgramIDs: []string{mode}, Open: sdktest.Snapshot{NamespaceName: "app"}, Healthy: sdktest.Snapshot{NamespaceName: "app"}}}
	yes := true
	meta := metav1.ObjectMeta{Name: "api-1", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "api-rs", Controller: &yes}}}
	pod := corev1.Pod{ObjectMeta: meta, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "synthetic:1"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "app", Ready: false}}}}
	c.Corpus.Open.PodList = []corev1.Pod{pod}
	pod = *pod.DeepCopy()
	pod.Status.ContainerStatuses[0].Ready = true
	c.Corpus.Healthy.PodList = []corev1.Pod{pod}
	deployment := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: pod.Spec}}}
	rs := appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "api-rs", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "api", Controller: &yes}}}, Spec: appsv1.ReplicaSetSpec{Template: corev1.PodTemplateSpec{Spec: pod.Spec}}}
	c.Corpus.Open.DeploymentList = []appsv1.Deployment{deployment}
	c.Corpus.Open.ReplicaSetList = []appsv1.ReplicaSet{rs}
	c.Corpus.Healthy.DeploymentList = []appsv1.Deployment{deployment}
	c.Corpus.Healthy.ReplicaSetList = []appsv1.ReplicaSet{rs}
	return c
}
func TestExecWithoutRBACRefused(t *testing.T) {
	p := runtimeProgram{mode: "exec"}
	r := Check(context.Background(), p, p, nil, runtimeFixture("exec"))
	if r.Status != "refused" || len(r.Actions) != 1 || r.Actions[0].Status != "refused" {
		t.Fatalf("%+v", r)
	}
}
func TestReconcileAndRollbackUseExplicitSource(t *testing.T) {
	for _, mode := range []string{"reconcile", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			p := runtimeProgram{mode: mode}
			r := Check(context.Background(), p, p, nil, runtimeFixture(mode))
			if r.Status != "replayed" {
				t.Fatalf("%+v", r)
			}
		})
	}
}
func TestDeletePodAndRestartSimulateRecreation(t *testing.T) {
	for _, mode := range []string{"deletepod", "restart"} {
		t.Run(mode, func(t *testing.T) {
			p := runtimeProgram{mode: mode}
			r := Check(context.Background(), p, p, nil, runtimeFixture(mode))
			if r.Status != "replayed" {
				t.Fatalf("%+v", r)
			}
			found := false
			for _, effect := range r.Effects {
				if len(effect) >= 10 && effect[:10] == "simulated:" {
					found = true
				}
			}
			if !found {
				t.Fatal("controller simulation not disclosed")
			}
		})
	}
}

type diffProgram struct{ fixtureProgram }

func (p diffProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	diff, err := v.DiffAgainstSource(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"})
	if err != nil {
		return program.Verdict{}, err
	}
	result := program.NoMatch
	if diff.Changed {
		result = program.Match
	}
	return program.Verdict{Result: result}, nil
}
func TestPostSourceDiffUsesMutatedCluster(t *testing.T) {
	p := diffProgram{fixtureProgram{mode: "field"}}
	r := Check(context.Background(), p, p, nil, fixture("field"))
	if r.Status != "replayed" {
		t.Fatalf("%+v", r)
	}
}

type probeProgram struct{ fixtureProgram }

func (p probeProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	answer, err := v.ProbeTCP(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, 80)
	if err != nil {
		return program.Verdict{}, err
	}
	result := program.Match
	if answer.Success {
		result = program.NoMatch
	}
	return program.Verdict{Result: result}, nil
}
func TestFixedProbeAnswersNeverProveRepair(t *testing.T) {
	p := probeProgram{fixtureProgram{mode: "field"}}
	c := fixture("field")
	args := json.RawMessage(`[{"api_version":"v1","kind":"Service","namespace":"app","name":"api"},80]`)
	c.Corpus.Recordings = []sdktest.PrimitiveCall{{Method: "ProbeTCP", Arguments: args, Result: json.RawMessage(`{"success":false}`)}, {Phase: "fixed", Method: "ProbeTCP", Arguments: args, Result: json.RawMessage(`{"success":true}`)}}
	r := Check(context.Background(), p, p, nil, c)
	if r.Status != "failed" || r.Post.Result != program.Unknown {
		t.Fatalf("fixed answer fabricated recovery: %+v", r)
	}
}

type webhookProgram struct{}

func (webhookProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "webhook", Params: []program.ParamSpec{{Name: "webhook", Kind: "MutatingWebhookConfiguration", Required: true}}}
}
func (webhookProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	o, err := sdk.ScopedSource(v).MutatingWebhookConfiguration(b.Objects["webhook"].Name)
	if err != nil {
		return program.Verdict{}, err
	}
	r := program.NoMatch
	if o != nil {
		r = program.Match
	}
	return program.Verdict{Result: r}, nil
}
func (webhookProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.DeleteObject{Ref: b.Objects["webhook"], Reason: "remove injected synthetic webhook"}}}, nil
}
func TestScopedWebhookDeleteRefreshesObservation(t *testing.T) {
	target := sdk.ObservationTarget{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "synthetic-webhook"}
	scoped := sdk.ScopedSnapshot{Version: 1, Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}, Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}}}
	healthy := sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: scoped}
	open := healthy
	open.ScopedSnapshot.Mutating = []admissionv1.MutatingWebhookConfiguration{{ObjectMeta: metav1.ObjectMeta{Name: target.Name}}}
	c := Case{ID: "webhook", Corpus: sdktest.CorpusCase{Open: open, Healthy: healthy, Fixed: healthy, RootCauseProgramIDs: []string{"webhook"}, Bindings: program.Bindings{Objects: program.ObjectBindings{"webhook": {APIVersion: target.APIVersion, Kind: target.Kind, Name: target.Name}}}}}
	p := webhookProgram{}
	r := Check(context.Background(), p, p, nil, c)
	if r.Status != "replayed" {
		t.Fatalf("%+v", r)
	}
}

func TestFiftyCaseTiming(t *testing.T) {
	p := fixtureProgram{mode: "field"}
	registry, _ := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	cases := make([]Case, 50)
	for i := range cases {
		cases[i] = fixture("field")
		cases[i].ID = fmt.Sprintf("synthetic-field-%02d", i)
	}
	start := time.Now()
	rows := Matrix(context.Background(), registry, nil, cases)
	elapsed := time.Since(start)
	if len(rows) != 50 {
		t.Fatalf("got %d checks", len(rows))
	}
	for _, r := range rows {
		if r.Status != "replayed" {
			t.Fatalf("%+v", r)
		}
	}
	t.Logf("50 synthetic in-memory cases in %s (%.3f ms/case after build)", elapsed, float64(elapsed)/float64(time.Millisecond)/50)
}

type serviceCreateProgram struct{}

func (serviceCreateProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "create-service"}
}
func (serviceCreateProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	r := program.NoMatch
	if len(v.Services()) == 0 {
		r = program.Match
	}
	return program.Verdict{Result: r}, nil
}
func (serviceCreateProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.CreateFromSource{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Reason: "restore synthetic service"}}}, nil
}
func TestCreateServiceNormalizesFakeStorageDefaults(t *testing.T) {
	c := fixture("field")
	c.ID = "create-service"
	c.Corpus.RootCauseProgramIDs = []string{c.ID}
	c.Corpus.Open.ServiceList = nil
	p := serviceCreateProgram{}
	r := Check(context.Background(), p, p, nil, c)
	if r.Status != "replayed" {
		t.Fatalf("%+v", r)
	}
}

type wrongExistingPathProgram struct{ fixtureProgram }

func (p wrongExistingPathProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	return program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.selector.untouched", Value: program.FromSource(ref, "spec.selector.untouched")}}}, nil
}
func TestWrongExistingPathExecutesAndFailsPostCheck(t *testing.T) {
	p := wrongExistingPathProgram{fixtureProgram{mode: "field"}}
	r := Check(context.Background(), p, p, nil, fixture("field"))
	if r.Status != "failed" || len(r.Actions) != 1 || r.Actions[0].Status != "applied" || r.Post.Result != program.Match {
		t.Fatalf("wrong fix escaped post-check: %+v", r)
	}
}
