package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type lifecycleTestClient struct {
	*planFakeClient
	creates, deletes int
	ignoreDelete     bool
}

func (c *lifecycleTestClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.objects[ref] == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: ref.Kind}, ref.Name)
	}
	return c.planFakeClient.Get(ctx, ref)
}
func (c *lifecycleTestClient) Create(_ context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if c.objects[ref] != nil {
		return nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: ref.Kind}, ref.Name)
	}
	created := object.DeepCopy()
	created.SetUID("created-uid")
	created.SetResourceVersion("2")
	c.objects[ref] = created
	c.creates++
	return created.DeepCopy(), nil
}
func (c *lifecycleTestClient) Delete(_ context.Context, ref sdk.ObjectRef, uid types.UID, version string) error {
	object := c.objects[ref]
	if object == nil {
		return apierrors.NewNotFound(schema.GroupResource{Resource: ref.Kind}, ref.Name)
	}
	if object.GetUID() != uid || object.GetResourceVersion() != version {
		return apierrors.NewConflict(schema.GroupResource{Resource: ref.Kind}, ref.Name, errors.New("identity changed"))
	}
	c.deletes++
	if !c.ignoreDelete {
		delete(c.objects, ref)
	}
	return nil
}

type lifecycleTestSource struct {
	objects map[sdk.ObjectRef]*unstructured.Unstructured
}

func (s lifecycleTestSource) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if object := s.objects[ref]; object != nil {
		return object.DeepCopy(), nil
	}
	return nil, rendered.ErrAcceptedManifestAbsent
}
func (lifecycleTestSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, errors.New("unavailable")
}

func TestPlanExecutorCreateFromSourceAndCompensate(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Service"} {
		t.Run(kind, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: kind, Namespace: "app", Name: "missing"}
			accepted := planObject(ref, map[string]any{"selector": map[string]any{"app": "api"}, "ports": []any{map[string]any{"port": float64(80)}}})
			if kind == "ConfigMap" {
				delete(accepted.Object, "spec")
				accepted.Object["data"] = map[string]any{"key": "safe"}
			}
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}}
			safety := fixturePlanSafety(t, ref, accepted)
			proof := safety.Objects[ref]
			proof.UID = ""
			proof.CreateAllowed = true
			safety.Objects[ref] = proof
			e := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: accepted}}, AcceptedRevision: func() string { return "accepted" }, Safety: safety, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions", Namespace: "app"}, program.Plan{Actions: []program.Action{program.CreateFromSource{Ref: ref, Reason: "restore accepted missing object"}}}, false)
			if err != nil || !result.VerificationPassed || client.creates != 1 {
				t.Fatalf("create: %v", err)
			}
			if len(result.ObjectEffects) != 1 || result.ObjectEffects[0].UID != "created-uid" || !result.ObjectEffects[0].Confirmed {
				t.Fatal("missing current UID receipt")
			}
			proto, err := result.Proto()
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := planApplicationState(proto).Result.Plan.Actions[0].(program.CreateFromSource); !ok {
				t.Fatal("contract lost create")
			}
			if err := e.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
			if client.objects[ref] != nil || client.deletes != 1 {
				t.Fatal("created object not compensated")
			}
		})
	}
}

func TestPlanExecutorDeleteObjectIrreversibleReceipt(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "foreign"}
	before := planObject(ref, map[string]any{"podSelector": map[string]any{}})
	before.SetUID("fixture-uid")
	before.SetResourceVersion("1")
	client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}}
	safety := fixturePlanSafety(t, ref, before)
	proof := safety.Objects[ref]
	proof.DeleteAllowed = true
	safety.Objects[ref] = proof
	e := PlanExecutor{Client: client, Source: lifecycleTestSource{}, AcceptedRevision: func() string { return "accepted" }, Safety: safety, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions", Namespace: "app"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "source-absent blocking policy"}}}, false)
	if err != nil || !result.VerificationPassed || client.deletes != 1 {
		t.Fatalf("delete: %v", err)
	}
	if len(result.ObjectEffects) != 1 || !result.ObjectEffects[0].Irreversible || result.ObjectEffects[0].DeletedPreImage == nil || !result.ObjectEffects[0].Confirmed {
		t.Fatal("missing irreversible preimage/absence receipt")
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var persisted PlanApplicationResult
	if err = json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	proto, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	decoded := planApplicationState(proto).Result
	if len(decoded.ObjectEffects) != 1 || decoded.ObjectEffects[0].DeletedPreImage == nil {
		t.Fatal("durable preimage missing")
	}
	if err := e.Rollback(context.Background(), result); err == nil {
		t.Fatal("irreversible delete claimed rollback")
	}
	if client.creates != 0 || client.objects[ref] != nil {
		t.Fatal("deleted object auto-recreated")
	}
}

func TestPlanObjectActionsRefuseKindsAndSourceContradictions(t *testing.T) {
	for _, kind := range []string{"Secret", "ServiceAccount", "Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding", "CertificateSigningRequest", "Node", "Namespace", "Deployment", "CustomResource"} {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: kind, Namespace: "app", Name: "placeholder"}
		for _, action := range []program.Action{program.CreateFromSource{Ref: ref, Reason: "fixture"}, program.DeleteObject{Ref: ref, Reason: "fixture"}} {
			if (program.Plan{Actions: []program.Action{action}}).Validate() == nil {
				t.Fatalf("forbidden kind %s accepted", kind)
			}
		}
	}
	ref := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "owned"}
	before := planObject(ref, map[string]any{"podSelector": map[string]any{}})
	before.SetUID("fixture-uid")
	before.SetResourceVersion("1")
	for _, source := range []PlanSource{lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}, planFakeSource{}} {
		client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}}
		safety := fixturePlanSafety(t, ref, before)
		proof := safety.Objects[ref]
		proof.DeleteAllowed = true
		safety.Objects[ref] = proof
		e := PlanExecutor{Client: client, Source: source, AcceptedRevision: func() string { return "accepted" }, Safety: safety, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
		if _, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "fixture"}}}, false); err == nil || client.deletes != 0 {
			t.Fatal("non-authoritative absence or source-owned delete accepted")
		}
	}
}

func TestPlanDeleteMustObserveAbsenceAndNeverRecreate(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "foreign"}
	before := planObject(ref, map[string]any{"podSelector": map[string]any{}})
	before.SetUID("fixture-uid")
	before.SetResourceVersion("1")
	client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}, ignoreDelete: true}
	safety := fixturePlanSafety(t, ref, before)
	proof := safety.Objects[ref]
	proof.DeleteAllowed = true
	safety.Objects[ref] = proof
	e := PlanExecutor{Client: client, Source: lifecycleTestSource{}, AcceptedRevision: func() string { return "accepted" }, Safety: safety, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "fixture"}}}, false)
	if err == nil || result.VerificationCompleted || result.ObjectEffects[0].Confirmed || client.creates != 0 {
		t.Fatal("missing absence evidence treated as success or recreation")
	}
}

func TestPlanDeleteNamedWebhookAndPolicyDeleteSafety(t *testing.T) {
	for _, kind := range []string{"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"} {
		t.Run(kind, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: kind, Name: "synthetic-outage"}
			object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": kind, "metadata": map[string]any{"name": ref.Name}, "webhooks": []any{map[string]any{"name": "fixture.example.invalid", "clientConfig": map[string]any{"service": map[string]any{"name": "fixture", "namespace": "app"}}, "rules": []any{map[string]any{"resources": []any{"pods"}}}}}}}
			object.SetUID("fixture-uid")
			object.SetResourceVersion("1")
			for _, allowed := range []bool{false, true} {
				client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
				safety := fixturePlanSafety(t, ref, object)
				proof := safety.Objects[ref]
				proof.DeleteAllowed = allowed
				safety.Objects[ref] = proof
				e := PlanExecutor{Client: client, Source: lifecycleTestSource{}, AcceptedRevision: func() string { return "accepted" }, Safety: safety, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
				result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "named fixture webhook outage"}}}, false)
				if (err == nil) != allowed || (!allowed && client.deletes != 0) || (allowed && !result.VerificationPassed) {
					t.Fatalf("allowed=%v err=%v", allowed, err)
				}
			}
		})
	}
}

type lifecycleFixtureProgram struct {
	action program.Action
	client *lifecycleTestClient
	ref    sdk.ObjectRef
}

func (p lifecycleFixtureProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "synthetic-object", Params: []program.ParamSpec{{Name: "target", Kind: program.Kind(p.ref.Kind), Required: true}}}
}
func (p lifecycleFixtureProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	exists := p.client.objects[p.ref] != nil
	repaired := (objectActionOperation(p.action) == "create" && exists) || (objectActionOperation(p.action) == "delete" && !exists)
	if repaired {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match}, nil
}
func (p lifecycleFixtureProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{p.action}}, nil
}

func TestResponderToolsAcceptCreateAndDelete(t *testing.T) {
	for _, operation := range []string{"create", "delete"} {
		t.Run(operation, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "synthetic"}
			object := planObject(ref, nil)
			delete(object.Object, "spec")
			object.Object["data"] = map[string]any{"key": "safe"}
			var action program.Action = program.CreateFromSource{Ref: ref, Reason: "synthetic missing accepted object"}
			source := lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}}
			if operation == "delete" {
				ref.APIVersion = "networking.k8s.io/v1"
				ref.Kind = "NetworkPolicy"
				object = planObject(ref, map[string]any{"podSelector": map[string]any{}})
				object.SetUID("fixture-uid")
				object.SetResourceVersion("1")
				client.objects[ref] = object
				source = lifecycleTestSource{}
				action = program.DeleteObject{Ref: ref, Reason: "synthetic source-absent policy"}
			}
			safety := fixturePlanSafety(t, ref, object)
			proof := safety.Objects[ref]
			proof.CreateAllowed = operation == "create"
			proof.DeleteAllowed = operation == "delete"
			if operation == "create" {
				proof.UID = ""
			}
			safety.Objects[ref] = proof
			p := lifecycleFixtureProgram{action: action, client: client, ref: ref}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			e := PlanExecutor{Client: client, Source: source, AcceptedRevision: func() string { return "accepted" }, Safety: safety, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			loop := NewResponderLoop(IncidentRequest{IncidentID: "synthetic", Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, nil, e, 5)
			loop.ReadView = func(sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
			planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: p.Spec().ID, Bindings: program.Bindings{Objects: program.ObjectBindings{"target": ref}}})
			if planned.Error != "" || planned.PlanID == "" {
				t.Fatalf("plan: %s", planned.Error)
			}
			if operation == "delete" && !planned.Patches[0].Irreversible {
				t.Fatal("plan did not disclose irreversible deletion")
			}
			applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
			if applied.Error != "" || applied.ApplicationID == "" {
				t.Fatalf("apply: %s", applied.Error)
			}
			if applications := loop.PlanApplications(); len(applications) != 1 || len(applications[0].Result.ObjectEffects) != 1 || !applications[0].Result.ObjectEffects[0].Confirmed {
				t.Fatal("missing verified object receipt")
			}
		})
	}
}

type lostCreateReplyClient struct{ *lifecycleTestClient }

func (c lostCreateReplyClient) Create(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	_, err := c.lifecycleTestClient.Create(ctx, ref, object)
	if err != nil {
		return nil, err
	}
	return nil, errors.New("synthetic lost reply")
}

func TestPlanCreateLostReplyAndRecreatedUIDFailClosed(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "missing"}
	accepted := planObject(ref, nil)
	delete(accepted.Object, "spec")
	accepted.Object["data"] = map[string]any{"key": "safe"}
	for _, lostReply := range []bool{true, false} {
		t.Run(fmt.Sprint(lostReply), func(t *testing.T) {
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}}
			safety := fixturePlanSafety(t, ref, accepted)
			proof := safety.Objects[ref]
			proof.UID = ""
			proof.CreateAllowed = true
			safety.Objects[ref] = proof
			var objects PlanObjectClient = client
			if lostReply {
				objects = lostCreateReplyClient{client}
			}
			e := PlanExecutor{Client: objects, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: accepted}}, AcceptedRevision: func() string { return "accepted" }, Safety: safety, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.CreateFromSource{Ref: ref, Reason: "fixture"}}}, false)
			if lostReply {
				if err == nil || result.PendingAction == nil || result.VerificationCompleted || client.deletes != 0 {
					t.Fatal("lost reply inferred ownership or mutated uncertain create")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				client.objects[ref].SetUID("replacement-uid")
				if err := e.Rollback(context.Background(), result); err == nil || client.deletes != 0 {
					t.Fatal("create compensation deleted a replacement UID")
				}
			}
		})
	}
}

type defaultingServiceCreateClient struct {
	*lifecycleTestClient
	changeSelector bool
}

func (c defaultingServiceCreateClient) Create(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	created, err := c.lifecycleTestClient.Create(ctx, ref, object)
	if err != nil {
		return nil, err
	}
	spec := created.Object["spec"].(map[string]any)
	spec["clusterIP"] = "10.96.0.8"
	spec["clusterIPs"] = []any{"10.96.0.8"}
	spec["ipFamilies"] = []any{"IPv4"}
	spec["ipFamilyPolicy"] = "SingleStack"
	spec["type"] = "ClusterIP"
	spec["sessionAffinity"] = "None"
	spec["internalTrafficPolicy"] = "Cluster"
	for _, item := range spec["ports"].([]any) {
		port := item.(map[string]any)
		port["protocol"] = "TCP"
		port["targetPort"] = port["port"]
	}
	if c.changeSelector {
		spec["selector"] = map[string]any{"app": "unapproved"}
	}
	c.objects[ref] = created
	return created.DeepCopy(), nil
}

func TestPlanCreateServiceDefaultsPreserveRoutingProof(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "missing"}
			accepted := planObject(ref, map[string]any{"selector": map[string]any{"app": "api"}, "ports": []any{map[string]any{"port": float64(80)}}})
			raw := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}}
			client := defaultingServiceCreateClient{lifecycleTestClient: raw, changeSelector: changed}
			safety := fixturePlanSafety(t, ref, accepted)
			proof := safety.Objects[ref]
			proof.UID = ""
			proof.CreateAllowed = true
			safety.Objects[ref] = proof
			e := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: accepted}}, AcceptedRevision: func() string { return "accepted" }, Safety: safety, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.CreateFromSource{Ref: ref, Reason: "fixture"}}}, false)
			if changed {
				if err == nil || result.VerificationCompleted {
					t.Fatal("changed selector passed default normalization")
				}
				return
			}
			if err != nil || !result.VerificationPassed {
				t.Fatalf("safe server defaults refused: %v", err)
			}
			if err := e.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func (s lifecycleTestSource) AcceptedScopedManifest(ctx context.Context, commit, namespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if namespace == "" {
		return nil, rendered.ErrEvidenceGap
	}
	return s.AcceptedManifest(ctx, commit, ref)
}
