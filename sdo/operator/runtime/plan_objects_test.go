package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

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
	deleteReads      int
	deleted          map[sdk.ObjectRef]bool
}

func (c *lifecycleTestClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.deleted[ref] && c.deleteReads > 0 {
		c.deleteReads--
		if c.deleteReads == 0 {
			delete(c.objects, ref)
		}
	}
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
	if c.deleted == nil {
		c.deleted = map[sdk.ObjectRef]bool{}
	}
	c.deleted[ref] = true
	if !c.ignoreDelete && c.deleteReads == 0 {
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

			e := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: accepted}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
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
			client.deleteReads = 3
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

	e := PlanExecutor{Client: client, Source: lifecycleTestSource{}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
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

func TestPlanDeleteMustObserveAbsenceAndNeverRecreate(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "foreign"}
	before := planObject(ref, map[string]any{"podSelector": map[string]any{}})
	before.SetUID("fixture-uid")
	before.SetResourceVersion("1")
	client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}, ignoreDelete: true}

	e := PlanExecutor{RolloutTimeout: 30 * time.Millisecond, Client: client, Source: lifecycleTestSource{}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "fixture"}}}, false)
	if err == nil || result.VerificationCompleted || result.ObjectEffects[0].Confirmed || client.creates != 0 {
		t.Fatal("missing absence evidence treated as success or recreation")
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

			if operation == "create" {

			}

			p := lifecycleFixtureProgram{action: action, client: client, ref: ref}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			e := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
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

			var objects PlanObjectClient = client
			if lostReply {
				objects = lostCreateReplyClient{client}
			}
			e := PlanExecutor{Client: objects, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: accepted}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
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

			e := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: accepted}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
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
		return nil, rendered.ErrAcceptedManifestAbsent
	}
	return s.AcceptedManifest(ctx, commit, ref)
}

// Run the real lifecycle producer into the real source-diff consumer: existence
// alone cannot prove that a recreated instance has an acceptance identity.
func TestSourceCreateEstablishesCurrentInstanceAcceptance(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Service", "Deployment"} {
		t.Run(kind, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: kind, Namespace: "app", Name: "restored"}
			if kind == "Deployment" {
				ref.APIVersion = "apps/v1"
			}
			source := planObject(ref, map[string]any{"replicas": float64(1)})
			if kind == "ConfigMap" {
				delete(source.Object, "spec")
				source.Object["data"] = map[string]any{"script": "synthetic"}
			}
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}}
			e := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: source}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions", Namespace: "app"}, program.Plan{Actions: []program.Action{program.CreateFromSource{Ref: ref, Reason: "fixture"}}}, false)
			if err != nil || !result.VerificationPassed {
				t.Fatalf("create: %v", err)
			}
			live := client.objects[ref]
			snapshot := &sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*live.DeepCopy()}}
			view := LiveReadView{DetectionContext: snapshot, Source: e.Source, SourceCommit: "accepted"}
			if _, err := view.DiffAgainstSource(ref); err != nil {
				t.Fatalf("created instance cannot be compared: %v", err)
			}
			if !acceptanceRecordBound(live, source) {
				t.Fatal("baseline is not bound to created UID")
			}
			live.SetUID("replacement")
			snapshot.Objects[0] = *live.DeepCopy()
			if _, err := view.DiffAgainstSource(ref); !errors.Is(err, ErrAcceptanceRecordUnavailable) {
				t.Fatalf("replacement inherited acceptance: %v", err)
			}
		})
	}
}

func TestDeleteWaitsForAbsenceBeforeContinuingLifecycleBatch(t *testing.T) {
	for _, kind := range []string{"Job", "Pod", "NetworkPolicy"} {
		t.Run(kind, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: kind, Namespace: "app", Name: "producer"}
			if kind == "Job" {
				ref.APIVersion = "batch/v1"
			}
			if kind == "NetworkPolicy" {
				ref.APIVersion = "networking.k8s.io/v1"
			}
			before := planObject(ref, nil)
			before.SetUID("old")
			before.SetResourceVersion("1")
			next := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "next"}
			source := planObject(next, nil)
			source.Object["data"] = map[string]any{"key": "value"}
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}, deleteReads: 3}
			e := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{next: source}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, err := e.Apply(ctx, IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions", Namespace: "app"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "fixture"}, program.CreateFromSource{Ref: next, Reason: "fixture"}}}, false)
			if err != nil || !result.VerificationPassed || client.creates != 1 || !result.ObjectEffects[0].Confirmed {
				t.Fatalf("asynchronous delete aborted plan: %+v %v", result, err)
			}
		})
	}
}

func (c *lifecycleTestClient) RecordAcceptance(_ context.Context, ref sdk.ObjectRef, record string, live *unstructured.Unstructured) error {
	object := c.objects[ref]
	if object == nil || object.GetUID() != live.GetUID() || object.GetResourceVersion() != live.GetResourceVersion() {
		return fmt.Errorf("acceptance identity conflict")
	}
	object.SetAnnotations(AcceptanceRecordObject(ref, record, live).GetAnnotations())
	object.SetResourceVersion("3")
	return nil
}

type failedAcceptanceClient struct {
	*lifecycleTestClient
	replace, discard bool
}

func (c failedAcceptanceClient) RecordAcceptance(_ context.Context, ref sdk.ObjectRef, _ string, _ *unstructured.Unstructured) error {
	if c.replace {
		c.objects[ref].SetUID("other-writer")
	}
	if c.discard {
		return nil
	}
	return errors.New("synthetic acceptance write failure")
}

func TestSourceCreateCannotSucceedWithoutPublishedInstanceAcceptance(t *testing.T) {
	for _, mode := range []string{"write-failed", "replacement", "write-not-observed"} {
		t.Run(mode, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "restored"}
			source := planObject(ref, nil)
			delete(source.Object, "spec")
			source.Object["data"] = map[string]any{"key": "safe"}
			raw := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}}
			client := failedAcceptanceClient{lifecycleTestClient: raw, replace: mode == "replacement", discard: mode == "write-not-observed"}
			e := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: source}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.CreateFromSource{Ref: ref, Reason: "fixture"}}}, false)
			var recordErr *AcceptanceRecordError
			if !errors.As(err, &recordErr) || result.VerificationCompleted || !result.Applied || len(result.ActionsApplied) != 1 || result.ObjectEffects[0].Confirmed {
				t.Fatalf("unpublished acceptance gained credit or lost create receipt: %+v %v", result, err)
			}
			if mode == "replacement" && raw.deletes != 0 {
				t.Fatal("acceptance failure compensated somebody else's UID")
			}
		})
	}
}

type replacingDeleteClient struct{ *lifecycleTestClient }

func (c replacingDeleteClient) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, version string) error {
	if err := c.lifecycleTestClient.Delete(ctx, ref, uid, version); err != nil {
		return err
	}
	c.objects[ref].SetUID("replacement")
	return nil
}
func TestDeleteObservationPreservesBoundAndRejectsReplacement(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "batch/v1", Kind: "Job", Namespace: "app", Name: "terminating"}
			before := planObject(ref, nil)
			before.SetUID("original")
			before.SetResourceVersion("1")
			before.SetFinalizers([]string{"example.test/hold"})
			raw := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}, ignoreDelete: true}
			var client PlanObjectClient = raw
			if replacement {
				client = replacingDeleteClient{raw}
			}
			e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, RolloutTimeout: 30 * time.Millisecond}
			started := time.Now()
			result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "fixture"}}}, false)
			if err == nil || result.VerificationCompleted || result.ObjectEffects[0].Confirmed || raw.creates != 0 || raw.deletes != 1 {
				t.Fatalf("unconfirmed deletion granted credit: %+v %v", result, err)
			}
			var pending *ObjectDeletionPendingError
			if replacement {
				if errors.As(err, &pending) {
					t.Fatal("replacement mistaken for terminating original")
				}
			} else {
				if !errors.As(err, &pending) || pending.UID != "original" || len(pending.Finalizers) != 1 || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("lost typed terminating evidence: %v", err)
				}
			}
			if time.Since(started) > time.Second {
				t.Fatal("delete exceeded observation budget")
			}
		})
	}
}
