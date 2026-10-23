package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// simTypedKube extends the existing deterministic API-server simulation with
// named Pod effects, a refused exec subresource, and explicit failed/stale writes.
type simTypedKube struct {
	*simKube
	mode       string
	once       bool
	deletedUID types.UID
	execCalls  int
}

func (k *simTypedKube) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if k.objects[ref] == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, ref.Name)
	}
	return k.simKube.Get(ctx, ref)
}
func (k *simTypedKube) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, raw []byte) error {
	if !k.once {
		k.once = true
		switch k.mode {
		case "write-error":
			return apierrors.NewInternalError(errors.New("seeded write failure"))
		case "stale-write":
			return nil // an acknowledgement without the requested effect
		}
	}
	return k.simKube.Patch(ctx, ref, kind, raw)
}
func (k *simTypedKube) Delete(_ context.Context, ref sdk.ObjectRef, uid types.UID, rv string) error {
	object := k.objects[ref]
	if object == nil {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, ref.Name)
	}
	if !k.once {
		k.once = true
		switch k.mode {
		case "conflict":
			k.bump(object)
		case "pod-recreated":
			object.SetUID("replacement-pod")
			k.bump(object)
		case "write-error":
			return apierrors.NewInternalError(errors.New("seeded delete failure"))
		case "stale-write":
			return nil
		}
	}
	if object.GetUID() != uid || object.GetResourceVersion() != rv {
		return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, ref.Name, errors.New("identity changed"))
	}
	k.deletedUID = uid
	delete(k.objects, ref)
	return nil
}
func (k *simTypedKube) Exec(_ context.Context, ref sdk.ObjectRef, _ string, _ []string) (int, error) {
	k.execCalls++
	return 0, apierrors.NewForbidden(schema.GroupResource{Resource: "pods/exec"}, ref.Name, errors.New("no controller grant"))
}

// A single existing seed sweep caps all new dimensions together at 45 seconds;
// the test process additionally runs this group with -timeout=2m.
func TestSimXV2ActionsAndValuesUnderServerFaults(t *testing.T) {
	sweepSimSeedsN(t, 24, func(t *testing.T, seed uint64) {
		rng := newSimRNG(seed)
		for _, origin := range []program.ValueOrigin{program.SourceValue, program.ObservedValue} {
			for _, mode := range []string{"normal", "conflict", "write-error", "stale-write", "partial-undo", "restart"} {
				t.Run(string(origin)+"/"+mode, func(t *testing.T) {
					initial := deploymentWithDrift("safe:v1")
					_ = unstructured.SetNestedField(initial.Object, false, "spec", "template", "spec", "automountServiceAccountToken")
					faults := drawSimFaults(newSimRNG(seed))
					if mode != "normal" && mode != "conflict" {
						faults = simFaults{churnPerPatch: func() int { return 0 }, failAfterCommit: func() bool { return false }, staleReads: func() int { return 0 }}
					}
					kube := &simTypedKube{simKube: newSimKube(faults, initial), mode: mode}
					var ref sdk.ObjectRef
					for ref = range kube.objects {
					}
					before := kube.objects[ref].DeepCopy()
					desired := int64(2 + rng.IntN(7))
					accepted := before.DeepCopy()
					_ = unstructured.SetNestedField(accepted.Object, desired, "spec", "replicas")
					observed := accepted.DeepCopy()
					observed.SetResourceVersion(fmt.Sprint(seed + 100))
					value := program.FromSource(ref, "spec.replicas")
					if origin == program.ObservedValue {
						value = program.FromObserved(ref, "spec.replicas")
					}
					e := PlanExecutor{Client: kube, Source: planFakeSource{source: accepted}, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Now: newSimClock().Now, Values: program.ValueInputs{Observed: func(sdk.ObjectRef) (map[string]any, error) { return observed.Object, nil }}}
					plan := program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: ref, Replicas: value}}}
					incident := IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}
					if mode == "partial-undo" {
						podRef := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "worker"}
						pod := planObject(podRef, map[string]any{})
						pod.SetUID("pod-uid")
						pod.SetResourceVersion("1")
						kube.objects[podRef] = pod
						e.Values.Spec = program.ProgramSpec{Params: []program.ParamSpec{{Name: "command", ValueKind: program.StringValue, Default: "/bin/true"}}}
						plan.Actions = append(plan.Actions, program.Exec{Pod: podRef, Container: "app", Command: []program.Value{program.FromParam("command")}})
					}
					if mode == "restart" {
						preview, err := e.Apply(context.Background(), incident, plan, true)
						if err != nil {
							t.Fatal(err)
						}
						if err = kube.Patch(context.Background(), ref, types.MergePatchType, preview.Patches[0].Patch); err != nil {
							t.Fatal(err)
						}
						pending := 0
						preview.PendingAction = &pending
						receipt, err := preview.Proto()
						if err != nil {
							t.Fatal(err)
						}
						restored := planApplicationState(receipt).Result
						fresh := e
						fresh.Values = program.ValueInputs{}
						reconciled, err := fresh.Reconcile(context.Background(), restored)
						if err != nil || reconciled.VerificationPassed || len(restored.Patches) > 0 || len(restored.PreImages) > 0 {
							t.Fatalf("restart manufactured mutation authority or success: %+v %v", reconciled, err)
						}
						inProcess, err := e.Reconcile(context.Background(), preview)
						if err != nil {
							t.Fatal(err)
						}
						if err = e.Rollback(context.Background(), inProcess); err != nil {
							t.Fatal(err)
						}
						got, _ := program.FieldValue(kube.objects[ref].Object, "spec.replicas")
						want, _ := program.FieldValue(before.Object, "spec.replicas")
						if !planValuesEqual(got, want) {
							t.Fatal("in-process undo lost captured pre-image")
						}
						return
					}
					r, err := e.Apply(context.Background(), incident, plan, false)
					live, _ := program.FieldValue(kube.objects[ref].Object, "spec.replicas")
					original, _ := program.FieldValue(before.Object, "spec.replicas")
					if mode == "partial-undo" {
						if err == nil || !r.RolledBack || !planValuesEqual(live, original) || r.VerificationPassed {
							t.Fatalf("partial failure did not undo pre-image: live=%v before=%v rolledBack=%v err=%v", live, original, r.RolledBack, err)
						}
						return
					}
					if r.VerificationPassed && (err != nil || !planValuesEqual(live, desired)) {
						t.Fatalf("stale or failed write verified: live=%v desired=%v err=%v", live, desired, err)
					}
					if mode == "stale-write" && (err == nil || r.VerificationPassed) {
						t.Fatal("unwritten scale reported success")
					}
					if err == nil && (!r.VerificationPassed || !planValuesEqual(live, desired)) {
						t.Fatalf("successful scale not resolved from %s: live=%v desired=%v", origin, live, desired)
					}
					if r.RolledBack && !planValuesEqual(live, original) {
						t.Fatal("undo did not restore captured replica value")
					}
					if len(r.ActionReports) > 0 && len(r.ActionReports[0].Values) > 0 {
						p := r.ActionReports[0].Values[0]
						if origin == program.SourceValue && p.SourceCommit != "accepted" || origin == program.ObservedValue && p.ResourceVersion != observed.GetResourceVersion() {
							t.Fatal("value provenance lost")
						}
					}
				})
			}
		}
		for _, mode := range []string{"normal", "conflict", "write-error", "stale-write", "pod-recreated"} {
			t.Run("DeletePod/"+mode, func(t *testing.T) {
				ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "worker"}
				pod := planObject(ref, map[string]any{})
				kube := &simTypedKube{simKube: newSimKube(drawSimFaults(newSimRNG(seed)), pod), mode: mode}
				old := kube.objects[ref].GetUID()
				e := PlanExecutor{Client: kube, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Now: newSimClock().Now}
				r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeletePod{Ref: ref}}}, false)
				if r.VerificationPassed && (err != nil || kube.deletedUID != old) {
					t.Fatalf("failed delete credited: uid=%s deleted=%s err=%v", old, kube.deletedUID, err)
				}
				if mode == "pod-recreated" && (kube.objects[ref] == nil || kube.objects[ref].GetUID() != "replacement-pod" || r.VerificationPassed) {
					t.Fatal("replacement Pod was deleted or failed write verified")
				}
			})
		}
	})
}
