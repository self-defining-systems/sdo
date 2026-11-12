package validate

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func writeAt(kind, objectKind, name string, started, completed time.Time) *contracts.RepairActionReceipt {
	return &contracts.RepairActionReceipt{ActionId: "sdo-api-" + name + kind, Kind: kind, Target: objectKind + "/" + name, Summary: "write", Details: "synthetic", Success: true, StartedAt: timestamppb.New(started), CompletedAt: timestamppb.New(completed),
		Resources: []*contracts.ObjectRef{{ApiVersion: "v1", Kind: objectKind, Namespace: "app", Name: name}}}
}

// A helper Pod the agent creates and deletes belongs to no cause. Treated as unowned it would bound every
// cause's clearance window, so a cause that was repaired minutes earlier would wait on an unrelated probe.
func TestAnAgentCreatedHelperBindsNoCause(t *testing.T) {
	t0 := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	front := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "front"}
	repair := writeAt("api-patch", "Deployment", "front", t0, t0.Add(time.Second))
	repair.Resources[0].ApiVersion = "apps/v1"
	for _, test := range []struct {
		kind  string
		bound bool
	}{
		{"api-create", true},
		{"api-helper-create", false},
		{"api-helper-delete", false},
	} {
		helper := writeAt(test.kind, "Pod", "probe", t0.Add(time.Minute), t0.Add(2*time.Minute))
		outcome := &contracts.OutcomeRecord{RepairActions: []*contracts.RepairActionReceipt{repair, helper}}
		repaired, after, valid := causeRepairs(outcome, []*contracts.ObjectRef{front})
		if !valid || len(repaired) != 1 {
			t.Fatalf("%s: repaired %v valid %v", test.kind, repaired, valid)
		}
		if bound := after.Equal(t0.Add(2 * time.Minute)); bound != test.bound {
			t.Fatalf("%s: clearance window ends %s, bound=%v want %v", test.kind, after, bound, test.bound)
		}
	}
}

// A helper that is itself a cause resource still counts: the cause names it.
func TestAHelperThatIsACauseResourceStillBinds(t *testing.T) {
	t0 := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	pod := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "app", Name: "probe"}
	helper := writeAt("api-helper-delete", "Pod", "probe", t0, t0.Add(time.Minute))
	repaired, after, valid := causeRepairs(&contracts.OutcomeRecord{RepairActions: []*contracts.RepairActionReceipt{helper}}, []*contracts.ObjectRef{pod})
	if !valid || len(repaired) != 1 || !after.Equal(t0.Add(time.Minute)) {
		t.Fatalf("repaired %v after %s valid %v", repaired, after, valid)
	}
}
