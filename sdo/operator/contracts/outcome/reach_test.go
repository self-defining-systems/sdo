package outcome_test

import (
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/outcome"
	"sdo.dev/operator/contracts/wire"
)

func reachFixture() (*contracts.ConfirmedRootCause, []*contracts.Finding) {
	store := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "store"}
	web := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "web"}
	opening := []*contracts.Finding{{
		DetectorId: "traffic-links", RuleId: "link-reachability.web.store.5432", Status: "active", Severity: "critical",
		Summary: "link web -> store:5432 is slow", Evidence: "p50 2s", PrimaryResource: store, RelatedResources: []*contracts.ObjectRef{web},
	}}
	cause := &contracts.ConfirmedRootCause{
		Summary: "egress delay on the path to store", Resources: []*contracts.ObjectRef{store},
		CauseClass: "network-delay", Reach: outcome.ReachOutOfReach,
		Evidence: []*contracts.RootCauseEvidence{{Kind: "detector-finding", Source: "traffic-links", Observation: "p50 2s against 5ms healthy"}},
	}
	return cause, opening
}

func TestReachValuesMatchProto(t *testing.T) {
	declared := wire.StringIn(&contracts.ConfirmedRootCause{}, "reach")
	named := []string{"", outcome.ReachInScope, outcome.ReachOutOfReach}
	slices.Sort(declared)
	slices.Sort(named)
	if !slices.Equal(declared, named) {
		t.Fatalf("reach constants %v differ from messages.proto rule %v", named, declared)
	}
}

func TestOutOfReachCauseBackedByOpeningFindingIsConfirmed(t *testing.T) {
	cause, opening := reachFixture()
	if !outcome.OutOfReachConfirmed(cause, opening, nil) {
		t.Fatal("an out-of-reach cause backed by an opening finding on its resource is not confirmed")
	}
	for _, source := range []string{"link-reachability.web.store.5432", "traffic-links/link-reachability.web.store.5432"} {
		cause.Evidence[0].Source = source
		if !outcome.OutOfReachConfirmed(cause, opening, nil) {
			t.Fatalf("evidence source %q naming the opening finding is not accepted", source)
		}
	}
}

func TestOutOfReachCauseNeedsPreRepairControllerEvidence(t *testing.T) {
	at := timestamppb.New(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
	for name, mutate := range map[string]func(*contracts.ConfirmedRootCause, *[]*contracts.Finding, *[]*contracts.RepairActionReceipt){
		"in scope": func(c *contracts.ConfirmedRootCause, _ *[]*contracts.Finding, _ *[]*contracts.RepairActionReceipt) {
			c.Reach = outcome.ReachInScope
		},
		"reach unset": func(c *contracts.ConfirmedRootCause, _ *[]*contracts.Finding, _ *[]*contracts.RepairActionReceipt) {
			c.Reach = ""
		},
		"live only": func(c *contracts.ConfirmedRootCause, _ *[]*contracts.Finding, _ *[]*contracts.RepairActionReceipt) {
			c.Evidence[0].Kind = "live-observation"
		},
		"other detector": func(c *contracts.ConfirmedRootCause, _ *[]*contracts.Finding, _ *[]*contracts.RepairActionReceipt) {
			c.Evidence[0].Source = "workload-health"
		},
		"finding resolved": func(_ *contracts.ConfirmedRootCause, f *[]*contracts.Finding, _ *[]*contracts.RepairActionReceipt) {
			(*f)[0].Status = "resolved"
		},
		"finding elsewhere": func(c *contracts.ConfirmedRootCause, _ *[]*contracts.Finding, _ *[]*contracts.RepairActionReceipt) {
			c.Resources[0] = &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "cache"}
		},
		"cause repaired": func(c *contracts.ConfirmedRootCause, _ *[]*contracts.Finding, r *[]*contracts.RepairActionReceipt) {
			*r = []*contracts.RepairActionReceipt{{ActionId: "a", Kind: "patch", Target: "store", Summary: "s", Details: "d", StartedAt: at, CompletedAt: at, Success: true, Resources: c.GetResources()}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			cause, opening := reachFixture()
			var repairs []*contracts.RepairActionReceipt
			mutate(cause, &opening, &repairs)
			if outcome.OutOfReachConfirmed(cause, opening, repairs) {
				t.Fatal("confirmed without pre-repair controller evidence for an unrepaired out-of-reach cause")
			}
		})
	}
}
