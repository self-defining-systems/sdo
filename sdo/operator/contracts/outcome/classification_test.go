package outcome_test

import (
	"slices"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/outcome"
	"sdo.dev/operator/contracts/wire"
)

func TestClassificationsMatchProto(t *testing.T) {
	declared := wire.StringIn(&contracts.OutcomeRecord{}, "classification")
	named := []string{
		outcome.Success, outcome.ClearedWithoutAction, outcome.Failed,
	}
	slices.Sort(declared)
	slices.Sort(named)
	if !slices.Equal(declared, named) {
		t.Fatalf("outcome constants %v differ from outcome.proto classification rule %v", named, declared)
	}
}
