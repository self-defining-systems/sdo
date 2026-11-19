package memoryhost

import (
	"fmt"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type classedDiffView struct {
	program.ReadView
	err error
}

func (v classedDiffView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{}, v.err
}

// Mitigate builds a plan from what its reads observed, with no later check on
// the inputs, so only a read SDO could not observe (or one that was illegal)
// fails it. A refused read is the target's own answer and a program may act on it.
func TestMitigateViewLatchesOnlyUnavailableAndInvalidReads(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "cm-a"}
	for _, c := range []struct {
		name  string
		err   error
		fails bool
	}{
		{"refused builds the plan", fmt.Errorf("login: %w", sdk.ErrRefused), false},
		{"absent builds the plan", fmt.Errorf("%w", sdk.ErrScopedObjectAbsent), false},
		{"unavailable fails Mitigate", fmt.Errorf("timeout: %w", sdk.ErrUnavailable), true},
		{"unclassified fails Mitigate", fmt.Errorf("boom"), true},
		{"invalid fails Mitigate", fmt.Errorf("secret: %w", sdk.ErrInvalidRead), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			view := &checkedView{ReadView: classedDiffView{ReadView: sdktest.Snapshot{NamespaceName: "app"}.RuntimeReadView(), err: c.err}}
			_, _ = view.DiffAgainstSource(ref)
			if failed := view.failure() != nil; failed != c.fails {
				t.Fatalf("failure latched = %v, want %v", failed, c.fails)
			}
		})
	}
}
