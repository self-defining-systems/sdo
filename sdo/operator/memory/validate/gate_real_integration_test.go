//go:build integration

package validate

import "testing"

// The compile rows of the gate table through the real `go` toolchain. They
// must reject for the same rule at the same place as FakeGo did in
// TestGateCompileRowsOnFakeGo, which is the check that FakeGo matches the
// compiler for the contracts that use it.
func TestGateCompileRowsOnRealGo(t *testing.T) {
	t.Run("a candidate every stage accepts", func(t *testing.T) {
		t.Parallel()
		if got := gateCompileDiagnostics(t, gateCompileRow{}, compileTree(t)); len(got) != 0 {
			t.Fatalf("rejected: %v", got)
		}
	})
	for _, row := range gateCompileRows {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			root := compileTree(t)
			row.real(t, root)
			assertGateCompile(t, row, gateCompileDiagnostics(t, row, root))
		})
	}
}
