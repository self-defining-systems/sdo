//go:build integration

package validate

import (
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/telemetry"
)

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
	// A bare query takes the provided default and a named one its source, in one
	// compile (the refusals are gate rows).
	t.Run("queries resolve against the provided sources", func(t *testing.T) {
		t.Parallel()
		root := compileTree(t)
		gateQueriesSource(t, root, "", "ops")
		request := compileRequest(root)
		request.Telemetry = telemetry.Policy{Provided: []telemetry.Source{{ID: "ops", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Adapter: "prometheus", Default: true, Origin: telemetry.OriginOperator}}}
		got, err := CompileCandidate(context.Background(), request)
		if err != nil || len(got) != 0 {
			t.Fatalf("rejected: %v, %v", got, err)
		}
	})
	// An authored pod read compiles, generates its contract test and loads in the
	// controller; the same declaration without a Pod watch is a gate row.
	t.Run("a pod read with a Pod watch", func(t *testing.T) {
		t.Parallel()
		root := compileTree(t)
		gatePodRead(t, root)
		edit(t, root, gateFixtureDetector, `{APIVersion: "v1", Kind: "ConfigMap"},`, `{APIVersion: "v1", Kind: "ConfigMap"}, {APIVersion: "v1", Kind: "Pod"},`)
		edit(t, root, ".sdo/diagnostics/detectors/incidents/missing-configmap/manifest.yaml", "      kind: ConfigMap\n", "      kind: ConfigMap\n    - apiVersion: v1\n      kind: Pod\n")
		if got := gateCompileDiagnostics(t, gateCompileRow{}, root); len(got) != 0 {
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

// The base replay through the real toolchain: Check clones the accepted base
// commit, compiles it with the same runner on the same healthy sample, and
// demotes only a candidate fault the base also has.
func TestPreexistingHealthyBaselineFaultsOnRealGo(t *testing.T) {
	for _, tc := range []struct {
		name      string
		baseFires bool
		blocking  bool
	}{
		{"the base fails the same way", true, false},
		{"the base is quiet", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := compileTree(t)
			if tc.baseFires {
				gateFiresOnHealthyBaseline(t, root)
			}
			commit(t, root, "base")
			if !tc.baseFires {
				gateFiresOnHealthyBaseline(t, root)
			}
			edit(t, root, gateFixtureTest, "package missing_configmap", "package missing_configmap\n\n// candidate edit")
			var notes []Diagnostic
			request := compileRequest(root)
			request.Base, request.HealthyBaseline, request.CoverageGaps = "HEAD", gateBaseline(t), &notes
			candidate, err := CompileCandidate(context.Background(), request)
			if err != nil || len(candidate) != 1 || candidate[0].Rule != RuleDetectorHealthyBaseline {
				t.Fatalf("candidate = %v, %v; want one healthy-baseline fault", candidate, err)
			}
			notes = nil
			blocking, err := separatePreexisting(context.Background(), request, candidate, CompileCandidate)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(blocking) > 0; got != tc.blocking {
				t.Fatalf("blocking = %v, want %v (notes %v)", blocking, tc.blocking, notes)
			}
			if !tc.blocking && (len(notes) != 1 || !strings.Contains(notes[0].Message, "pre-existing (also fails at HEAD)")) {
				t.Fatalf("notes = %v, want one pre-existing note", notes)
			}
		})
	}
}
