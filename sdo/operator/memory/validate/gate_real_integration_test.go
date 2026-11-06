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

// A detector's metrics query resolves against the effective sources through
// the real toolchain: a provided source answers the default rule and a named
// query, a source of another kind is refused, and a bare query with no default
// is unbound.
func TestDetectorQueriesResolveAgainstEffectiveSourcesOnRealGo(t *testing.T) {
	provided := func(kinds ...telemetry.Kind) func(*Request) {
		return func(r *Request) {
			r.Telemetry = telemetry.Policy{Provided: []telemetry.Source{{ID: "ops", Kinds: kinds, Adapter: "prometheus", Default: true, Origin: telemetry.OriginOperator}}}
		}
	}
	for _, tc := range []struct {
		name, source string
		request      func(*Request)
		rule         string
	}{
		{"a bare query resolves to the provided default", "", provided(telemetry.KindMetrics), ""},
		{"a named query resolves to the provided source", "ops", provided(telemetry.KindMetrics), ""},
		{"a source that does not serve metrics", "ops", provided(telemetry.KindLogs), RuleTelemetryKind},
		{"a bare query with no source bound", "", nil, RuleTelemetryUnbound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := compileTree(t)
			gateQueriesSource(t, root, tc.source)
			request := compileRequest(root)
			if tc.request != nil {
				tc.request(&request)
			}
			got, err := CompileCandidate(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if tc.rule == "" {
				if len(got) != 0 {
					t.Fatalf("rejected: %v", got)
				}
				return
			}
			if len(got) != 1 || got[0].Rule != tc.rule {
				t.Fatalf("diagnostics = %v, want one %s", got, tc.rule)
			}
		})
	}
}
