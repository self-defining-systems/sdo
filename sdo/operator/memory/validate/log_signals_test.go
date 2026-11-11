package validate

import (
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/logsignal"
)

const cacheLogSignal = `components: [cache]
signals:
  - {id: refused, re2: 'connection refused'}
`

func logSignalDiagnostics(t *testing.T, signals map[string]string) []Diagnostic {
	t.Helper()
	root := mapTree(t, &plainMapYAML)
	for id, body := range signals {
		write(t, root, layout.LogSignalPath(id), body)
	}
	blocking, _ := staticMapCheck(t, root, HealthJudge)
	var out []Diagnostic
	for _, d := range blocking {
		if strings.HasPrefix(d.Rule, "log-signal.") {
			out = append(out, d)
		}
	}
	return out
}

func TestLogSignalsResolveToMapComponents(t *testing.T) {
	if got := logSignalDiagnostics(t, map[string]string{"cache-refused": cacheLogSignal}); len(got) != 0 {
		t.Fatalf("a valid log signal was rejected: %v", got)
	}
	for name, tc := range map[string]struct{ body, rule, want string }{
		"unknown component": {strings.Replace(cacheLogSignal, "[cache]", "[search]", 1), RuleLogSignalComponent, "component search is not an application-map component"},
		"bad pattern":       {strings.Replace(cacheLogSignal, "connection refused", "(", 1), RuleLogSignalInvalid, "pattern refused"},
		"unknown field":     {cacheLogSignal + "rate: 3\n", RuleLogSignalInvalid, "does not parse"},
	} {
		got := logSignalDiagnostics(t, map[string]string{"cache-refused": tc.body})
		if len(got) == 0 || got[0].Rule != tc.rule || !strings.Contains(got[0].Message, tc.want) {
			t.Errorf("%s: diagnostics = %+v, want %s %q", name, got, tc.rule, tc.want)
		}
	}
}

func TestLogSignalsSharePatternBudget(t *testing.T) {
	signals := map[string]string{}
	var many strings.Builder
	many.WriteString("components: [cache]\nsignals:\n")
	for i := 0; i < logsignal.MaxPatterns; i++ {
		many.WriteString("  - {id: p" + string(rune('a'+i)) + ", re2: 'line " + string(rune('a'+i)) + "'}\n")
	}
	for i := 0; i <= logsignal.MaxTotalPatterns/logsignal.MaxPatterns; i++ {
		signals["s"+string(rune('a'+i))] = many.String()
	}
	got := logSignalDiagnostics(t, signals)
	if len(got) == 0 || !strings.Contains(got[0].Message, "patterns together") {
		t.Fatalf("diagnostics = %+v, want the shared pattern budget", got)
	}
}

// Only the detection designer changes log signals.
func TestLogSignalsBelongToTheDetectionDesigner(t *testing.T) {
	if !allowed(HealthJudge, InitialPhase, layout.KindLogSignal, "cache-refused") {
		t.Fatal("the detection designer cannot write a log signal")
	}
	for _, actor := range []Actor{Responder, Deployer, Upkeep, Upstream} {
		for _, phase := range []Phase{InitialPhase, ReflectionPhase, ProposalPhase, AdvancePhase} {
			if allowed(actor, phase, layout.KindLogSignal, "cache-refused") {
				t.Fatalf("%s may write a log signal in the %s phase", actor, phase)
			}
		}
	}
	if kind, id, ok := layout.Classify(".sdo/diagnostics/logs/cache-refused.yaml"); !ok || kind != layout.KindLogSignal || id != "cache-refused" {
		t.Fatalf("Classify = %s %s %v", kind, id, ok)
	}
	for _, p := range []string{".sdo/diagnostics/logs/Cache.yaml", ".sdo/diagnostics/logs/x.yml", ".sdo/diagnostics/logs/sub/x.yaml"} {
		if kind, _, ok := layout.Classify(p); ok && kind == layout.KindLogSignal {
			t.Fatalf("%s classified as a log signal", p)
		}
	}
}
