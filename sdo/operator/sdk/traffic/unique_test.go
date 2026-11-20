package traffic

import (
	"regexp"
	"testing"
)

func engineWithNonce(t *testing.T, nonce uint64) *Engine {
	t.Helper()
	engine := &Engine{workload: Workload{Seed: 42}}
	engine.UseNonce(nonce)
	return engine
}

func tokens(t *testing.T, engine *Engine, iterations int) []string {
	t.Helper()
	param := Unique("sdo-probe-")
	out := make([]string, 0, iterations)
	for i := 0; i < iterations; i++ {
		value, err := param.Value(engine.Rand(uint64(i)), State{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, value)
	}
	return out
}

func TestRestartedProberDrawsDisjointValues(t *testing.T) {
	first := map[string]bool{}
	for _, v := range tokens(t, engineWithNonce(t, 1), 500) {
		first[v] = true
	}
	for _, v := range tokens(t, engineWithNonce(t, 2), 500) {
		if first[v] {
			t.Fatalf("restarted prober replayed %s", v)
		}
	}
}

func TestNewEnginesDrawDifferentNonces(t *testing.T) {
	a, b := newRunNonce(RealClock.Now()), newRunNonce(RealClock.Now())
	if a == b {
		t.Fatalf("two runs share nonce %d", a)
	}
}

func TestIterationsAreUniqueAndReplayableWithinARun(t *testing.T) {
	engine := engineWithNonce(t, 7)
	seen := map[string]bool{}
	for _, v := range tokens(t, engine, 1000) {
		if seen[v] {
			t.Fatalf("duplicate %s within one run", v)
		}
		seen[v] = true
	}
	again := tokens(t, engine, 3)
	for _, v := range again {
		if !seen[v] {
			t.Fatalf("iteration did not replay: %s", v)
		}
	}
}

func TestUniqueTokensAreFixedWidth(t *testing.T) {
	shape := regexp.MustCompile(`^sdo-probe-[0-9a-f]{16}$`)
	for _, v := range tokens(t, engineWithNonce(t, 3), 2000) {
		if !shape.MatchString(v) {
			t.Fatalf("token %q is not prefix plus %d hex digits", v, UniqueWidth)
		}
	}
}
