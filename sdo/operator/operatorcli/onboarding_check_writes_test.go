package operatorcli

import "testing"

// The quiet window must run while writes happen: the flag is on by default so
// a probe that passes only on a static dataset fails in the check.
func TestQuietWriteRoundsDefaultToOn(t *testing.T) {
	if defaultQuietWriteRounds < 1 {
		t.Fatalf("defaultQuietWriteRounds = %d: the quiet window would run with no writes", defaultQuietWriteRounds)
	}
}
