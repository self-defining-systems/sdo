//go:build integration && race

package runtime

import "time"

// The race detector slows the real compiler runs inside hermetic rehearsals;
// the extended nightly shares its runner with other race-instrumented tests.
const rehearsalBudget = 6 * time.Minute
