package traffic

import (
	"strings"
	"testing"
)

// Agent-authored tests call the conformance helper; a nil context must surface
// as an actionable error, never as a panic from inside the clock.
func TestCheckDetectsRejectsNilContext(t *testing.T) {
	err := CheckDetects(nil, laterScenario(), FaultWrongBody) //nolint:staticcheck // the contract under test
	if err == nil || !strings.Contains(err.Error(), "context.Background()") {
		t.Fatalf("CheckDetects(nil, ...) = %v, want an error naming context.Background()", err)
	}
}
