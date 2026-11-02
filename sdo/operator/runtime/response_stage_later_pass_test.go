package runtime

import (
	"testing"

	"sdo.dev/operator/sdk"
)

func splitFindingsByPlaybook(t *testing.T, findings []sdk.Finding) (first, second sdk.Finding) {
	t.Helper()
	if len(findings) != 2 {
		t.Fatalf("fixture has %d findings", len(findings))
	}
	return findings[0], findings[1]
}

