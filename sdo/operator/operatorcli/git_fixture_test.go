package operatorcli

import (
	"testing"

	"sdo.dev/operator/internal/testutil"
)

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	return testutil.Git(t, dir, args...)
}
