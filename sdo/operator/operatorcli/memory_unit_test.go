package operatorcli

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func memoryCheckEnv(t *testing.T) map[string]string {
	t.Helper()
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	return map[string]string{"SDO_ACTOR": "responder", "SDO_PHASE": "reflection", "SDO_BASE_COMMIT": "HEAD", "SDO_OPERATOR_ROOT": operatorRoot}
}

func runMemory(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := RunMemory(context.Background(), args, func(key string) string { return env[key] }, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestMemoryCheckNoFlagsFailsClosedWithoutAcceptanceInputs(t *testing.T) {
	for _, env := range []map[string]string{
		nil,
		{"SDO_ACTOR": "responder", "SDO_PHASE": "reflection"},
		{"SDO_BASE_COMMIT": "HEAD", "SDO_ACTOR": "responder"},
		{"SDO_BASE_COMMIT": "HEAD", "SDO_PHASE": "reflection"},
	} {
		code, _, stderr := runMemory(t, env, "check")
		if code != 2 || stderr == "" {
			t.Fatalf("missing inputs accepted: env=%v code=%d stderr=%s", env, code, stderr)
		}
	}
}
