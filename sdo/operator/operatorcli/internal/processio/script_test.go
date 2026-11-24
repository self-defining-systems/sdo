package processio

import (
	"bytes"
	"encoding/json"
	"testing"

	"sdo.dev/operator/sdk/program"
)

func TestScriptDiscardsPrintedSecret(t *testing.T) {
	const secret = "synth-canary"
	var out program.ScriptExecutionResult
	exit := executeScript(t.Context(), `echo "$VALUE"; echo "$VALUE" >&2; exit 23`, []string{"VALUE=" + secret}, &out, nil)
	if exit != 23 {
		t.Fatalf("exit = %d, want 23", exit)
	}
	data, err := json.Marshal(out)
	if err != nil || bytes.Contains(data, []byte(secret)) {
		t.Fatalf("result serialization failed or retained printed value: %v", err)
	}
}
