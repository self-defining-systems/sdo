package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func databaseCall(t *testing.T, phase, user string, class, text string) sdktest.PrimitiveCall {
	t.Helper()
	target := program.DatabaseTarget{
		Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store-a-0"}, Container: "store", Database: "app",
		Auth: &program.DatabaseAuth{Database: "admin", Username: program.CredentialRef{Env: user + "_USER"}, Password: program.CredentialRef{Env: user + "_PASS"}},
	}
	args, err := json.Marshal([]any{target})
	if err != nil {
		t.Fatal(err)
	}
	return sdktest.PrimitiveCall{Phase: phase, Method: "DatabaseUsers", Arguments: args, Result: json.RawMessage(`null`), Error: text, ErrorClass: class}
}

func recordedCalls(t *testing.T, root, incident string) []sdktest.PrimitiveCall {
	t.Helper()
	dir, err := IncidentCorpusDir(root, incident)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(data, &calls); err != nil {
		t.Fatal(err)
	}
	return calls
}

// A program that tries one credential and falls back to another makes two
// observations of one catalog: which credential was used decides the answer,
// so a capture keeps both and a replay of the fallback agrees with the live run.
func TestCapturedCatalogReadsAreKeyedByCredentialReference(t *testing.T) {
	root := t.TempDir()
	if _, err := PrepareIncidentSession(root, "", "incident-1"); err != nil {
		t.Fatal(err)
	}
	dir, err := IncidentCorpusDir(root, "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "open@detect-recordings.json"), []byte("[]"), 0o600); err != nil {
		t.Fatal(err)
	}
	refused := databaseCall(t, "", "ADMIN", sdk.ReadClassRefused, "database catalog observation unavailable")
	fallback := databaseCall(t, "", "ROOT", "", "")
	repeat := databaseCall(t, "", "ADMIN", sdk.ReadClassRefused, "database catalog observation unavailable")
	if err := AppendCapturedRecordings(root, "incident-1", "open@detect", []sdktest.PrimitiveCall{refused, fallback, repeat}); err != nil {
		t.Fatal(err)
	}
	calls := recordedCalls(t, root, "incident-1")
	if len(calls) != 2 {
		t.Fatalf("captured %d catalog reads, want the admin refusal and the root fallback once each: %+v", len(calls), calls)
	}
	if calls[0].ErrorClass != sdk.ReadClassRefused || calls[1].Error != "" {
		t.Fatalf("captured reads lost their outcome: %+v", calls)
	}
	var target program.DatabaseTarget
	var args []program.DatabaseTarget
	if err := json.Unmarshal(calls[1].Arguments, &args); err != nil || len(args) != 1 {
		t.Fatalf("fallback arguments = %s (%v)", calls[1].Arguments, err)
	}
	target = args[0]
	if target.Auth == nil || target.Auth.Username.Env != "ROOT_USER" {
		t.Fatalf("second capture is not the root credential: %+v", target.Auth)
	}
}
