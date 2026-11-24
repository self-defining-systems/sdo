package sdktest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type secretOwnDiagnoser struct{}

func (secretOwnDiagnoser) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "secret-drift"} }
func (secretOwnDiagnoser) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	key := program.SecretKey{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "creds"}, Key: "key"}
	source := key
	source.Accepted = true
	_, err := view.SecretEqual(key, source)
	return program.Verdict{Result: program.Match}, err
}

func TestSecretOwnIncidentWithoutComparisonIsUnverified(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "open@detect.json"), []byte(`{"namespace":"app"}`), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := DiagnoseOnIncident(secretOwnDiagnoser{}, dir)
	if err != nil || got.Outcome != OwnIncidentUnverified || !strings.Contains(got.Reason, "SecretEqual") {
		t.Fatalf("own incident = %+v, %v", got, err)
	}
}
