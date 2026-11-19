package sdktest

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type ownDiagnoser struct {
	spec     program.ProgramSpec
	diagnose func(program.ReadView) (program.Verdict, error)
}

func (d ownDiagnoser) Spec() program.ProgramSpec { return d.spec }
func (d ownDiagnoser) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	return d.diagnose(view)
}

func ownCapture(t *testing.T, recordings string) string {
	t.Helper()
	dir := t.TempDir()
	raw, err := json.Marshal(Snapshot{NamespaceName: "app"})
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"open@detect.json": raw, "open@detect-recordings.json": []byte(recordings)} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func match(program.ReadView) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}

// A new playbook's Diagnose runs on the incident it was written for, and the
// verdict has exactly three outcomes: it matches, it provably does not, or the
// capture cannot say. Only the middle one may block publication.
func TestDiagnoseOnIncidentHasThreeOutcomes(t *testing.T) {
	logRead := func(view program.ReadView) (program.Verdict, error) {
		if _, err := view.Logs(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "p"}, "c", time.Minute); err != nil {
			return program.Verdict{}, err
		}
		return program.Verdict{Result: program.Match}, nil
	}
	noMatch := func(program.ReadView) (program.Verdict, error) {
		return program.Verdict{Result: program.NoMatch, Evidence: []program.Evidence{{Fact: "configmap present", Observed: "present", Expected: "absent"}}}, nil
	}
	for name, tc := range map[string]struct {
		diagnoser ownDiagnoser
		want      OwnIncidentOutcome
		reason    string
	}{
		"matches":                               {ownDiagnoser{spec: program.ProgramSpec{ID: "p"}, diagnose: match}, OwnIncidentMatch, ""},
		"no match is a definite failure":        {ownDiagnoser{spec: program.ProgramSpec{ID: "p"}, diagnose: noMatch}, OwnIncidentFailed, "configmap present"},
		"an unrecorded read cannot be verified": {ownDiagnoser{spec: program.ProgramSpec{ID: "p"}, diagnose: logRead}, OwnIncidentUnverified, "unrecorded"},
		"object roles cannot be bound":          {ownDiagnoser{spec: program.ProgramSpec{ID: "p", Params: []program.ParamSpec{{Name: "component", Kind: "Deployment", Required: true}}}, diagnose: match}, OwnIncidentUnverified, "component"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := DiagnoseOnIncident(tc.diagnoser, ownCapture(t, "[]"))
			if err != nil {
				t.Fatal(err)
			}
			if got.Outcome != tc.want || got.ProgramID != "p" || !strings.Contains(got.Reason, tc.reason) {
				t.Fatalf("got %+v, want outcome %s with reason containing %q", got, tc.want, tc.reason)
			}
			if got.Capture != "open@detect.json" {
				t.Fatalf("capture = %q", got.Capture)
			}
		})
	}
}

// A read error the capture holds replays as the Unknown live gave. It says
// nothing about the program, so it is unverified with the error text, never a
// definite failure.
func TestDiagnoseOnIncidentKeepsAReadErrorAsUnverified(t *testing.T) {
	failing := ownDiagnoser{spec: program.ProgramSpec{ID: "p"}, diagnose: func(program.ReadView) (program.Verdict, error) {
		return program.Verdict{}, errors.New("object not captured: v1/ConfigMap app/settings")
	}}
	got, err := DiagnoseOnIncident(failing, ownCapture(t, "[]"))
	if err != nil || got.Outcome != OwnIncidentUnverified || !strings.Contains(got.Reason, "object not captured: v1/ConfigMap app/settings") {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestDiagnoseOnIncidentWithoutACaptureIsUnverified(t *testing.T) {
	got, err := DiagnoseOnIncident(ownDiagnoser{spec: program.ProgramSpec{ID: "p"}, diagnose: match}, t.TempDir())
	if err != nil || got.Outcome != OwnIncidentUnverified || !strings.Contains(got.Reason, "no captured open state") {
		t.Fatalf("got %+v, %v", got, err)
	}
}

// A capture redacts ConfigMap values (a live read sees them) and drops objects over its budget, so a NoMatch on
// such a capture may be the capture and not the playbook: it is unverified, never a definite failure.
func TestDiagnoseOnIncidentDoesNotCallNoMatchDefiniteOnARedactedCapture(t *testing.T) {
	noMatch := ownDiagnoser{spec: program.ProgramSpec{ID: "p"}, diagnose: func(program.ReadView) (program.Verdict, error) {
		return program.Verdict{Result: program.NoMatch}, nil
	}}
	redacted := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "script", Namespace: "app", Annotations: map[string]string{"sdo.dev/configmap-values-redacted": "true"}}}
	for name, snapshot := range map[string]Snapshot{
		"redacted ConfigMap values": {NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{redacted}},
		"trimmed objects":           {NamespaceName: "app", TrimmedObjects: map[string]int{"Pod": 3}},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			raw, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			for file, data := range map[string][]byte{"open@detect.json": raw, "open@detect-recordings.json": []byte("[]")} {
				if err := os.WriteFile(filepath.Join(dir, file), data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := DiagnoseOnIncident(noMatch, dir)
			if err != nil || got.Outcome != OwnIncidentUnverified || !strings.Contains(got.Reason, "NoMatch") {
				t.Fatalf("got %+v, %v; want unverified naming the NoMatch", got, err)
			}
		})
	}
}
