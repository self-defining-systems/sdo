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

// A capture redacts ConfigMap values (a live read sees them) and drops objects over its budget, but a NoMatch is
// only unverified when Diagnose read a kind the capture withholds: otherwise the capture cannot be why it
// never matched, and the playbook fails on its own incident.
func TestDiagnoseOnIncidentDowngradesNoMatchOnlyWhenItReadWhatTheCaptureWithholds(t *testing.T) {
	readNothingWithheld := func(view program.ReadView) (program.Verdict, error) {
		_ = view.Deployments()
		return program.Verdict{Result: program.NoMatch}, nil
	}
	readConfigMaps := func(view program.ReadView) (program.Verdict, error) {
		_ = view.ConfigMaps()
		return program.Verdict{Result: program.NoMatch}, nil
	}
	readConfigMapObject := func(view program.ReadView) (program.Verdict, error) {
		_, _ = view.Object(sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "script"})
		return program.Verdict{Result: program.NoMatch}, nil
	}
	readEvents := func(view program.ReadView) (program.Verdict, error) {
		_ = view.Events()
		return program.Verdict{Result: program.NoMatch}, nil
	}
	redacted := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "script", Namespace: "app", Annotations: map[string]string{"sdo.dev/configmap-values-redacted": "true"}}}
	redactedSnapshot := Snapshot{NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{redacted}}
	trimmedSnapshot := Snapshot{NamespaceName: "app", TrimmedObjects: map[string]int{"Event": 3}}
	for name, tc := range map[string]struct {
		snapshot Snapshot
		diagnose func(program.ReadView) (program.Verdict, error)
		want     OwnIncidentOutcome
	}{
		"redacted ConfigMap, never read":      {redactedSnapshot, readNothingWithheld, OwnIncidentFailed},
		"redacted ConfigMap, list read":       {redactedSnapshot, readConfigMaps, OwnIncidentUnverified},
		"redacted ConfigMap, object read":     {redactedSnapshot, readConfigMapObject, OwnIncidentUnverified},
		"trimmed Events, never read":          {trimmedSnapshot, readNothingWithheld, OwnIncidentFailed},
		"trimmed Events, read":                {trimmedSnapshot, readEvents, OwnIncidentUnverified},
		"trimmed Events, only ConfigMap read": {trimmedSnapshot, readConfigMaps, OwnIncidentFailed},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			raw, err := json.Marshal(tc.snapshot)
			if err != nil {
				t.Fatal(err)
			}
			for file, data := range map[string][]byte{"open@detect.json": raw, "open@detect-recordings.json": []byte("[]")} {
				if err := os.WriteFile(filepath.Join(dir, file), data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := DiagnoseOnIncident(ownDiagnoser{spec: program.ProgramSpec{ID: "p"}, diagnose: tc.diagnose}, dir)
			if err != nil || got.Outcome != tc.want || !strings.Contains(got.Reason, "NoMatch") {
				t.Fatalf("got %+v, %v; want %s naming the NoMatch", got, err, tc.want)
			}
			if tc.want == OwnIncidentFailed && !strings.Contains(got.Reason, "match its own incident") {
				t.Fatalf("a failure must say how to fix it: %q", got.Reason)
			}
		})
	}
}

func TestRecoveredDiagnoseRequiresCompleteReads(t *testing.T) {
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "healthy"}
	config := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app",
		Annotations: map[string]string{"sdo.dev/configmap-values-redacted": "true"}}}
	for _, tc := range []struct {
		name  string
		setup func(*Snapshot)
		read  func(program.ReadView)
		want  bool
	}{
		{"healthy drift", func(*Snapshot) {}, func(v program.ReadView) { v.Pods() }, true},
		{"redacted ConfigMap", func(s *Snapshot) { s.ConfigMapList = []corev1.ConfigMap{config} }, func(v program.ReadView) {
			_, _ = v.Object(sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"})
		}, false},
		{"trimmed Pods", func(s *Snapshot) { s.TrimmedObjects = map[string]int{"Pod": 1} }, func(v program.ReadView) { v.Pods() }, false},
		{"swallowed diff error", func(*Snapshot) {}, func(v program.ReadView) { _, _ = v.DiffAgainstSource(pod) }, false},
		{"invalid Kubernetes read", func(*Snapshot) {}, func(v program.ReadView) { _, _ = v.KubernetesRead(program.KubernetesRequest{}) }, false},
		{"correct NoMatch", func(s *Snapshot) { s.PodList = nil }, func(v program.ReadView) { v.Pods() }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}}}
			tc.setup(&snapshot)
			dir := t.TempDir()
			raw, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "fixed.json"), raw, 0o600); err != nil {
				t.Fatal(err)
			}
			d := ownDiagnoser{spec: program.ProgramSpec{ID: "synthetic"}, diagnose: func(v program.ReadView) (program.Verdict, error) {
				tc.read(v)
				if len(v.Pods()) == 0 {
					return program.Verdict{Result: program.NoMatch}, nil
				}
				absent := pod
				absent.Name = "absent"
				return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: pod}, {Resource: absent}}}, nil
			}}
			got, result := DiagnoseOnRecovered(context.Background(), d, dir)
			wantResult := program.Unknown
			if tc.want {
				wantResult = program.Match
			} else if tc.name == "correct NoMatch" {
				wantResult = program.NoMatch
			}
			if result != wantResult {
				t.Fatalf("result = %s, want %s", result, wantResult)
			}
			if !tc.want && got != nil || tc.want && (got == nil || len(got.Objects) != 1 || got.Objects[0] != pod) {
				t.Fatalf("got %+v, want only captured Pod when match is evaluable: %v", got, tc.want)
			}
		})
	}
}

func TestOpenReplayKeepsBlockingNoMatchAfterRecordedConfigMapDiff(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
	for _, tc := range []struct {
		method string
		diff   func(program.ReadView, sdk.ObjectRef) (program.Diff, error)
	}{
		{"DiffAgainstSource", program.ReadView.DiffAgainstSource},
		{"DiffAgainstBaseline", program.ReadView.DiffAgainstBaseline},
	} {
		t.Run(tc.method, func(t *testing.T) {
			snapshot := Snapshot{NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{{
				ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace,
					Annotations: map[string]string{"sdo.dev/configmap-values-redacted": "true"}},
			}}}
			args, err := json.Marshal([]sdk.ObjectRef{ref})
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			for name, value := range map[string]any{
				"open@detect.json": snapshot,
				"open@detect-recordings.json": []PrimitiveCall{{
					Method: tc.method, Arguments: args, Result: json.RawMessage("{}"),
				}},
			} {
				raw, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name), raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			d := ownDiagnoser{spec: program.ProgramSpec{ID: "synthetic"}, diagnose: func(v program.ReadView) (program.Verdict, error) {
				_, err := tc.diff(v, ref)
				return program.Verdict{Result: program.NoMatch}, err
			}}
			got, err := DiagnoseOnIncident(d, dir)
			if err != nil || got.Outcome != OwnIncidentFailed {
				t.Fatalf("open verdict = %+v, %v, want blocking NoMatch", got, err)
			}
		})
	}
}
