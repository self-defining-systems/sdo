package replay

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// The three hand-written program shapes below mirror ideal Diagnose programs
// for source drift, a database grant and a webhook backend. Each must judge on
// what the controller recorded, never on a replay-side recomputation.

type recordedDriftProgram struct{ kind string }

func (p recordedDriftProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "recorded-drift", Params: []program.ParamSpec{{Name: "object", Kind: program.Kind(p.kind), Required: true}}}
}

func (recordedDriftProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	for range 2 {
		// A second identical read must see the same frozen answer.
		if _, err := v.DiffAgainstSource(b.Objects["object"]); err != nil {
			return program.Verdict{}, err
		}
	}
	diff, err := v.DiffAgainstSource(b.Objects["object"])
	if err != nil {
		return program.Verdict{}, err
	}
	if diff.Changed && !diff.SourceAbsent {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

type recordedGrantProgram struct{ target program.DatabaseTarget }

func (recordedGrantProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "recorded-grant", Params: []program.ParamSpec{{Name: "pod", Kind: "Pod", Required: true}}}
}

func (p recordedGrantProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	target := p.target
	target.Pod = b.Objects["pod"]
	for range 2 {
		users, err := v.DatabaseUsers(target)
		if err != nil {
			return program.Verdict{}, err
		}
		for _, u := range users {
			for _, r := range u.Roles {
				if u.Name == "app" && r == (program.DatabaseRole{Database: "app-db", Name: "readWrite"}) {
					return program.Verdict{Result: program.NoMatch}, nil
				}
			}
		}
	}
	return program.Verdict{Result: program.Match}, nil
}

type recordedWebhookBackendProgram struct{}

func (recordedWebhookBackendProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "recorded-webhook-backend", Params: []program.ParamSpec{{Name: "configuration", Kind: "ValidatingWebhookConfiguration", Required: true}}}
}

func (recordedWebhookBackendProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	admission, observed := v.PodAdmission()
	if !observed || admission.Error != "" {
		return program.Verdict{Result: program.Unknown}, nil
	}
	for range 2 {
		webhooks, err := v.AdmissionWebhooksFor(sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: v.Namespace()})
		if err != nil {
			return program.Verdict{}, err
		}
		for _, w := range webhooks {
			if w.ReadyEndpoints == 0 && !admission.Admitted && strings.Contains(admission.Rejection, "failed calling webhook") {
				return program.Verdict{Result: program.Match}, nil
			}
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func recordedCall(t *testing.T, phase, method string, argument, result any) sdktest.PrimitiveCall {
	t.Helper()
	args, err := json.Marshal([]any{argument})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return sdktest.PrimitiveCall{Phase: phase, Method: method, Arguments: args, Result: raw}
}

// A captured ConfigMap keeps key names only, so recomputing its diff on replay
// always differs from source. The recorded comparison is the observation.
func redactedConfigMap(value string) unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "flags", "namespace": "app", "annotations": map[string]any{"sdo.dev/configmap-values-redacted": "true"}}, "data": map[string]any{"flags.json": value}}}
}

func TestDiagnoseJudgesRecordedSourceDiffs(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	snapshot := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{redactedConfigMap("")}}
	otherLane := redactedConfigMap("other lane's build")
	for name, accepted := range map[string][]*unstructured.Unstructured{"empty inventory": {}, "no inventory": nil, "another lane's inventory": {&otherLane}} {
		t.Run(name, func(t *testing.T) {
			c := Case{ID: "drift", Accepted: accepted, Corpus: sdktest.CorpusCase{Open: snapshot, Fixed: snapshot, Healthy: snapshot, RootCauseProgramIDs: []string{"recorded-drift"}, Bindings: program.Bindings{Objects: program.ObjectBindings{"object": ref}}, Recordings: []sdktest.PrimitiveCall{
				recordedCall(t, "", "DiffAgainstSource", ref, program.Diff{Changed: true, Text: "flag on"}),
				recordedCall(t, "fixed", "DiffAgainstSource", ref, program.Diff{}),
			}}}
			r := Check(context.Background(), recordedDriftProgram{kind: "ConfigMap"}, nil, nil, c)
			if r.Faulted.Result != program.Match || r.Healthy.Result != program.NoMatch {
				t.Fatalf("faulted=%s healthy=%s errors=%v", r.Faulted.Result, r.Healthy.Result, r.Errors)
			}
		})
	}
}

func TestExplicitHealthyRecordingsJudgeHealthyPhase(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	c := Case{ID: "drift", Corpus: sdktest.CorpusCase{Open: snapshot, Fixed: snapshot, Healthy: snapshot, RootCauseProgramIDs: []string{"recorded-drift"}, Bindings: program.Bindings{Objects: program.ObjectBindings{"object": ref}}, Recordings: []sdktest.PrimitiveCall{
		recordedCall(t, "", "DiffAgainstSource", ref, program.Diff{Changed: true}),
		recordedCall(t, "fixed", "DiffAgainstSource", ref, program.Diff{Changed: true}),
		recordedCall(t, "healthy", "DiffAgainstSource", ref, program.Diff{}),
	}}}
	r := Check(context.Background(), recordedDriftProgram{kind: "Deployment"}, nil, nil, c)
	if r.Faulted.Result != program.Match || r.Healthy.Result != program.NoMatch {
		t.Fatalf("faulted=%s healthy=%s errors=%v", r.Faulted.Result, r.Healthy.Result, r.Errors)
	}
	// The healthy baseline snapshot itself carries recorded replies.
	c.Corpus.Recordings = c.Corpus.Recordings[:1]
	c.Corpus.Healthy.Readings = []sdktest.PrimitiveCall{recordedCall(t, "", "DiffAgainstSource", ref, program.Diff{})}
	r = Check(context.Background(), recordedDriftProgram{kind: "Deployment"}, nil, nil, c)
	if r.Faulted.Result != program.Match || r.Healthy.Result != program.NoMatch {
		t.Fatalf("healthy readings: faulted=%s healthy=%s errors=%v", r.Faulted.Result, r.Healthy.Result, r.Errors)
	}
}

func TestUnrecordedObservationIsUnknownAndNamed(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	snapshot := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{redactedConfigMap("")}}
	c := Case{ID: "drift", Accepted: []*unstructured.Unstructured{}, Corpus: sdktest.CorpusCase{Open: snapshot, Fixed: snapshot, Healthy: snapshot, RootCauseProgramIDs: []string{"recorded-drift"}, Bindings: program.Bindings{Objects: program.ObjectBindings{"object": ref}}}}
	r := Check(context.Background(), recordedDriftProgram{kind: "ConfigMap"}, nil, nil, c)
	if r.Faulted.Result != program.Unknown || !strings.Contains(verdictText(r.Faulted), "unrecorded DiffAgainstSource") || !strings.Contains(verdictText(r.Faulted), "flags") {
		t.Fatalf("unrecorded read was not reported: %+v", r.Faulted)
	}

	target := program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}, Container: "db", Database: "app-db"}
	db := Case{ID: "grant", Corpus: sdktest.CorpusCase{Open: snapshot, Fixed: snapshot, Healthy: snapshot, RootCauseProgramIDs: []string{"recorded-grant"}, Bindings: program.Bindings{Objects: program.ObjectBindings{"pod": target.Pod}}}}
	r = Check(context.Background(), recordedGrantProgram{target: target}, nil, nil, db)
	if r.Faulted.Result != program.Unknown || !strings.Contains(verdictText(r.Faulted), "unrecorded DatabaseUsers") || !strings.Contains(verdictText(r.Faulted), "db-0") {
		t.Fatalf("unrecorded catalog read was not reported: %+v", r.Faulted)
	}
}

func TestDiagnoseJudgesRecordedDatabaseCatalog(t *testing.T) {
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}
	credential := func(key string) program.CredentialRef {
		return program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "db-script", Key: "init.sh", Assignment: key}}
	}
	target := program.DatabaseTarget{Engine: program.MongoDB, Pod: pod, Container: "db", Database: "app-db", Auth: &program.DatabaseAuth{Database: "admin", Username: credential("ADMIN_USER"), Password: credential("ADMIN_PWD")}, Shell: "mongo"}
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	granted := []program.DatabaseUser{{Name: "app", Roles: []program.DatabaseRole{{Database: "app-db", Name: "readWrite"}}}}
	c := Case{ID: "grant", Corpus: sdktest.CorpusCase{Open: snapshot, Fixed: snapshot, Healthy: snapshot, RootCauseProgramIDs: []string{"recorded-grant"}, Bindings: program.Bindings{Objects: program.ObjectBindings{"pod": pod}}, Recordings: []sdktest.PrimitiveCall{
		recordedCall(t, "", "DatabaseUsers", target, []program.DatabaseUser{}),
		recordedCall(t, "fixed", "DatabaseUsers", target, granted),
	}}}
	r := Check(context.Background(), recordedGrantProgram{target: target}, nil, nil, c)
	if r.Faulted.Result != program.Match || r.Healthy.Result != program.NoMatch {
		t.Fatalf("faulted=%s healthy=%s errors=%v", r.Faulted.Result, r.Healthy.Result, r.Errors)
	}
}

func TestDiagnoseJudgesRecordedAdmissionWebhooks(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"}
	open := sdktest.Snapshot{NamespaceName: "app", Admission: &sdk.PodAdmissionResult{Rejection: `failed calling webhook "policy.example": service "policy" not found`}}
	healthy := sdktest.Snapshot{NamespaceName: "app", Admission: &sdk.PodAdmissionResult{Admitted: true}}
	namespace := sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: "app"}
	backend := func(ready int) []program.AdmissionWebhook {
		return []program.AdmissionWebhook{{Configuration: ref, Webhook: "policy.example", FailurePolicy: "Fail", Service: &sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "policy", Name: "policy"}, ReadyEndpoints: ready}}
	}
	c := Case{ID: "backend", Corpus: sdktest.CorpusCase{Open: open, Fixed: healthy, Healthy: healthy, RootCauseProgramIDs: []string{"recorded-webhook-backend"}, Bindings: program.Bindings{Objects: program.ObjectBindings{"configuration": ref}}, Recordings: []sdktest.PrimitiveCall{
		recordedCall(t, "", "AdmissionWebhooksFor", namespace, backend(0)),
		recordedCall(t, "fixed", "AdmissionWebhooksFor", namespace, backend(1)),
	}}}
	r := Check(context.Background(), recordedWebhookBackendProgram{}, nil, nil, c)
	if r.Faulted.Result != program.Match || r.Healthy.Result != program.NoMatch {
		t.Fatalf("faulted=%s healthy=%s errors=%v", r.Faulted.Result, r.Healthy.Result, r.Errors)
	}
}

func verdictText(v program.Verdict) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}
