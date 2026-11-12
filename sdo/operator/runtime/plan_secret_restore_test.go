package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const (
	acceptedSyntheticValue = "synthetic-accepted-value"
	foreignSyntheticValue  = "synthetic-foreign-value"
)

// secretSource is an accepted source that knows one Secret's literal data.
type secretSource struct {
	data map[string][]byte
	err  error
}

func (secretSource) AcceptedManifest(context.Context, string, sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return nil, errors.New("not used")
}
func (secretSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, errors.New("not used")
}
func (s secretSource) AcceptedSecretData(_ context.Context, revision, namespace string, ref sdk.ObjectRef) (map[string][]byte, error) {
	if revision != "0123456789abcdef0123456789abcdef01234567" || namespace != "app" || ref.Name != "creds" {
		return nil, errors.New("unexpected source read")
	}
	return s.data, s.err
}

func secretRestoreFixture() (*fake.Clientset, sdk.ObjectRef, sdk.ObjectRef, IncidentRequest) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "creds", UID: "uid-secret", ResourceVersion: "1"},
		Data: map[string][]byte{"key": []byte(foreignSyntheticValue)}}
	web := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web", UID: types.UID("uid-web"), ResourceVersion: "1"}, Spec: appsv1.DeploymentSpec{Template: restartTemplate()}}
	secretRef := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "creds"}
	webRef := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "0123456789abcdef0123456789abcdef01234567",
		Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: webRef}}}
	return fake.NewSimpleClientset(secret, web), secretRef, webRef, incident
}

// ReconcileFromSource of a Secret writes the accepted literal data and nothing
// else is read or recorded: the receipt holds key names, never a value, and the
// readers listed after it are restarted.
func TestReconcileFromSourceRestoresASecretThenRestartsItsReaders(t *testing.T) {
	client, secretRef, webRef, incident := secretRestoreFixture()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: secretSource{data: map[string][]byte{"key": []byte(acceptedSyntheticValue)}},
		Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}, Now: func() time.Time { return at }}
	plan := program.Plan{Actions: []program.Action{
		program.ReconcileFromSource{Ref: secretRef},
		program.RestartRollout{Ref: webRef, Reason: "readers hold the foreign value"},
	}}
	result, err := executor.Apply(context.Background(), incident, plan, false)
	if err != nil || !result.Applied || len(result.ActionsApplied) != 2 {
		t.Fatalf("Apply = %+v, %v", result, err)
	}
	got, err := client.CoreV1().Secrets("app").Get(context.Background(), "creds", metav1.GetOptions{})
	if err != nil || string(got.Data["key"]) != acceptedSyntheticValue {
		t.Fatalf("Secret after restore: %v, %v", got, err)
	}
	web, _ := client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	if web.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] == "" {
		t.Fatal("the readers were not restarted")
	}
	encoded, _ := json.Marshal(result)
	for _, value := range []string{acceptedSyntheticValue, foreignSyntheticValue, "c3ludGhldGlj"} {
		if strings.Contains(string(encoded), value) {
			t.Fatalf("the application result carries Secret data %q: %s", value, encoded)
		}
	}
	if report := result.ActionReports[0]; report.Status != "applied" {
		t.Fatalf("restore report = %+v", report)
	}
}

// A dry run reads the accepted source and writes nothing.
func TestSecretRestoreDryRunWritesNothing(t *testing.T) {
	client, secretRef, _, incident := secretRestoreFixture()
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: secretSource{data: map[string][]byte{"key": []byte(acceptedSyntheticValue)}}, Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}}
	if _, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: secretRef}}}, true); err != nil {
		t.Fatal(err)
	}
	got, _ := client.CoreV1().Secrets("app").Get(context.Background(), "creds", metav1.GetOptions{})
	if string(got.Data["key"]) != foreignSyntheticValue {
		t.Fatal("a dry run wrote the Secret")
	}
}

// A source with no literal data (or a generated one) is a typed refusal before
// any write; the Secret is untouched.
func TestSecretRestoreRefusesWithoutAcceptedLiteralData(t *testing.T) {
	for name, err := range map[string]error{"no literal": rendered.ErrSecretSourceNoLiteral, "generated": rendered.ErrSecretSourceNondeterministic} {
		t.Run(name, func(t *testing.T) {
			client, secretRef, _, incident := secretRestoreFixture()
			executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: secretSource{err: err}, Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}}
			result, applyErr := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: secretRef}}}, false)
			if !errors.Is(applyErr, err) || result.Applied {
				t.Fatalf("Apply = %+v, %v", result, applyErr)
			}
			got, _ := client.CoreV1().Secrets("app").Get(context.Background(), "creds", metav1.GetOptions{})
			if string(got.Data["key"]) != foreignSyntheticValue {
				t.Fatal("a refused restore wrote the Secret")
			}
		})
	}
}

// The control namespace is never restored into.
func TestSecretRestoreRefusesOtherNamespaces(t *testing.T) {
	client, _, _, incident := secretRestoreFixture()
	other := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "elsewhere", Name: "creds"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: secretSource{data: map[string][]byte{"key": []byte("x")}}, Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}}
	if result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: other}}}, false); err == nil || result.Applied {
		t.Fatalf("Apply = %+v, %v; want refusal", result, err)
	}
}
