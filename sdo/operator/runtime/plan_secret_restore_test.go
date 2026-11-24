package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
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
		Data: map[string][]byte{"key": []byte(foreignSyntheticValue), "other": []byte("keep-live")}}
	web := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web", UID: types.UID("uid-web"), ResourceVersion: "1"}, Spec: appsv1.DeploymentSpec{Template: restartTemplate()}}
	secretRef := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "creds"}
	webRef := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "0123456789abcdef0123456789abcdef01234567",
		Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: webRef}}}
	return fake.NewSimpleClientset(secret, web), secretRef, webRef, incident
}

type learnedSecretRestore struct{ actions []program.Action }

func (learnedSecretRestore) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "restore"} }
func (p learnedSecretRestore) Mitigate(_ context.Context, view program.ReadView, _ program.Bindings) (program.Plan, error) {
	for _, action := range p.actions {
		if restore, ok := action.(program.ReconcileFromSource); ok {
			left := program.SecretKey{Ref: restore.Ref, Key: "key"}
			right := left
			right.Accepted = true
			if _, err := view.SecretEqual(left, right); err != nil {
				return program.Plan{}, err
			}
		}
	}
	return program.Plan{Actions: p.actions}, nil
}

// ReconcileFromSource of a Secret writes the accepted literal data and nothing
// else is read or recorded: the receipt holds key names, never a value, and the
// readers listed after it are restarted.
func TestReconcileFromSourceRestoresASecretThenRestartsItsReaders(t *testing.T) {
	client, secretRef, webRef, incident := secretRestoreFixture()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: secretSource{data: map[string][]byte{"key": []byte(acceptedSyntheticValue), "other": []byte("keep-source")}},
		Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}, Now: func() time.Time { return at }}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app"}), Client: client,
		Source: executor.Source, SourceCommit: incident.SourceCommit, SourceRevision: func() string { return "later-commit" }}
	first := NewCycleReadView(view)
	live := program.SecretKey{Ref: secretRef, Key: "key"}
	accepted := program.SecretKey{Ref: secretRef, Key: "key", Accepted: true}
	if equal, err := first.SecretEqual(live, accepted); err != nil || equal {
		t.Fatalf("SecretEqual before restore = %t, %v", equal, err)
	}
	plan, err := BuildMitigationPlan(context.Background(), learnedSecretRestore{[]program.Action{
		program.ReconcileFromSource{Ref: secretRef, Keys: []string{"key"}},
		program.RestartRollout{Ref: webRef, Reason: "readers hold the foreign value"},
	}}, first, program.Bindings{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("patch", "secrets", func(action kubetesting.Action) (bool, kruntime.Object, error) {
		patch := action.(kubetesting.PatchAction).GetPatch()
		var fields struct {
			Metadata struct {
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
			Data map[string]string `json:"data"`
		}
		if err := json.Unmarshal(patch, &fields); err != nil || fields.Metadata.ResourceVersion != "1" || len(fields.Data) != 1 || fields.Data["key"] == "" {
			t.Error("write lacks the observed version precondition or modifies extra keys")
		}
		return false, nil, nil
	})
	result, err := executor.Apply(context.Background(), incident, plan, false)
	if err != nil || !result.Applied || len(result.ActionsApplied) != 2 {
		t.Fatalf("Apply = %+v, %v", result, err)
	}
	got, err := client.CoreV1().Secrets("app").Get(context.Background(), "creds", metav1.GetOptions{})
	if err != nil || string(got.Data["key"]) != acceptedSyntheticValue {
		t.Fatalf("Secret after restore: %v, %v", got, err)
	}
	if string(got.Data["other"]) != "keep-live" {
		t.Fatal("restore overwrote an unexamined key")
	}
	if equal, err := view.SecretEqual(live, accepted); err != nil || !equal {
		t.Fatalf("SecretEqual after restore = %t, %v", equal, err)
	}
	web, _ := client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	if web.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] == "" {
		t.Fatal("the readers were not restarted")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	wire, err := protojson.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{acceptedSyntheticValue, foreignSyntheticValue} {
		for _, data := range [][]byte{encoded, wire} {
			if strings.Contains(string(data), value) || strings.Contains(string(data), base64.StdEncoding.EncodeToString([]byte(value))) {
				t.Fatal("the application result or wire receipt carries Secret data")
			}
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
	if _, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: secretRef, Keys: []string{"key"}, SecretCommit: incident.SourceCommit, SecretResourceVersion: "1"}}}, true); err != nil {
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
			result, applyErr := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: secretRef, Keys: []string{"key"}, SecretCommit: incident.SourceCommit, SecretResourceVersion: "1"}}}, false)
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

func TestSecretEqualReadsWithoutServiceEndpoints(t *testing.T) {
	client, ref, _, incident := secretRestoreFixture()
	view := LiveReadView{
		Carrier: inputs.New(sdktest.Snapshot{NamespaceName: incident.Namespace}),
		Client:  client, SourceCommit: incident.SourceCommit,
		Source: secretSource{data: map[string][]byte{"key": []byte(acceptedSyntheticValue)}},
	}
	other := ref
	other.Name = "other"
	if _, err := client.CoreV1().Secrets(ref.Namespace).Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: other.Name, Namespace: other.Namespace},
		Data:       map[string][]byte{"key": []byte(foreignSyntheticValue)},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	live := program.SecretKey{Ref: ref, Key: "key"}
	accepted := program.SecretKey{Ref: ref, Key: "key", Accepted: true}
	for _, tc := range []struct {
		right program.SecretKey
		equal bool
	}{{live, true}, {program.SecretKey{Ref: other, Key: "key"}, true}, {accepted, false}} {
		if equal, err := view.SecretEqual(live, tc.right); err != nil || equal != tc.equal {
			t.Fatalf("SecretEqual = %t, %v; want %t", equal, err, tc.equal)
		}
	}
	accepted.Key = "missing"
	if _, err := view.SecretEqual(live, accepted); !errors.Is(err, sdk.ErrUnavailable) {
		t.Fatalf("missing accepted key = %v", err)
	}
}

func TestSecretEqualMissingKeyIsDriftAndDenialIsTyped(t *testing.T) {
	client, ref, _, incident := secretRestoreFixture()
	view := LiveReadView{
		Carrier: inputs.New(sdktest.Snapshot{NamespaceName: incident.Namespace}),
		Client:  client, SourceCommit: incident.SourceCommit,
		Source: secretSource{data: map[string][]byte{"missing": []byte(acceptedSyntheticValue)}},
	}
	left := program.SecretKey{Ref: ref, Key: "missing"}
	right := program.SecretKey{Ref: ref, Key: "missing", Accepted: true}
	if equal, err := view.SecretEqual(left, right); err != nil || equal {
		t.Errorf("a missing live key is a mismatch, got %t, %v", equal, err)
	}
	client.PrependReactor("get", "secrets", func(kubetesting.Action) (bool, kruntime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "creds", errors.New("synthetic-denial-canary"))
	})
	if _, err := view.SecretEqual(left, right); !errors.Is(err, sdk.ErrRefused) || strings.Contains(err.Error(), "synthetic-denial-canary") {
		t.Fatalf("expected value-free refused class, got %v", err)
	}
}

func TestSecretRestoreRefusesVersionChangeAndUncomparedKey(t *testing.T) {
	client, ref, _, incident := secretRestoreFixture()
	source := secretSource{data: map[string][]byte{"key": []byte(acceptedSyntheticValue), "other": []byte("keep-source")}}
	view := NewCycleReadView(LiveReadView{
		Carrier: inputs.New(sdktest.Snapshot{NamespaceName: incident.Namespace}),
		Client:  client, SourceCommit: incident.SourceCommit, Source: source,
	})
	ctx := context.Background()
	left := program.SecretKey{Ref: ref, Key: "key"}
	right := left
	right.Accepted = true
	if _, err := view.SecretEqual(left, right); err != nil {
		t.Fatal(err)
	}
	build := func(keys ...string) (program.Plan, error) {
		return BuildMitigationPlan(ctx, learnedSecretRestore{[]program.Action{
			program.ReconcileFromSource{Ref: ref, Keys: keys},
		}}, view, program.Bindings{}, time.Second)
	}
	if _, err := build("key", "other"); err == nil {
		t.Fatal("built a plan with an uncompared key")
	}
	plan, err := build("key")
	if err != nil {
		t.Fatal(err)
	}
	secrets := client.CoreV1().Secrets("app")
	live, _ := secrets.Get(ctx, ref.Name, metav1.GetOptions{})
	live.ResourceVersion = "2"
	live.Data["key"] = []byte("synthetic-concurrent-rotation")
	if _, err := secrets.Update(ctx, live, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: source, Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}}
	if result, err := executor.Apply(ctx, incident, plan, false); err == nil || result.Applied {
		t.Fatal("stale plan overwrote a concurrent rotation")
	}
}

func TestSecretRestoreConcurrentPatchConflictIsValueFree(t *testing.T) {
	client, ref, _, incident := secretRestoreFixture()
	client.PrependReactor("patch", "secrets", func(kubetesting.Action) (bool, kruntime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, ref.Name, errors.New("synthetic-conflict-canary"))
	})
	executor := PlanExecutor{
		Client: TypedPlanClient{Client: client}, Source: secretSource{data: map[string][]byte{"key": []byte(acceptedSyntheticValue)}},
		Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{},
	}
	plan := program.Plan{Actions: []program.Action{program.ReconcileFromSource{
		Ref: ref, Keys: []string{"key"}, SecretCommit: incident.SourceCommit, SecretResourceVersion: "1",
	}}}
	result, err := executor.Apply(context.Background(), incident, plan, false)
	if err == nil || !strings.Contains(err.Error(), "changed after comparison") || result.Applied || strings.Contains(err.Error(), "synthetic-conflict-canary") {
		t.Fatalf("expected value-free conflict and no reported write, got %v", err)
	}
}

func TestSecretRestoreManualAndLostReply(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			client, ref, _, incident := secretRestoreFixture()
			if lost {
				client.PrependReactor("patch", "secrets", func(kubetesting.Action) (bool, kruntime.Object, error) {
					return true, nil, io.ErrUnexpectedEOF
				})
			}
			executor := PlanExecutor{
				Client: TypedPlanClient{Client: client}, Source: secretSource{data: map[string][]byte{"key": []byte(acceptedSyntheticValue), "other": []byte("manual")}},
				Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{},
			}
			plan := program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}
			result, err := executor.Apply(t.Context(), incident, plan, false)
			if !result.Applied || (lost && (!errors.Is(err, errSecretWriteUnknown) || result.ActionReports[0].Status != "unknown")) || (!lost && err != nil) {
				t.Fatalf("manual restore applied=%t reports=%v err=%v", result.Applied, result.ActionReports, err)
			}
			if !lost {
				live, _ := client.CoreV1().Secrets(ref.Namespace).Get(t.Context(), ref.Name, metav1.GetOptions{})
				if string(live.Data["other"]) != "manual" {
					t.Fatal("manual restore did not write all accepted keys")
				}
			}
		})
	}
}

func TestSecretEqualMissingObjectIsDrift(t *testing.T) {
	client, ref, _, incident := secretRestoreFixture()
	if err := client.CoreV1().Secrets(ref.Namespace).Delete(t.Context(), ref.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: incident.Namespace}), Client: client,
		SourceCommit: incident.SourceCommit, Source: secretSource{data: map[string][]byte{"key": []byte(acceptedSyntheticValue)}}}
	left := program.SecretKey{Ref: ref, Key: "key"}
	right := left
	right.Accepted = true
	if equal, err := view.SecretEqual(left, right); err != nil || equal {
		t.Fatalf("missing Secret: equal=%t, err=%v", equal, err)
	}
}

type countedSecretInventory int

func (n *countedSecretInventory) AcceptedSecretInventory(context.Context, string, string) (func(sdk.ObjectRef) (map[string][]byte, error), error) {
	*n++
	return func(sdk.ObjectRef) (map[string][]byte, error) {
		return map[string][]byte{"a": []byte("synthetic"), "b": []byte("synthetic"), "c": []byte("synthetic"), "d": []byte("synthetic")}, nil
	}, nil
}

func TestSecretEqualReusesAcceptedInventoryAcrossKeys(t *testing.T) {
	client, ref, _, incident := secretRestoreFixture()
	renders := countedSecretInventory(0)
	source := &incidentSecretCache{source: &renders, commit: incident.SourceCommit, namespace: incident.Namespace}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: incident.Namespace}), Client: client,
		SourceCommit: incident.SourceCommit, secretSource: source}
	for _, key := range []string{"a", "b", "c", "d"} {
		left := program.SecretKey{Ref: ref, Key: key}
		right := left
		right.Accepted = true
		if equal, err := view.SecretEqual(left, right); err != nil || equal {
			t.Fatalf("comparison %s: equal=%t err=%v", key, equal, err)
		}
	}
	if renders != 1 {
		t.Fatalf("rendered %d inventories, want one", renders)
	}
}

func TestMitigateByNameRestoresSecretFromSource(t *testing.T) {
	client, ref, _, incident := secretRestoreFixture()
	_, loop := targetLoop(t)
	loop.Request = incident
	loop.Executor = PlanExecutor{Client: TypedPlanClient{Client: client},
		Source:   secretSource{data: map[string][]byte{"key": []byte(acceptedSyntheticValue)}},
		Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}}
	response := loop.Handle(t.Context(), ToolRequest{Command: "mitigate", Targets: []MitigateTarget{{
		Name: "ReconcileFromSource", Parameters: map[string]BindValues{"ref": {"Secret/app/creds"}},
	}}})
	live, err := client.CoreV1().Secrets(ref.Namespace).Get(t.Context(), ref.Name, metav1.GetOptions{})
	if err != nil || response.Error != "" || live == nil || string(live.Data["key"]) != acceptedSyntheticValue {
		t.Fatalf("manual mitigate did not restore the Secret: response=%q reports=%v read=%v", response.Error, response.MenuExecution, err)
	}
}
