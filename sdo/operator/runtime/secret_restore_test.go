package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/render"
	"sdo.dev/operator/render/raw"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Synthetic values: no other fixture contains them, so every record, receipt,
// diff and error can be searched for them.
const (
	declaredSecretValue = "synthetic-declared-7c1e"
	rotatedSecretValue  = "synthetic-rotated-2b9d"
	extraSecretValue    = "synthetic-extra-5a03"
)

const (
	secretSourceCommit = "0123456789abcdef0123456789abcdef01234567"
	secretSourceTree   = "89abcdef0123456789abcdef0123456789abcdef"
)

// secretSource is accepted source holding one literal Secret ("creds") and one
// identity-only Secret ("bare"), served by a synthetic object store.
func secretSource(t *testing.T) (rendered.Source, string) {
	t.Helper()
	files := rendertest.MapFS{
		"k8s/creds.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: creds, labels: {app: synthetic}}\ntype: Opaque\nstringData: {access-key: " + declaredSecretValue + ", region: synthetic-region}\n",
		"k8s/bare.yaml":  "apiVersion: v1\nkind: Secret\nmetadata: {name: bare}\n",
	}
	catalog := render.NewCatalog()
	if err := raw.Register(catalog); err != nil {
		t.Fatal(err)
	}
	deployment, err := rendered.LegacyRawSource([]string{"k8s"})
	if err != nil {
		t.Fatal(err)
	}
	return rendered.Source{Repository: "/synthetic", Deployment: deployment, Catalog: catalog, Git: secretTreeGit(files)}, secretSourceCommit
}

// secretTreeGit answers the Git reads the accepted-source reader makes for one
// commit whose tree is files (no .sdo/ directories).
func secretTreeGit(files rendertest.MapFS) rendered.GitFunc {
	return func(_ context.Context, _ string, args ...string) ([]byte, error) {
		switch {
		case len(args) == 4 && args[0] == "rev-parse" && args[3] == secretSourceCommit+"^{tree}":
			return []byte(secretSourceTree + "\n"), nil
		case len(args) == 3 && args[0] == "cat-file" && args[1] == "-t" && args[2] == secretSourceTree:
			return []byte("tree\n"), nil
		case len(args) == 5 && args[0] == "ls-tree" && args[3] == secretSourceTree:
			return nil, nil
		case len(args) >= 1 && args[0] == "ls-tree" && args[len(args)-1] == secretSourceTree:
			names := make([]string, 0, len(files))
			for name := range files {
				names = append(names, name)
			}
			sort.Strings(names)
			var out strings.Builder
			for _, name := range names {
				blob, _ := files.Blob(name)
				fmt.Fprintf(&out, "100644 blob %s %d\t%s\x00", blob, len(files[name]), name)
			}
			return []byte(out.String()), nil
		case len(args) == 3 && args[0] == "cat-file" && args[1] == "blob":
			for name, content := range files {
				if blob, _ := files.Blob(name); blob == args[2] {
					return []byte(content), nil
				}
			}
		}
		return nil, fmt.Errorf("synthetic repository has no object for git %v", args)
	}
}

func secretRestoreRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: name}
}

func assertNoSecretValue(t *testing.T, what, text string) {
	t.Helper()
	for _, value := range []string{declaredSecretValue, rotatedSecretValue, extraSecretValue} {
		if strings.Contains(text, value) {
			t.Fatalf("%s exposes a Secret value: %s", what, text)
		}
	}
}

func secretRestoreExecutor(client *fake.Clientset, source rendered.Source) PlanExecutor {
	return PlanExecutor{Client: GenericPlanClient{TypedPlanClient: TypedPlanClient{Client: client}}, Source: source,
		Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}, Now: func() time.Time { return time.Unix(0, 0) }}
}

// TestRestoreSecretFromSourceWritesDeclaredDataWithValueFreeReceipt: a rotated
// key is restored and an undeclared key removed exactly as accepted source
// declares, and the receipt, its wire form and the persisted plan name keys
// and digests only.
func TestRestoreSecretFromSourceWritesDeclaredDataWithValueFreeReceipt(t *testing.T) {
	source, commit := secretSource(t)
	live := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "creds", ResourceVersion: "1", Annotations: map[string]string{"kept": "yes"}},
		Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"access-key": []byte(rotatedSecretValue), "region": []byte("synthetic-region"), "extra": []byte(extraSecretValue)}}
	client := fake.NewSimpleClientset(live)
	ref := secretRestoreRef("creds")
	incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", SourceCommit: commit, RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
	plan := program.Plan{Actions: []program.Action{program.RestoreSecretFromSource{Ref: ref, Reason: "store rejects the client key"}}}
	result, err := secretRestoreExecutor(client, source).Apply(context.Background(), incident, plan, false)
	if err != nil || !result.Applied {
		t.Fatalf("Apply = %+v, %v", result, err)
	}
	written, err := client.CoreV1().Secrets("app").Get(context.Background(), "creds", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(written.Data, map[string][]byte{"access-key": []byte(declaredSecretValue), "region": []byte("synthetic-region")}) {
		t.Fatalf("restored data keys = %v", sortedKeys(written.Data))
	}
	if written.Annotations["kept"] != "yes" {
		t.Fatal("restore dropped live metadata")
	}
	report := result.ActionReports[0]
	if report.Status != "applied" || report.SecretRestore == nil {
		t.Fatalf("report = %+v", report)
	}
	got := report.SecretRestore
	if !reflect.DeepEqual(got.GetKeys(), []string{"access-key", "region"}) || !reflect.DeepEqual(got.GetRestored(), []string{"access-key"}) ||
		!reflect.DeepEqual(got.GetRemoved(), []string{"extra"}) || got.GetCreated() {
		t.Fatalf("receipt = %v", got)
	}
	if got.GetDigests()["access-key"] != rendered.SecretValueDigest([]byte(declaredSecretValue)) {
		t.Fatalf("digests = %v", got.GetDigests())
	}
	wire, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := protojson.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSecretValue(t, "plan application", string(encoded))
	direct, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSecretValue(t, "plan result", string(direct))
	persisted, err := encodePersistedActions(plan.Actions)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodePersistedActions(persisted)
	if err != nil || !reflect.DeepEqual(decoded, plan.Actions) {
		t.Fatalf("persisted round trip = %+v, %v", decoded, err)
	}
}

// TestRestoreSecretFromSourceCreatesAnAbsentSecret: a deleted Secret is
// recreated with the declared type, labels and data.
func TestRestoreSecretFromSourceCreatesAnAbsentSecret(t *testing.T) {
	source, commit := secretSource(t)
	client := fake.NewSimpleClientset()
	ref := secretRestoreRef("creds")
	incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", SourceCommit: commit, RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
	result, err := secretRestoreExecutor(client, source).Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.RestoreSecretFromSource{Ref: ref, Reason: "Secret deleted"}}}, false)
	if err != nil || !result.Applied || !result.ActionReports[0].SecretRestore.GetCreated() {
		t.Fatalf("Apply = %+v, %v", result, err)
	}
	written, err := client.CoreV1().Secrets("app").Get(context.Background(), "creds", metav1.GetOptions{})
	if err != nil || string(written.Data["access-key"]) != declaredSecretValue || written.Labels["app"] != "synthetic" || written.Type != corev1.SecretTypeOpaque {
		t.Fatalf("created Secret = %v keys, %v", sortedKeys(written.Data), err)
	}
}

// TestRestoreSecretFromSourceRefusesSecretsWithoutLiteralSourceData: a Secret
// source does not declare, or declares without literal data, is refused with
// a typed, actionable reason before any write.
func TestRestoreSecretFromSourceRefusesSecretsWithoutLiteralSourceData(t *testing.T) {
	source, commit := secretSource(t)
	for name, tc := range map[string]struct {
		secret string
		reason rendered.SecretSourceReason
	}{
		"identity only": {secret: "bare", reason: rendered.SecretNotLiteral},
		"undeclared":    {secret: "external", reason: rendered.SecretNotDeclared},
	} {
		t.Run(name, func(t *testing.T) {
			live := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: tc.secret, ResourceVersion: "1"}, Data: map[string][]byte{"k": []byte(rotatedSecretValue)}}
			client := fake.NewSimpleClientset(live)
			ref := secretRestoreRef(tc.secret)
			incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", SourceCommit: commit, RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
			result, err := secretRestoreExecutor(client, source).Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.RestoreSecretFromSource{Ref: ref, Reason: "restore"}}}, false)
			var refusal *rendered.SecretSourceError
			if !errors.As(err, &refusal) || refusal.Reason != tc.reason || result.Applied {
				t.Fatalf("Apply = %+v, %v; want %s refusal", result, err, tc.reason)
			}
			if !strings.Contains(err.Error(), tc.secret) {
				t.Fatalf("refusal does not name the Secret: %v", err)
			}
			assertNoSecretValue(t, "refusal", err.Error())
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" && action.GetVerb() != "watch" {
					t.Fatalf("refused restore wrote: %v", action)
				}
			}
		})
	}
}

// TestSecretDriftNamesDifferingKeysOnly: the accepted-source diff and the
// drift finding compare digests inside Go and name the Secret and its
// differing keys, never a value.
func TestSecretDriftNamesDifferingKeysOnly(t *testing.T) {
	source, commit := secretSource(t)
	live := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "creds", ResourceVersion: "2"},
		Data: map[string][]byte{"access-key": []byte(rotatedSecretValue), "region": []byte("synthetic-region"), "extra": []byte(extraSecretValue)}}
	client := fake.NewSimpleClientset(live)
	ref := secretRestoreRef("creds")

	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app"}), Context: context.Background(), Client: client, Source: source, SourceCommit: commit}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || !diff.Changed {
		t.Fatalf("DiffAgainstSource = %+v, %v", diff, err)
	}
	if !strings.Contains(diff.Text, "access-key") || !strings.Contains(diff.Text, "extra") || strings.Contains(diff.Text, "region") {
		t.Fatalf("diff text = %q", diff.Text)
	}
	assertNoSecretValue(t, "diff", diff.Text)

	config := testControllerConfig()
	config.Namespace, config.SourceCommit = "app", commit
	c, err := NewController(config, nil, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{Source: source, Client: client})
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"namespace": "app", "name": "creds"}}}
	c.processSecretDrift(context.Background(), time.Unix(0, 0), secretDriftView{namespace: "app"}, ObjectChange{Old: object, New: object})
	c.mu.Lock()
	events := append([]sdk.DriftEvent(nil), c.driftEvents...)
	c.mu.Unlock()
	if len(events) != 1 || events[0].Ref != ref || events[0].Classification != sdk.DriftUnexplained {
		t.Fatalf("drift events = %+v", events)
	}
	var paths []string
	for _, field := range events[0].Fields {
		paths = append(paths, field.Path+"="+field.New)
	}
	if !reflect.DeepEqual(paths, []string{"data.access-key=value differs", "data.extra=not declared"}) {
		t.Fatalf("drift fields = %v", paths)
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSecretValue(t, "drift event", string(encoded))

	// A Secret that matches its declaration, that source declares without
	// literal data, or that source does not declare raises nothing.
	restored := live.DeepCopy()
	restored.Data = map[string][]byte{"access-key": []byte(declaredSecretValue), "region": []byte("synthetic-region")}
	if _, err := client.CoreV1().Secrets("app").Update(context.Background(), restored, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	bare := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"namespace": "app", "name": "bare"}}}
	c.mu.Lock()
	c.driftEvents = nil
	c.mu.Unlock()
	c.processSecretDrift(context.Background(), time.Unix(0, 0), secretDriftView{namespace: "app"}, ObjectChange{Old: object, New: object})
	c.processSecretDrift(context.Background(), time.Unix(0, 0), secretDriftView{namespace: "app"}, ObjectChange{Old: bare, New: bare})
	external := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"namespace": "app", "name": "external"}}}
	c.processSecretDrift(context.Background(), time.Unix(0, 0), secretDriftView{namespace: "app"}, ObjectChange{Old: external, New: external})
	if len(c.driftEvents) != 0 {
		t.Fatalf("matching or non-literal Secrets raised drift: %+v", c.driftEvents)
	}
}

type secretDriftView struct {
	program.ReadView
	namespace string
}

func (v secretDriftView) Namespace() string { return v.namespace }

func sortedKeys(data map[string][]byte) []string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	return keys
}
