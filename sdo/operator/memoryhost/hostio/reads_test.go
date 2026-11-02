package hostio

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	utilexec "k8s.io/client-go/util/exec"
	v1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// hostPod is the Pod the mirror carries and the fake cluster serves.
var hostPod = sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "toy", Name: "db-0"}

type fakeExec struct {
	commands [][]string
	code     int
	stdout   string
}

func (f *fakeExec) Stream(_ context.Context, _ sdk.ObjectRef, _ string, command []string, _ string, stdout io.Writer) error {
	f.commands = append(f.commands, command)
	if stdout != nil {
		_, _ = io.WriteString(stdout, f.stdout)
	}
	if f.code != 0 {
		return utilexec.CodeExitError{Err: errors.New("exit"), Code: f.code}
	}
	return nil
}

type fakeDoer struct{ paths []string }

func (d *fakeDoer) Do(r *http.Request) (*http.Response, error) {
	d.paths = append(d.paths, r.Method+" "+r.URL.Path)
	body := `{"kind":"List","items":[{"metadata":{"name":"x"}}]}`
	switch r.URL.Path {
	case "/apis/metrics.k8s.io/v1beta1/namespaces/toy/pods/db-0":
		body = `{"kind":"PodMetrics","apiVersion":"metrics.k8s.io/v1beta1","metadata":{"name":"db-0","namespace":"toy"},"timestamp":"2026-01-01T00:00:00Z","window":"30s","containers":[{"name":"mongo","usage":{"cpu":"10m","memory":"64Mi"}}]}`
	case "/apis/metrics.k8s.io/v1beta1/nodes/n1":
		body = `{"kind":"NodeMetrics","apiVersion":"metrics.k8s.io/v1beta1","metadata":{"name":"n1"},"timestamp":"2026-01-01T00:00:00Z","window":"30s","usage":{"cpu":"100m","memory":"1Gi"}}`
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
}

func hostCluster() *fake.Clientset {
	policy := admissionv1.Fail
	return fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "toy", Labels: map[string]string{"env": "test"}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "db-0", Namespace: "toy", UID: "uid-1"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mongo"}}}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ca", Namespace: "toy"}, Data: map[string][]byte{"ca.crt": []byte("PEM")}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ca", Namespace: "sdo-control"}, Data: map[string][]byte{"ca.crt": []byte("PEM")}},
		&admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "guard"}, Webhooks: []admissionv1.ValidatingWebhook{{
			Name: "pods.guard.dev", FailurePolicy: &policy,
			Rules:        []admissionv1.RuleWithOperations{{Operations: []admissionv1.OperationType{admissionv1.Create}, Rule: admissionv1.Rule{APIGroups: []string{""}, Resources: []string{"pods"}}}},
			ClientConfig: admissionv1.WebhookClientConfig{CABundle: []byte("PEM")},
		}}},
	)
}

// readsProgram performs each live read. The host records one typed receipt per
// read, which is what the tests inspect: a failed read makes the verdict unknown
// whatever the program does with the answer.
type readsProgram struct{}

func (readsProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "reads"} }
func (readsProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	get := func(path string) program.KubernetesRequest {
		return program.KubernetesRequest{Method: "GET", Path: path}
	}
	_, _ = v.KubernetesRead(get("/api/v1/namespaces/toy/configmaps"))
	_, _ = v.KubernetesRead(get("/api/v1/namespaces/sdo-control/configmaps"))
	_, _ = v.KubernetesRead(get("/api/v1/namespaces/other/configmaps"))
	_, _ = v.KubernetesRead(get("/api/v1/pods"))
	_, _ = v.DatabaseUsers(program.DatabaseTarget{Engine: program.MongoDB, Pod: hostPod, Container: "mongo", Database: "app"})
	_, _ = v.Observe(program.ExternalOperation{Exec: &program.Command{Pod: hostPod, Container: "mongo", Args: []string{"true"}}})
	_, _ = v.PodUsage(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "toy", Name: "db-0"})
	_, _ = v.NodeUsage(sdk.ObjectRef{APIVersion: "v1", Kind: "Node", Name: "n1"})
	ca := func(namespace string) program.WebhookCA {
		return program.WebhookCA{Ref: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "guard"}, Webhook: "pods.guard.dev", SecretNamespace: namespace, SecretName: "ca", SecretKey: "ca.crt"}
	}
	_, _ = v.WebhookCAMatchesSecret(ca("toy"))
	_, _ = v.WebhookCAMatchesSecret(ca("sdo-control"))
	_, _ = v.AdmissionWebhooksFor(sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: "toy"})
	_, _ = v.AdmissionWebhooksFor(sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: "sdo-control"})
	return program.Verdict{Result: program.NeedsJudgement, Question: "reads"}, nil
}

func hostWith(t *testing.T, backends ReadBackends) *memoryhost.Server {
	t.Helper()
	registry, err := program.NewRegistry([]program.Diagnoser{readsProgram{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := memoryhost.NewServer("c", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	pod, err := structpb.NewStruct(map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "db-0", "namespace": "toy", "uid": "uid-1"},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "mongo"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	s.WithPrograms(registry).WithReadFactory(func(ctx context.Context, snapshot sdktest.Snapshot) (program.ReadView, error) {
		return NewLiveReadView(ctx, snapshot, backends), nil
	})
	err = s.Apply(&v1alpha1.MirrorRequest{Sequence: 1, ResetMirror: true, Namespace: "toy", Complete: true, Upserts: []*v1alpha1.MemoryObject{{
		Ref: &v1alpha1.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "toy", Name: "db-0"}, Object: pod,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// receipt is one recorded read: its operation, arguments, answer and error.
type receipt struct{ op, args, result, err string }

func diagnoseReads(t *testing.T, backends ReadBackends) []receipt {
	t.Helper()
	s := hostWith(t, backends)
	got, err := s.Diagnose(context.Background(), &v1alpha1.DiagnoseRequest{Input: &v1alpha1.MemoryProgramInput{ProgramId: "reads", MirrorSequence: 1}})
	if err != nil || got.Error != "" {
		t.Fatal(got, err)
	}
	var out []receipt
	for _, r := range got.Reads {
		args, _ := r.Arguments.MarshalJSON()
		result, _ := r.Result.MarshalJSON()
		out = append(out, receipt{r.Operation, string(args), string(result), r.Error})
	}
	return out
}

// find returns the receipt for operation whose arguments contain needle.
func find(t *testing.T, receipts []receipt, op, needle string) receipt {
	t.Helper()
	for _, r := range receipts {
		if r.op == op && strings.Contains(r.args, needle) {
			return r
		}
	}
	t.Fatalf("no %s receipt for %q in %+v", op, needle, receipts)
	return receipt{}
}

func fullBackends(c *fake.Clientset, e *fakeExec, d *fakeDoer) ReadBackends {
	return ReadBackends{
		Client: c, Exec: e, ProtectedNamespace: "sdo-control",
		APITransport: func(r program.KubernetesRequest) (kubereads.APIDoer, *url.URL, error) {
			return d, &url.URL{Scheme: "https", Host: "api.invalid", Path: r.Path}, nil
		},
	}
}

// Every live read a saved program may issue is answered by the host, with a
// typed receipt per read.
func TestHostServesLiveReadsToPrograms(t *testing.T) {
	cluster, exec, doer := hostCluster(), &fakeExec{stdout: `[{"name":"app","roles":[{"database":"app","name":"readWrite"}]}]`}, &fakeDoer{}
	receipts := diagnoseReads(t, fullBackends(cluster, exec, doer))
	for _, c := range []struct{ op, needle, result string }{
		{"KubernetesRead", "namespaces/toy/configmaps", `"x"`},
		{"DatabaseUsers", "mongo", `readWrite`},
		{"Observe", "mongo", `0`},
		{"PodUsage", "db-0", `"mongo"`},
		{"NodeUsage", "n1", `"n1"`},
		{"WebhookCAMatchesSecret", `"toy"`, `true`},
		{"AdmissionWebhooksFor", `"toy"`, `pods.guard.dev`},
	} {
		r := find(t, receipts, c.op, c.needle)
		if r.err != "" || !strings.Contains(r.result, c.result) {
			t.Errorf("%s: result %s error %q, want result containing %s", c.op, r.result, r.err, c.result)
		}
	}
}

// The reads stay in the observed namespace, refuse the control namespace, and
// never write: every API call is a get or list, every request a GET. A refused
// read is an error receipt, never an empty answer.
func TestHostLiveReadsAreScopedAndReadOnly(t *testing.T) {
	cluster, exec, doer := hostCluster(), &fakeExec{stdout: `[]`}, &fakeDoer{}
	receipts := diagnoseReads(t, fullBackends(cluster, exec, doer))
	for _, c := range []struct{ op, needle string }{
		{"KubernetesRead", "sdo-control"},
		{"KubernetesRead", "namespaces/other"},
		{"KubernetesRead", `"/api/v1/pods"`},
		{"WebhookCAMatchesSecret", `"sdo-control"`},
		{"AdmissionWebhooksFor", `"sdo-control"`},
	} {
		if r := find(t, receipts, c.op, c.needle); r.err == "" || r.result != "null" && r.result != "false" {
			t.Errorf("%s %s: refused read recorded as result %s error %q", c.op, c.needle, r.result, r.err)
		}
	}
	for _, path := range doer.paths {
		if !strings.HasPrefix(path, "GET ") || strings.Contains(path, "sdo-control") || strings.Contains(path, "/other/") {
			t.Errorf("unexpected API request %q", path)
		}
	}
	for _, action := range cluster.Actions() {
		if verb := action.GetVerb(); verb != "get" && verb != "list" {
			t.Errorf("host wrote to the cluster: %v", action)
		}
		if action.GetNamespace() == "sdo-control" {
			t.Errorf("host reached the control namespace: %v", action)
		}
	}
	if len(exec.commands) == 0 {
		t.Fatal("exec reads never ran")
	}
	for _, command := range exec.commands {
		if joined := strings.Join(command, " "); strings.Contains(joined, "createUser") || strings.Contains(joined, "updateUser") {
			t.Errorf("host ran a catalog write: %v", command)
		}
	}
}

func liveView(backends ReadBackends) *LiveReadView {
	return NewLiveReadView(context.Background(), sdktest.Snapshot{NamespaceName: "toy", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "db-0", Namespace: "toy", UID: "uid-1"}}}}, backends)
}

// The refusals carry named reasons for the controller-side operator to act on.
func TestHostLiveReadRefusalsAreNamed(t *testing.T) {
	view := liveView(fullBackends(hostCluster(), &fakeExec{}, &fakeDoer{}))
	get := func(path string) error {
		_, err := view.KubernetesRead(program.KubernetesRequest{Method: "GET", Path: path})
		return err
	}
	for path, want := range map[string]string{
		"/api/v1/namespaces/sdo-control/configmaps": "control namespace",
		"/api/v1/namespaces/other/configmaps":       "not \"toy\"",
		"/api/v1/pods":                              "must name namespace",
		"/api/v1/namespaces":                        "listing namespaces",
	} {
		if err := get(path); !errors.Is(err, kubereads.ErrOutOfScope) || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want out-of-scope containing %q", path, err, want)
		}
	}
	if _, err := view.KubernetesRead(program.KubernetesRequest{Method: "DELETE", Path: "/api/v1/namespaces/toy/configmaps/x"}); err == nil || !strings.Contains(err.Error(), "GET or HEAD") {
		t.Errorf("write request: %v", err)
	}
	if _, err := view.AdmissionWebhooksFor(sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: "other"}); !errors.Is(err, kubereads.ErrOutOfScope) {
		t.Errorf("other namespace admission: %v", err)
	}
	other := program.ExternalOperation{Exec: &program.Command{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "sdo-control", Name: "p"}, Container: "c", Args: []string{"true"}}}
	if _, err := view.Observe(other); !errors.Is(err, kubereads.ErrOutOfScope) {
		t.Errorf("control namespace observe: %v", err)
	}
}

// A read the host cannot serve says so by name; it never answers empty.
func TestHostReadsWithoutBackendsFailLoudly(t *testing.T) {
	view := liveView(ReadBackends{Client: hostCluster(), ProtectedNamespace: "sdo-control"})
	if _, err := view.KubernetesRead(program.KubernetesRequest{Method: "GET", Path: "/api/v1/namespaces/toy/configmaps"}); !errors.Is(err, kubereads.ErrAPITransportUnavailable) {
		t.Errorf("kubernetes read without transport: %v", err)
	}
	if _, err := view.Observe(program.ExternalOperation{Exec: &program.Command{Pod: hostPod, Container: "mongo", Args: []string{"true"}}}); !errors.Is(err, kubereads.ErrExecUnavailable) {
		t.Errorf("observe without exec: %v", err)
	}
	_, err := view.DatabaseUsers(program.DatabaseTarget{Engine: program.MongoDB, Pod: hostPod, Container: "mongo", Database: "app"})
	var failure *kubereads.DatabaseOperationError
	if !errors.As(err, &failure) || failure.Step != kubereads.DatabaseExecTransport {
		t.Errorf("database users without exec: %v", err)
	}
	// Through the server the same failures are error receipts.
	receipts := diagnoseReads(t, ReadBackends{Client: hostCluster(), ProtectedNamespace: "sdo-control"})
	for _, op := range []string{"KubernetesRead", "DatabaseUsers", "Observe"} {
		if r := find(t, receipts, op, ""); r.err == "" {
			t.Errorf("%s without a backend answered %s", op, r.result)
		}
	}
}

// Observe and DatabaseUsers refuse to answer for a replaced Pod.
func TestHostExecReadsRefuseReplacedPod(t *testing.T) {
	cluster := hostCluster()
	pod, err := cluster.CoreV1().Pods("toy").Get(context.Background(), "db-0", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "uid-2"
	if _, err = cluster.CoreV1().Pods("toy").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	exec := &fakeExec{stdout: `[]`}
	receipts := diagnoseReads(t, fullBackends(cluster, exec, &fakeDoer{}))
	for _, op := range []string{"Observe", "DatabaseUsers"} {
		if r := find(t, receipts, op, "mongo"); r.err == "" {
			t.Errorf("%s answered for a replaced Pod: %s", op, r.result)
		}
	}
	if len(exec.commands) != 0 {
		t.Errorf("a command ran in a replaced Pod: %v", exec.commands)
	}
}

// Even a snapshot of the control namespace itself is refused by every pod read.
func TestHostLiveReadsRefuseControlSnapshot(t *testing.T) {
	view := NewLiveReadView(context.Background(), sdktest.Snapshot{NamespaceName: "sdo-control"}, fullBackends(hostCluster(), &fakeExec{}, &fakeDoer{}))
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "sdo-control", Name: "p"}
	if _, err := view.Observe(program.ExternalOperation{Exec: &program.Command{Pod: pod, Container: "c", Args: []string{"true"}}}); !errors.Is(err, kubereads.ErrOutOfScope) {
		t.Errorf("observe: %v", err)
	}
	if _, err := view.DatabaseUsers(program.DatabaseTarget{Engine: program.MongoDB, Pod: pod, Container: "c", Database: "app"}); !errors.Is(err, kubereads.ErrOutOfScope) {
		t.Errorf("database users: %v", err)
	}
	if _, err := view.AdmissionWebhooksFor(sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: "sdo-control"}); !errors.Is(err, kubereads.ErrOutOfScope) {
		t.Errorf("admission webhooks: %v", err)
	}
}
