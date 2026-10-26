package shellpath

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk/sdktest"
)

type repositoryFixture struct{ snapshot Snapshot }

func (r *repositoryFixture) Current(context.Context) (Snapshot, error) { return r.snapshot, nil }

type executorFunc func(context.Context, string, Script) Result

func (f executorFunc) Execute(c context.Context, r string, s Script) Result { return f(c, r, s) }

type readerFunc func(context.Context) (Report, error)

func (f readerFunc) Read(c context.Context) (Report, error) { return f(c) }

func TestShellServiceActivatesCommitAndResetsStreak(t *testing.T) {
	at := time.Unix(100, 0)
	code := 1
	repository := &repositoryFixture{Snapshot{Commit: "first", Root: "root", Scripts: []Script{{Path: "health/a/check.sh"}}}}
	reports := &ReportStore{}
	service := &Service{Repository: repository, Reports: reports, Executor: executorFunc(func(_ context.Context, _ string, s Script) Result {
		return Result{Script: s.Path, ExitCode: code, At: at}
	})}
	for _, offset := range []time.Duration{0, 30 * time.Second} {
		at = time.Unix(100, 0).Add(offset)
		if err := service.Step(context.Background(), at, false); err != nil {
			t.Fatal(err)
		}
	}
	if !reports.Get().Results[0].Fired {
		t.Fatal("second failure was not reported")
	}
	repository.snapshot.Commit = "second"
	at = at.Add(time.Second)
	if err := service.Step(context.Background(), at, false); err != nil {
		t.Fatal(err)
	}
	report := reports.Get()
	if report.AcceptedCommit != "second" || !report.ActivatedAt.Equal(at) || report.Results[0].Fired {
		t.Fatalf("activation failed to reset state: %#v", report)
	}
	code = 0
	at = at.Add(30 * time.Second)
	if err := service.Step(context.Background(), at, false); err != nil {
		t.Fatal(err)
	}
	at = at.Add(15 * time.Second)
	if err := service.Step(context.Background(), at, false); err != nil {
		t.Fatal(err)
	}
	if !reports.Get().Healthy {
		t.Fatal("stable clear scripts never became healthy")
	}
}
func TestShellReportReadOnlyAndStale(t *testing.T) {
	at := time.Unix(100, 0)
	store := &ReportStore{}
	store.Set(Report{Version: 1, AcceptedCommit: strings.Repeat("a", 40), At: at, ActivatedAt: at, Results: []Result{}})
	server := httptest.NewServer(store)
	defer server.Close()
	reader := HTTPReader{URL: server.URL + "/results", Now: func() time.Time { return at }}
	if _, err := reader.Read(context.Background()); err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(server.URL+"/results", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatal("report endpoint accepted mutation")
	}
	at = at.Add(3 * time.Minute)
	if _, err := reader.Read(context.Background()); err == nil {
		t.Fatal("stale report accepted")
	}
}
func TestShellDetectorEmitsPerScriptFindingsAndKeepsErrorsUnknown(t *testing.T) {
	at := time.Unix(100, 0)
	report := Report{Version: 1, AcceptedCommit: "accepted", At: at, ActivatedAt: at, Results: []Result{{Script: "a.sh", ExitCode: 1, Fired: true, Stdout: "fault A", At: at}, {Script: "b.sh", ExitCode: 1, Fired: true, Stderr: "fault B", At: at}, {Script: "bad.sh", ExitCode: -1, TimedOut: true, Error: "timeout", At: at}}}
	detector := &Detector{Namespace: "app", Commit: "accepted", Reader: readerFunc(func(context.Context) (Report, error) { return report, nil })}
	findings, err := detector.Detect(context.Background(), sdktest.Snapshot{})
	if err != nil || len(findings) != 2 || findings[0].RuleID == findings[1].RuleID || !strings.Contains(findings[1].Evidence, "fault B") {
		t.Fatalf("findings %v %v", findings, err)
	}
	if detector.Confirmed(nil) || !strings.Contains(detector.ConfirmationReason(nil), "bad.sh: detector error") {
		t.Fatal("error was lost")
	}
	report.Results = report.Results[2:]
	findings, err = detector.Detect(context.Background(), sdktest.Snapshot{})
	if err == nil || len(findings) != 0 {
		t.Fatal("timeout fired or cleared")
	}
	report.Results = []Result{{Script: "blip.sh", ExitCode: 1}}
	findings, err = detector.Detect(context.Background(), sdktest.Snapshot{})
	if err != nil || len(findings) != 0 || detector.Confirmed(nil) {
		t.Fatal("single blip fired or established health")
	}
	report.Results = []Result{{Script: "clear.sh", ExitCode: 0}}
	if _, err = detector.Detect(context.Background(), nil); err != nil || !detector.Confirmed(nil) {
		t.Fatal("clear result was not independently confirmable")
	}
}
func TestShellRunnerResourcesAreReadOnlyAndNeverGrantSecretReads(t *testing.T) {
	config := RunnerConfig{Namespace: "app", ControlNamespace: "control", Image: "tools:test", RepositoryPVC: "repo", RepositoryMountPath: "/workspace", AcceptedRoot: "/workspace/application"}
	resources, err := RunnerResources(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range resources {
		switch r := object.(type) {
		case *rbacv1.Role:
			for _, rule := range r.Rules {
				for _, resource := range rule.Resources {
					if resource == "*" || strings.Contains(resource, "secrets") {
						t.Fatal("Secret or wildcard grant")
					}
				}
				for _, verb := range rule.Verbs {
					if verb != "get" && verb != "list" && verb != "watch" {
						t.Fatalf("write grant %s", verb)
					}
				}
			}
		case *corev1.Pod:
			if !*r.Spec.Containers[0].SecurityContext.ReadOnlyRootFilesystem || !r.Spec.Containers[0].VolumeMounts[0].ReadOnly {
				t.Fatal("runner filesystem is writable")
			}
		}
	}
	client := fake.NewSimpleClientset()
	if err := EnsureReadOnlyRunner(context.Background(), client, config); err != nil {
		t.Fatal(err)
	}
	if err := EnsureReadOnlyRunner(context.Background(), client, config); err != nil {
		t.Fatal(err)
	}
	config.Image = "tools:updated"
	if err := EnsureReadOnlyRunner(context.Background(), client, config); err != nil {
		t.Fatal(err)
	}
	updated, err := client.CoreV1().Pods(config.ControlNamespace).Get(context.Background(), RunnerName, metav1.GetOptions{})
	if err != nil || updated.Spec.Containers[0].Image != config.Image {
		t.Fatalf("runner update not activated: %v %v", updated, err)
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "secrets" {
			t.Fatal("installer read a Secret")
		}
	}
}
func TestShellGitRepositoryPullsOnlyAcceptedRevision(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("config", "user.name", "test")
	git("config", "user.email", "test@example.com")
	script := filepath.Join(root, DetectorDirectory, "health/check/ready.sh")
	if err := os.MkdirAll(filepath.Dir(script), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(script, []byte("exit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "first")
	repository := &GitRepository{Root: root, Scratch: t.TempDir()}
	first, err := repository.Current(context.Background())
	if err != nil || len(first.Scripts) != 1 {
		t.Fatalf("snapshot %#v %v", first, err)
	}
	if err := os.WriteFile(script, []byte("exit 1\n"), 0755); err != nil {
		t.Fatal(err)
	}
	unchanged, err := repository.Current(context.Background())
	if err != nil || unchanged.Commit != first.Commit {
		t.Fatal("uncommitted script activated")
	}
	git("add", ".")
	git("commit", "-qm", "second")
	second, err := repository.Current(context.Background())
	if err != nil || second.Commit == first.Commit || second.Root == first.Root {
		t.Fatalf("accepted script not pulled: %#v %v", second, err)
	}
}
