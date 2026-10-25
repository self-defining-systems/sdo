package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/install"
	"sdo.dev/operator/onboarding"
)

type onboardingHealthProbe struct {
	status string
	calls  int
}

func (p *onboardingHealthProbe) Evaluate(context.Context, string, string, string) (string, error) {
	p.calls++
	return p.status, nil
}
func TestInstallExistingOnboardingRequiresIndependentClearAndNeverRecaptures(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "installer-script", Namespace: "app"}})
	probe := &onboardingHealthProbe{status: "firing"}
	launch := InstallExistingConfig{Repository: t.TempDir(), Install: install.Config{Namespace: "app", ControlNamespace: "control"}}
	// No source content is needed until independent health is clear.
	if err := acknowledgeExistingNamespace(ctx, client, launch, strings.Repeat("a", 40), probe); err == nil {
		t.Fatal("firing health granted onboarding")
	}
	if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record != nil {
		t.Fatalf("unhealthy acknowledgement persisted: %+v %v", record, err)
	}
	// Existing installer evidence wins over any new event or unhealthy probe.
	if _, err := onboarding.Establish(ctx, client, "control", "app", strings.Repeat("a", 40), nil, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if err := acknowledgeExistingNamespace(ctx, client, launch, strings.Repeat("b", 40), probe); err != nil {
		t.Fatal(err)
	}
	if probe.calls != 1 {
		t.Fatal("restart/reinstall repeated independent build or recaptured inventory")
	}
}

func TestInstallExistingClearAdoptionPersistsOnlyOutOfSourceIdentities(t *testing.T) {
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
	git("config", "user.name", "SDO Test")
	git("config", "user.email", "test@example.invalid")
	if err := os.WriteFile(filepath.Join(root, "app.yaml"), []byte("apiVersion: v1\nkind: Service\nmetadata:\n  name: query\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "app.yaml")
	git("commit", "-qm", "accepted source")
	commit := git("rev-parse", "HEAD")
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "installer-script", Namespace: "app"}}, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "query", Namespace: "app"}})
	launch := InstallExistingConfig{Repository: root, Install: install.Config{Namespace: "app", ControlNamespace: "control", ExtraControllerArgs: []string{"--deployed-manifest-path=app.yaml"}}}
	probe := &onboardingHealthProbe{status: "clear"}
	if err := acknowledgeExistingNamespace(context.Background(), client, launch, commit, probe); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(context.Background(), client, "control", "app")
	if err != nil || record.GetAcceptedCommit() != commit || len(record.GetObjects()) != 1 || record.Objects[0].Name != "installer-script" {
		t.Fatalf("verified adoption record: %+v %v", record, err)
	}
}
