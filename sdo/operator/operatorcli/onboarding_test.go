package operatorcli

import (
	"context"
	"net"
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
	err    error
	status string
	calls  int
}

func (p *onboardingHealthProbe) Evaluate(context.Context, string, string, string) (string, error) {
	p.calls++
	return p.status, p.err
}
func TestInstallExistingOnboardingRequiresIndependentClearAndNeverRecaptures(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "installer-script", Namespace: "app"}})
	probe := &onboardingHealthProbe{status: "firing"}
	launch := InstallExistingConfig{Repository: t.TempDir(), Install: install.Config{Namespace: "app", ControlNamespace: "control"}}
	// Existing installer evidence wins over any new event or unhealthy probe.
	if _, err := onboarding.Establish(ctx, client, "control", "app", strings.Repeat("a", 40), nil, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if err := acknowledgeExistingNamespace(ctx, client, launch, strings.Repeat("b", 40), probe); err != nil {
		t.Fatal(err)
	}
	if probe.calls != 0 {
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
	// Exercise the production install-existing hook through the real prepared
	// installer: repository validation, resource planning, hook, controller Job.
	config := launch.Install
	config.AcceptedCommit = commit
	config.Application = "app"
	config.Model = "fixture-no-model-calls"
	config.TimeoutSeconds = 60
	config.NativeCommand = []string{"/usr/local/bin/sdo-operator"}
	config.BuildKitImage = "moby/buildkit@sha256:" + strings.Repeat("a", 64)
	config.RegistryPushSecret = "registry-push"
	config.RegistryPushDockerConfig = []byte("{}")
	config.ControllerImage = "controller:test"
	config.ResponderImage = "responder:test"
	config.ValidatorImage = "validator:test"
	config.RepositoryPVC = "accepted"
	config.CredentialsSecret = "worker-credentials"
	installer := install.Installer{Client: client, BeforeControllerStart: existingNamespaceOnboardingHook(client, launch, commit)}
	if _, err := installer.InstallPrepared(context.Background(), config, root); err != nil {
		t.Fatal(err)
	}
	if _, err := client.BatchV1().Jobs("control").Get(context.Background(), install.ControllerJobName, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	probe := &onboardingHealthProbe{err: &net.DNSError{Name: "consul.app.svc.cluster.local", Err: "no such host", IsNotFound: true}}
	if err := acknowledgeExistingNamespace(context.Background(), client, launch, commit, probe); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(context.Background(), client, "control", "app")
	if probe.calls != 0 {
		t.Fatalf("install ran %d host-local health probes; cluster DNS is unavailable on the host", probe.calls)
	}
	if err != nil || record != nil {
		t.Fatalf("install must leave verification pending: %+v %v", record, err)
	}
	pending, err := onboarding.Pending(context.Background(), client, "control", "app")
	if err != nil || pending.GetAcceptedCommit() != commit || len(pending.GetObjects()) != 1 || pending.Objects[0].Name != "installer-script" {
		t.Fatalf("frozen install identities: %+v %v", pending, err)
	}
}
