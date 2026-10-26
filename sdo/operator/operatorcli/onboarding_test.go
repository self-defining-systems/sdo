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

func TestInstallExistingOnboardingNeverRecapturesExistingRecord(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "installer-script", Namespace: "app"}})
	launch := InstallExistingConfig{Repository: t.TempDir(), Install: install.Config{Namespace: "app", ControlNamespace: "control"}}
	// Existing installer evidence wins over any new install event.
	if err := onboarding.Stage(ctx, client, "control", "app", strings.Repeat("a", 40), nil, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	pending, err := onboarding.Pending(ctx, client, "control", "app")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := onboarding.Complete(ctx, client, "control", pending); err != nil {
		t.Fatal(err)
	}
	if err := stageExistingNamespace(ctx, client, launch, strings.Repeat("b", 40)); err != nil {
		t.Fatal(err)
	}
}

func TestInstallExistingOnboardingStagesOnlyInstallIdentitiesWithoutHostProbe(t *testing.T) {
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
	if err := stageExistingNamespace(context.Background(), client, launch, commit); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(context.Background(), client, "control", "app")
	if err != nil || record != nil {
		t.Fatalf("install must leave verification pending: %+v %v", record, err)
	}
	pending, err := onboarding.Pending(context.Background(), client, "control", "app")
	if err != nil || pending.GetAcceptedCommit() != commit || len(pending.GetObjects()) != 1 || pending.Objects[0].Name != "installer-script" {
		t.Fatalf("frozen install identities: %+v %v", pending, err)
	}
}

func TestInstallExistingGrantsOnlyAcceptedClusterObjectNames(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git: %s %v", out, err)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("config", "user.name", "Test")
	git("config", "user.email", "test@example.invalid")
	if err := os.WriteFile(filepath.Join(root, "role.yaml"), []byte("apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata:\n  name: app-cleanup\nrules: []\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "role.yaml")
	git("commit", "-qm", "source")
	commit := git("rev-parse", "HEAD")
	client := fake.NewSimpleClientset()
	client.Resources = []*metav1.APIResourceList{{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: "ClusterRole"}}}}
	config := install.Config{Namespace: "app", ControlNamespace: "control"}
	launch := InstallExistingConfig{Repository: root, Install: config}
	launch.Install.ExtraControllerArgs = []string{"--deployed-manifest-path=role.yaml"}
	// Existing onboarding evidence must not suppress authority refresh.
	if err := onboarding.Stage(context.Background(), client, "control", "app", commit, nil, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	hook := existingNamespaceOnboardingHook(client, launch, commit)
	if err := hook(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	roles, err := client.RbacV1().ClusterRoles().List(context.Background(), metav1.ListOptions{})
	if err != nil || len(roles.Items) != 1 || len(roles.Items[0].Rules) != 1 || roles.Items[0].Rules[0].ResourceNames[0] != "app-cleanup" {
		t.Fatalf("accepted source grants: %+v %v", roles, err)
	}
}
