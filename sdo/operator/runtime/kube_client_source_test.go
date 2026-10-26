package runtime

import (
	"errors"
	"os"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestClientGoSourceBindingBecomesStaleAfterAcceptedRoleRemoval(t *testing.T) {
	root := t.TempDir()
	testutil.InitGit(t, root)
	nativeGit(t, root, "config", "user.name", "Test")
	nativeGit(t, root, "config", "user.email", "test@example.com")
	write := func(name string) string {
		t.Helper()
		manifest := "apiVersion: v1\nkind: Service\nmetadata:\n  name: " + name + "\n  namespace: app\nspec:\n  selector: {app: api}\n  ports: [{port: 80, targetPort: 8080}]\n"
		if err := os.WriteFile(filepath.Join(root, "app.yaml"), []byte(manifest), 0600); err != nil {
			t.Fatal(err)
		}
		nativeGit(t, root, "add", "app.yaml")
		nativeGit(t, root, "commit", "-qm", "accepted source")
		return seedRenderedLock(t, GitPlanSource{Repository: root, ManifestPaths: []string{"app.yaml"}}, nativeGit(t, root, "rev-parse", "HEAD"), "app")
	}
	accepted := write("api")
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}, Ports: []corev1.ServicePort{{Port: 80}}}}}}
	view := LiveReadView{DetectionContext: snapshot, Source: GitPlanSource{Repository: root, ManifestPaths: []string{"app.yaml"}}, SourceCommit: accepted}
	bindings := program.Bindings{Objects: program.ObjectBindings{"frontend": ref}}
	if _, err := program.Kubernetes(view, bindings); err != nil {
		t.Fatalf("accepted role: %v", err)
	}
	view.SourceCommit = write("replacement")
	_, err := program.Kubernetes(view, bindings)
	var stale *program.StaleBindingError
	if !errors.As(err, &stale) || stale.Role != "frontend" || stale.Resource != ref {
		t.Fatalf("removed role reused despite source change: %v", err)
	}
}
