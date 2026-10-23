package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

func TestPlanWebhookDeleteUsesRealRenderedSourceAbsence(t *testing.T) {
	for _, mode := range []string{"absent", "present", "invalid-lock", "stale-input"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			git := func(args ...string) string {
				t.Helper()
				out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
				if err != nil {
					t.Fatalf("fixture git: %v %s", err, out)
				}
				return strings.TrimSpace(string(out))
			}
			git("init", "-q")
			git("config", "user.email", "fixture@example.invalid")
			git("config", "user.name", "Synthetic fixture")
			manifest := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\ndata:\n  flag: safe\n"
			if mode == "present" {
				manifest += "---\napiVersion: admissionregistration.k8s.io/v1\nkind: MutatingWebhookConfiguration\nmetadata:\n  name: fixture\nwebhooks: []\n"
			}
			if err := os.WriteFile(filepath.Join(root, "objects.yaml"), []byte(manifest), 0600); err != nil {
				t.Fatal(err)
			}
			git("add", ".")
			git("commit", "-qm", "synthetic source")
			source := rendered.Source{Repository: root, ManifestPaths: []string{"objects.yaml"}}
			snapshot, err := source.BuildLock(context.Background(), git("rev-parse", "HEAD"), "app")
			if err != nil {
				t.Fatal(err)
			}
			if err := snapshot.Write(root); err != nil {
				t.Fatal(err)
			}
			if mode == "invalid-lock" {
				if err := os.WriteFile(filepath.Join(root, rendered.LockPath), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			git("add", ".")
			git("commit", "-qm", "synthetic accepted lock")
			if mode == "stale-input" {
				if err := os.WriteFile(filepath.Join(root, "objects.yaml"), []byte(manifest+"# changed selected input\n"), 0600); err != nil {
					t.Fatal(err)
				}
				git("add", ".")
				git("commit", "-qm", "stale selected input")
			}
			accepted := git("rev-parse", "HEAD")
			ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "fixture"}
			before := planObject(ref, nil)
			delete(before.Object, "spec")
			before.Object["webhooks"] = []any{}
			before.SetUID("fixture-uid")
			before.SetResourceVersion("1")
			safety := fixturePlanSafety(t, ref, before)
			safety.SourceCommit = accepted
			proof := safety.Objects[ref]
			proof.DeleteAllowed = true
			safety.Objects[ref] = proof
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}}
			e := PlanExecutor{Client: client, Source: source, Safety: safety, AcceptedRevision: func() string { return accepted }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: accepted, RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "synthetic reviewed fixture"}}}, false)
			if mode == "absent" {
				if err != nil || !result.VerificationPassed || client.deletes != 1 {
					t.Fatalf("authoritative cluster absence refused: %v", err)
				}
			} else if err == nil || client.deletes != 0 {
				t.Fatal("source presence or evidence gap authorized deletion")
			}
		})
	}
}
