package memory

import (
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk"
	"testing"
)

func TestCaptureScopeReadsAuditActionsAndFindings(t *testing.T) {
	root := t.TempDir()
	corpus, err := IncidentCorpusDir(root, "incident")
	if err != nil {
		t.Fatal(err)
	}
	session := filepath.Dir(corpus)
	for name, body := range map[string]string{
		"api-history/events.jsonl": `{"group":"admissionregistration.k8s.io","version":"v1","resource":"validatingwebhookconfigurations","name":"policy","verb":"get"}` + "\n" + `{"group":"apps","version":"v1","resource":"deployments","namespace":"policy-system","name":"server","verb":"patch"}`,
		"repair-actions.jsonl":     `{"resources":[{"api_version":"v1","kind":"PersistentVolumeClaim","namespace":"app","name":"claim"}]}`,
		"commands.jsonl":           `{"command":"kubectl -n another get widgets.example.test custom"}`,
	} {
		if err = os.WriteFile(filepath.Join(session, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := CapturedSessionRefs(session, []sdk.ObjectRef{{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []sdk.ObjectRef{{APIVersion: "admissionregistration.k8s.io/v1", Kind: "validatingwebhookconfigurations", Name: "policy"}, {APIVersion: "apps/v1", Kind: "deployments", Namespace: "policy-system", Name: "server"}, {APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "app", Name: "claim"}, {Kind: "widgets.example.test", Namespace: "another", Name: "custom"}, {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}} {
		found := false
		for _, got := range refs {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing %+v in %+v", want, refs)
		}
	}
}
