package memory

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestCaptureIncidentCorpusKeepsFirstRealSnapshot(t *testing.T) {
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "app"}}}}
	if err := CaptureIncidentSnapshot(root, "incident", "open", view, "app"); err != nil {
		t.Fatal(err)
	}
	view.PodList[0].Name = "changed"
	if err := CaptureIncidentSnapshot(root, "incident", "open", view, "app"); err != nil {
		t.Fatal(err)
	}
	if err := CaptureIncidentSnapshot(root, "incident", "fixed", view, "app"); err != nil {
		t.Fatal(err)
	}
	path, err := IncidentCorpusDir(root, "incident")
	if err != nil {
		t.Fatal(err)
	}
	open, err := os.ReadFile(filepath.Join(path, "open.json"))
	if err != nil || !strings.Contains(string(open), "broken") || strings.Contains(string(open), "changed") {
		t.Fatalf("open snapshot changed: %s, %v", open, err)
	}
	fixed, err := os.ReadFile(filepath.Join(path, "fixed.json"))
	if err != nil || !strings.Contains(string(fixed), "changed") {
		t.Fatalf("fixed snapshot missing: %s, %v", fixed, err)
	}
	if err := CaptureIncidentSnapshot(root, "incident", "healthy", view, "app"); err == nil {
		t.Fatal("arbitrary capture phase accepted")
	}
}

func TestCaptureIncidentCorpusKeepsSeparateOpenMoments(t *testing.T) {
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "detect", Namespace: "app"}}}}
	for _, phase := range []string{"open@detect", "open@settle", "open@pre-apply"} {
		if err := CaptureIncidentSnapshot(root, "incident", phase, view, "app"); err != nil {
			t.Fatal(err)
		}
		view.PodList[0].Name = phase
	}
	dir, err := IncidentCorpusDir(root, "incident")
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"open@detect", "open@settle", "open@pre-apply"} {
		if _, err := os.Stat(filepath.Join(dir, phase+".json")); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIncidentCaptureNeverPersistsConfigMapValues(t *testing.T) {
	const canary = "CORPUS_SYNTHETIC_CONFIGMAP_CREDENTIAL_CANARY"
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: "app"}, Data: map[string]string{"ca.crt": canary}, BinaryData: map[string][]byte{"binary": []byte(canary)}}}}
	for _, phase := range []string{"open@detect", "open@settle", "open@pre-apply", "fixed"} {
		if err := CaptureIncidentSnapshot(root, "synthetic", phase, view, "app"); err != nil {
			t.Fatal(err)
		}
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(canary))
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), canary) || strings.Contains(string(data), encoded) {
			t.Errorf("ConfigMap credential persisted in %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if view.ConfigMapList[0].Data["ca.crt"] != canary {
		t.Fatal("live ConfigMap changed")
	}
}

func TestDatabaseCatalogCaptureKeepsBoundaryFirstObservation(t *testing.T) {
	root := t.TempDir()
	const incident = "database-recording-boundary"
	if err := CaptureIncidentSnapshot(root, incident, "open@pre-apply", sdktest.Snapshot{NamespaceName: "app"}, "app"); err != nil {
		t.Fatal(err)
	}
	target := program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "database"}, Container: "mongo", Database: "app"}
	args, _ := json.Marshal([]program.DatabaseTarget{target})
	first := sdktest.PrimitiveCall{Method: "DatabaseUsers", Arguments: args, Result: json.RawMessage(`[]`)}
	if err := AppendCapturedRecording(root, incident, "open@pre-apply", first); err != nil {
		t.Fatal(err)
	}
	// A second responder action sees the restored catalog. This is a later
	// observation, not a replacement for the immutable pre-repair boundary.
	later := first
	later.Result = json.RawMessage(`[{"name":"app","roles":[]}]`)
	if err := AppendCapturedRecording(root, incident, "open@pre-apply", later); err != nil {
		t.Fatal(err)
	}
	dir, err := IncidentCorpusDir(root, incident)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "open@pre-apply-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(data, &calls); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || string(calls[0].Result) != "[]" {
		t.Fatal("later database read replaced/conflicted with the open catalog")
	}
	target.Shell = "mongosh"
	target.Auth = &program.DatabaseAuth{Database: "admin", Username: program.CredentialRef{Env: "NEW_USER"}, Password: program.CredentialRef{Env: "NEW_PASSWORD"}}
	later.Arguments, _ = json.Marshal([]program.DatabaseTarget{target})
	if err := AppendCapturedRecording(root, incident, "open@pre-apply", later); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "open@pre-apply-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &calls); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 {
		t.Fatal("post-repair authentication configuration became open evidence")
	}
	later.Phase = "fixed"
	if err := AppendCapturedRecording(root, incident, "open@pre-apply", later); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "open@pre-apply-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &calls); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[1].Phase != "fixed" {
		t.Fatal("independent fixed observation was discarded")
	}
}
