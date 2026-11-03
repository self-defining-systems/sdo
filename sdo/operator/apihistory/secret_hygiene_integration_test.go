//go:build integration

package apihistory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Credentials never reach API history files. Each body carries the canary where
// Kubernetes puts credentials; after the store records them, no file under the
// history directory may contain it, in any encoding the bodies used.
func TestCredentialsNeverReachHistoryFiles(t *testing.T) {
	const canary = "hygienecanary"
	bodies := map[string]string{
		"secrets": `{"apiVersion":"v1","kind":"Secret","metadata":{"name":"db","namespace":"app"},"data":{"password":"` + canary + `"},"stringData":{"token":"` + canary + `"}}`,
		"pods": `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"web","namespace":"app","annotations":{"note":"` + canary + `"}},"spec":{"containers":[{"name":"web","image":"https://u:` + canary + `@registry.invalid/web:v1","command":["run","--token=` + canary + `"],"env":[{"name":"TOKEN","value":"` + canary + `"}],` +
			`"readinessProbe":{"httpGet":{"path":"/","port":8080,"httpHeaders":[{"name":"Authorization","value":"Bearer ` + canary + `"}]}}}]}}`,
	}
	dir := t.TempDir()
	store, err := Open(dir, "incident", Limits{Bytes: 1 << 20, Events: 16, Queue: 16})
	if err != nil {
		t.Fatal(err)
	}
	for resource, body := range bodies {
		store.Record(Event{Started: time.Now(), Phase: "end", Verb: "get", Resource: resource, Namespace: "app", Name: "x", Status: 200}, []byte(body))
	}
	store.Close()
	files := 0
	err = filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		files++
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), canary) {
			t.Errorf("credential canary stored in %s", path)
		}
		return nil
	})
	if err != nil || files == 0 {
		t.Fatalf("history directory not written: %d files, %v", files, err)
	}
}
