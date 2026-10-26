package broker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/rendered"
)

func rv4PinnedSnapshot(t *testing.T) (*Broker, rendered.Source, string, rendered.Snapshot) {
	t.Helper()
	root := t.TempDir()
	testutil.InitGit(t, root)
	for _, args := range [][]string{{"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}} {
		if _, err := runGit(context.Background(), root, args...); err != nil {
			t.Fatal(err)
		}
	}
	body := `apiVersion: apps/v1
kind: Deployment
metadata: {name: api}
spec:
  replicas: 1
  selector:
    matchLabels: {app: api}
  template:
    metadata:
      labels: {app: api}
    spec:
      initContainers:
      - name: init
        image: example/init@sha256:AAAAAAAA
      containers:
      - name: api
        image: example/api@sha256:BBBBBBBB
`
	body = strings.ReplaceAll(body, "AAAAAAAA", strings.Repeat("a", 64))
	body = strings.ReplaceAll(body, "BBBBBBBB", strings.Repeat("b", 64))
	if err := os.WriteFile(filepath.Join(root, "app.yaml"), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := runGit(context.Background(), root, "add", "."); err != nil {
		t.Fatal(err)
	}
	if _, err := runGit(context.Background(), root, "commit", "-qm", "source"); err != nil {
		t.Fatal(err)
	}
	head, err := gitText(context.Background(), root, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	source := rendered.Source{Repository: root, ManifestPaths: []string{"app.yaml"}}
	snapshot, err := source.BuildLock(context.Background(), head, "app")
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(root, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return b, source, head, snapshot
}

func TestRV4BrokerRejectsHandcraftedPinnedImage(t *testing.T) {
	b, _, head, snapshot := rv4PinnedSnapshot(t)
	changed := "other/image@sha256:" + strings.Repeat("c", 64)
	for i, entry := range snapshot.Lock.Objects {
		var doc map[string]any
		if err := yaml.Unmarshal(snapshot.Files[entry.File], &doc); err != nil {
			t.Fatal(err)
		}
		containers := doc["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
		containers[0].(map[string]any)["image"] = changed
		data, err := yaml.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		snapshot.Files[entry.File] = data
		snapshot.Lock.Objects[i].SHA256 = fmt.Sprintf("%x", sum)
		for j := range snapshot.Lock.Objects[i].Images {
			if !snapshot.Lock.Objects[i].Images[j].Init {
				snapshot.Lock.Objects[i].Images[j].Digest = changed
			}
		}
	}
	locked, err := b.RecordRenderedLock(context.Background(), head, snapshot)
	if err != nil {
		t.Logf("handcrafted lock rejected: %v", err)
		return
	}
	t.Fatalf("broker accepted handcrafted image substitution absent from pinned source, commit=%s", locked)
}

func TestRV4BrokerRejectsRealStaleSourceAndExpectedHead(t *testing.T) {
	b, source, head, snapshot := rv4PinnedSnapshot(t)
	if err := os.WriteFile(filepath.Join(source.Repository, "config.env"), []byte("PORT=9000\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := runGit(context.Background(), source.Repository, "add", "config.env"); err != nil {
		t.Fatal(err)
	}
	if _, err := runGit(context.Background(), source.Repository, "commit", "-qm", "application input"); err != nil {
		t.Fatal(err)
	}
	next, err := b.Head(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.RecordRenderedLock(context.Background(), head, snapshot); err == nil {
		t.Fatal("stale expected head accepted")
	} else {
		t.Logf("old expected head rejected: %v", err)
	}
	if _, err := b.RecordRenderedLock(context.Background(), next, snapshot); err == nil {
		t.Fatal("stale source accepted")
	} else {
		t.Logf("old source inputs rejected: %v", err)
	}
}

// This probes whether Go-only publication also checks evidence provenance.
func TestRV4BrokerHandcraftedGapProbe(t *testing.T) {
	b, _, head, snapshot := rv4PinnedSnapshot(t)
	entry := snapshot.Lock.Objects[0]
	delete(snapshot.Files, entry.File)
	snapshot.Lock.Objects[0] = rendered.Object{Ref: entry.Ref, Error: "invented gap"}
	locked, err := b.RecordRenderedLock(context.Background(), head, snapshot)
	t.Logf("fabricated gap result: commit=%s error=%v", locked, err)
}
