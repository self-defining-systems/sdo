package memory

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/safefs"
	"sdo.dev/operator/sdk/sdktest"
)

// The responder's pod mounts its session directory writable, so the capture
// directory below it, and anything in it, can be a link the agent planted.
func TestCaptureWritersRefusePlantedLinks(t *testing.T) {
	const incident = "synthetic-incident"
	view := sdktest.Snapshot{NamespaceName: "app"}
	recordings := "open@detect-recordings.json"
	call := sdktest.PrimitiveCall{Method: "ProbeTCP", Arguments: []byte("[]")}
	cases := []struct {
		name string
		// link is the planted path below the capture directory.
		link string
		// files are what the writer reads, relative to the capture directory.
		files map[string]string
		write func(t *testing.T, root string) error
	}{
		{name: "snapshot", write: func(_ *testing.T, root string) error {
			return CaptureIncidentSnapshot(root, incident, "open@detect", view, "app")
		}},
		{name: "recordings", files: map[string]string{recordings: "[]"}, write: func(_ *testing.T, root string) error {
			return AppendCapturedRecording(root, incident, "open@detect", call)
		}},
		{name: "recordings file", link: recordings, files: map[string]string{recordings: "[]"}, write: func(_ *testing.T, root string) error {
			return AppendCapturedRecording(root, incident, "open@detect", call)
		}},
		{name: "incident healthy baseline", link: "healthy", write: func(t *testing.T, root string) error {
			if err := CaptureHealthyBaseline(root, "revision", view, "app"); err != nil {
				t.Fatal(err)
			}
			return BindIncidentHealthyBaseline(root, "revision", incident)
		}},
		{name: "closure healthy baseline", files: map[string]string{"fixed.json": "{}"}, write: func(_ *testing.T, root string) error {
			return BindClosureHealthyBaseline(root, incident)
		}},
		{name: "pre-apply from writes", files: map[string]string{"open@detect.json": `{"namespace":"app"}`}, write: func(t *testing.T, root string) error {
			recordBeforeState(t, root, incident)
			return CapturePreApplyFromWrites(root, incident, recordedAt.Add(time.Hour))
		}},
		{name: "join context", write: func(_ *testing.T, root string) error {
			return CaptureJoinContext(root, incident, 1, JoinCaptureContext{})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root, outside := t.TempDir(), t.TempDir()
			corpus, err := IncidentCorpusDir(root, incident)
			if err != nil {
				t.Fatal(err)
			}
			for name, content := range c.files {
				if err := os.WriteFile(filepath.Join(outside, name), []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			link, target := corpus, outside
			if c.link != "" {
				if err := os.Mkdir(corpus, 0o700); err != nil {
					t.Fatal(err)
				}
				link = filepath.Join(corpus, c.link)
				if _, read := c.files[c.link]; read {
					target = filepath.Join(outside, c.link)
				}
			}
			if err := os.Symlink(target, link); err != nil {
				t.Fatal(err)
			}
			before := treeContents(t, outside)
			if err := c.write(t, root); !errors.Is(err, safefs.ErrSymlink) {
				t.Fatalf("want ErrSymlink, got %v", err)
			}
			if after := treeContents(t, outside); !reflect.DeepEqual(before, after) {
				t.Fatalf("wrote through the planted link: %v", after)
			}
		})
	}
}

var recordedAt = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

// recordBeforeState records one agent write's before-state in the incident's
// API history.
func recordBeforeState(t *testing.T, root, incident string) {
	t.Helper()
	relative, err := PrepareIncidentSession(root, "", incident)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, relative, "api-history")
	lock, err := apihistory.BeginWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.Close() })
	store, err := apihistory.Open(dir, incident, apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.Record(apihistory.Event{RequestID: "aaaaaaaab", Phase: "before", Started: recordedAt, Verb: "get", Version: "v1", Resource: "services", Namespace: "app", Name: "api"},
		[]byte(`{"apiVersion":"v1","kind":"Service","metadata":{"name":"api","namespace":"app"}}`))
}

func treeContents(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		var data []byte
		if !d.IsDir() {
			if data, err = os.ReadFile(p); err != nil {
				return err
			}
		}
		out[p] = string(data)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}
