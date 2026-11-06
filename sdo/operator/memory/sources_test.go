package memory_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
)

func syntheticSource(extra map[string]any) []layout.Source {
	config := map[string]any{"repository": "git://synthetic.invalid/app.git", "ref": "main", "poll_interval": "45s"}
	for k, v := range extra {
		config[k] = v
	}
	return []layout.Source{{Path: ".", Kind: "git", Config: config}}
}

func TestWriteGoalPublishesSourcesTheLoaderReads(t *testing.T) {
	checkout := t.TempDir()
	if err := memory.WriteGoal(checkout, "app", "stay healthy", syntheticSource(nil)); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{layout.GoalFile, layout.SourcesFile} {
		if _, err := os.Stat(filepath.Join(checkout, filepath.FromSlash(rel))); err != nil {
			t.Fatalf("WriteGoal did not write %s: %v", rel, err)
		}
	}
	sources, err := memory.ReadSources(checkout, upstreams.Catalog())
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].Kind != "git" || sources[0].Path != "." || sources[0].PollInterval != 45*time.Second {
		t.Fatalf("sources = %+v", sources)
	}
}

func TestWriteGoalWithoutSourcesFollowsNothing(t *testing.T) {
	checkout := t.TempDir()
	if err := memory.WriteGoal(checkout, "app", "stay healthy", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(checkout, filepath.FromSlash(layout.SourcesFile))); !os.IsNotExist(err) {
		t.Fatalf("sources file written without sources: %v", err)
	}
	sources, err := memory.ReadSources(checkout, upstreams.Catalog())
	if err != nil || sources != nil {
		t.Fatalf("ReadSources = %v, %v; want nil, nil", sources, err)
	}
}

func TestReadSourcesRejectsWhatTheCatalogCannotFollow(t *testing.T) {
	for name, tc := range map[string]struct {
		sources []layout.Source
		want    string
	}{
		"unknown kind":  {[]layout.Source{{Path: ".", Kind: "helm-repo", Config: map[string]any{"a": "b"}}}, "unknown upstream watcher"},
		"unknown field": {syntheticSource(map[string]any{"branch": "x"}), "branch"},
		"bad interval":  {syntheticSource(map[string]any{"poll_interval": "soon"}), "Duration"},
	} {
		t.Run(name, func(t *testing.T) {
			checkout := t.TempDir()
			target := filepath.Join(checkout, filepath.FromSlash(layout.SourcesFile))
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				t.Fatal(err)
			}
			encoded, err := layout.MarshalSources(layout.Sources{Sources: tc.sources})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(target, encoded, 0o644); err != nil {
				t.Fatal(err)
			}
			_, err = memory.ReadSources(checkout, upstreams.Catalog())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
