package memory

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteIncidentPagesRewritesFilesInTheSessionIncidentDirectory(t *testing.T) {
	mount := t.TempDir()
	if err := WriteIncidentPages(mount, "inc-1", map[string][]byte{"timeline.csv": []byte("a\n"), "changes.csv": []byte("b\n")}); err != nil {
		t.Fatal(err)
	}
	if err := WriteIncidentPages(mount, "inc-1", map[string][]byte{"timeline.csv": []byte("c\n")}); err != nil {
		t.Fatal(err)
	}
	sub, err := IncidentSessionSubPath("", "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"timeline.csv": "c\n", "changes.csv": "b\n"} {
		got, err := os.ReadFile(filepath.Join(mount, sub, IncidentPagesDir, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s = %q, %v; want %q", name, got, err, want)
		}
	}
}

func TestWriteIncidentPagesRefusesNamesOutsideTheDirectory(t *testing.T) {
	for _, name := range []string{"../x.csv", "a/b.csv", "", "."} {
		if err := WriteIncidentPages(t.TempDir(), "inc-1", map[string][]byte{name: nil}); err == nil {
			t.Fatalf("%q was accepted", name)
		}
	}
}
