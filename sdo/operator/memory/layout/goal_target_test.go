package layout

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadGoalDetectionTarget(t *testing.T) {
	for _, tc := range []struct {
		body    string
		want    time.Duration
		invalid bool
	}{
		{body: "Stay healthy."},
		{body: "Detection target: 45s", want: 45 * time.Second},
		{body: "Detection target: soon", invalid: true},
		{body: "Detection target: 0s", invalid: true},
		{body: "Detection target: 30s\nDetection target: 45s", invalid: true},
	} {
		root := t.TempDir()
		path := filepath.Join(root, GoalFile)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("# Health objective\n\n"+tc.body), 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := ReadGoalDetectionTarget(root)
		if got != tc.want || (err != nil) != tc.invalid {
			t.Errorf("%q: got %v, %v", tc.body, got, err)
		}
	}
}
