package imagesrc

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestRepositoryStripsTagAndDigestOnly(t *testing.T) {
	digest := "@sha256:" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for image, want := range map[string]string{
		"app":                                 "app",
		"app:1.2":                             "app",
		"reg.example/team/app:1.2":            "reg.example/team/app",
		"reg.example:5000/app":                "reg.example:5000/app",
		"reg.example:5000/app:v1":             "reg.example:5000/app",
		"reg.example/app" + digest:            "reg.example/app",
		"reg.example/app:v1" + digest:         "reg.example/app",
		"localhost:5000/team/app:v1" + digest: "localhost:5000/team/app",
	} {
		if got := Repository(image); got != want {
			t.Errorf("Repository(%q) = %q, want %q", image, got, want)
		}
	}
}

func TestSafeCheckoutPath(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "app"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("app", filepath.Join(root, "inside")); err != nil {
		t.Fatal(err)
	}
	real, _ := filepath.EvalSymlinks(root)
	got, err := SafeCheckoutPath(root, "inside")
	if err != nil || got != filepath.Join(real, "app") {
		t.Fatalf("symlink inside the checkout = %q, %v", got, err)
	}
	for _, raw := range []string{"", "..", "../x", "/etc/passwd", "escape", "app/../../x"} {
		if _, err := SafeCheckoutPath(root, raw); err == nil {
			t.Errorf("SafeCheckoutPath(%q) accepted a path outside the checkout", raw)
		}
	}
	if _, err := SafeCheckoutPath(root, "missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("missing path error = %v, want fs.ErrNotExist", err)
	}
}
