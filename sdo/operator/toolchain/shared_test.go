package toolchain

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSharedSeedIsAtomicAndReusable(t *testing.T) {
	root := t.TempDir()
	compiler := filepath.Join(t.TempDir(), "go")
	cache := filepath.Join(t.TempDir(), "cache")
	for _, dir := range []string{compiler, cache} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "content"), []byte("trusted"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := seedFrom(context.Background(), root, "image@sha256:one", compiler, "/missing-cache"); err == nil {
		t.Fatal("missing cache accepted")
	}
	if _, err := os.Stat(filepath.Join(root, SharedSubPath("image@sha256:one"))); !os.IsNotExist(err) {
		t.Fatal("partial seed published")
	}
	if err := seedFrom(context.Background(), root, "image@sha256:one", compiler, cache); err != nil {
		t.Fatal(err)
	}
	if err := seedFrom(context.Background(), root, "image@sha256:one", "/absent", "/absent"); err != nil {
		t.Fatal("seed copied again", err)
	}
	for _, name := range []string{"go", "cache"} {
		data, err := os.ReadFile(filepath.Join(root, SharedSubPath("image@sha256:one"), name, "content"))
		if err != nil || string(data) != "trusted" {
			t.Fatal(name, err, string(data))
		}
	}
	if SharedSubPath("image@sha256:two") == SharedSubPath("image@sha256:one") {
		t.Fatal("image versions share seed")
	}
}
