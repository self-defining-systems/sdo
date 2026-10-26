package toolchain

import (
	"context"
	"fmt"
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

// Same model-free compiler/cache tree: the old setup makes two full copies,
// while refresh and controller startup now reuse the same image-bound seed.
func BenchmarkSetupToolchain(b *testing.B) {
	source := b.TempDir()
	data := make([]byte, 256<<10)
	for _, name := range []string{"go", "cache"} {
		dir := filepath.Join(source, name)
		if err := os.MkdirAll(dir, 0700); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < 64; i++ {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprint(i)), data, 0600); err != nil {
				b.Fatal(err)
			}
		}
	}
	for _, reuse := range []bool{false, true} {
		name := "private_refresh_then_controller_seed"
		if reuse {
			name = "shared_refresh_then_controller_reuse"
		}
		b.Run(name, func(b *testing.B) {
			root := b.TempDir()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				first := filepath.Join(root, fmt.Sprint(i), "refresh")
				second := filepath.Join(root, fmt.Sprint(i), "controller")
				if reuse {
					second = first
				}
				for _, destination := range []string{first, second} {
					if err := seedFrom(context.Background(), destination, "image@sha256:one", filepath.Join(source, "go"), filepath.Join(source, "cache")); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if err := os.RemoveAll(filepath.Join(root, fmt.Sprint(i))); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}
