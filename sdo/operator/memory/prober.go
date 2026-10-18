package memory

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// PublishProber places the built binary in a content-addressed shared cache.
// Only this repository edge knows the cache's physical layout.
func PublishProber(repositoryMount, binary string) (string, error) {
	if repositoryMount == "" || binary == "" {
		return "", errors.New("prober binary and repository mount are required")
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		return "", fmt.Errorf("read built prober: %w", err)
	}
	digest := sha256.Sum256(data)
	target := filepath.Join(repositoryMount, ".sdo-prober", fmt.Sprintf("%x", digest[:8]), "sdo-prober")
	if _, err := os.Stat(target); err == nil {
		return target, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return "", err
	}
	staged, err := os.CreateTemp(filepath.Dir(target), ".sdo-prober-")
	if err != nil {
		return "", err
	}
	defer os.Remove(staged.Name())
	if _, err := staged.Write(data); err != nil {
		_ = staged.Close()
		return "", err
	}
	if err := staged.Chmod(0o555); err != nil {
		_ = staged.Close()
		return "", err
	}
	if err := staged.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(staged.Name(), target); err != nil {
		return "", err
	}
	return target, nil
}
