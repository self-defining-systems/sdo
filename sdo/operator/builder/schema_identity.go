package builder

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// SchemaIdentity fingerprints the Go SDK and builder contract baked into a
// validator image. The identity has no Python source dependency.
func SchemaIdentity(operatorRoot string) (string, error) {
	root, err := filepath.Abs(operatorRoot)
	if err != nil {
		return "", err
	}
	var files []string
	for _, packageName := range []string{"sdk", "builder", "memory/validate"} {
		packageRoot := filepath.Join(root, packageName)
		before := len(files)
		err = filepath.WalkDir(packageRoot, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() && entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				return nil
			}
			files = append(files, path)
			return nil
		})
		if err != nil {
			return "", fmt.Errorf("read validator %s schema: %w", packageName, err)
		}
		if len(files) == before {
			return "", fmt.Errorf("validator %s Go schema source missing: %s", packageName, packageRoot)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i] < files[j] })
	digest := sha256.New()
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read validator schema source %s: %w", path, err)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return "", err
		}
		_, _ = digest.Write([]byte(filepath.ToSlash(relative)))
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write(data)
		_, _ = digest.Write([]byte{0})
	}
	return fmt.Sprintf("sdo.dev/v1alpha1+sdk.%x", digest.Sum(nil)[:8]), nil
}
