package operatorcli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// BaselineDirectory is a controller-owned volume location outside accepted
// operational memory. The validator receives only this subdirectory.
const BaselineDirectory = "/workspace/.sdo-baseline/healthy"

var baselineFileName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*\.json$`)

// RunBaselineWrite publishes a bounded set of recorded healthy snapshots.
// It is invoked in the Go-only controller image by the benchmark adapter.
func RunBaselineWrite(input io.Reader, directory string) error {
	data, err := io.ReadAll(io.LimitReader(input, 8<<20+1))
	if err != nil {
		return err
	}
	if len(data) > 8<<20 {
		return errors.New("healthy baseline exceeds 8 MiB")
	}
	var files map[string]json.RawMessage
	if err := json.Unmarshal(data, &files); err != nil {
		return fmt.Errorf("decode healthy snapshots: %w", err)
	}
	if len(files) == 0 || len(files) > 16 {
		return errors.New("healthy baseline requires 1-16 snapshots")
	}
	for name, document := range files {
		if !baselineFileName.MatchString(name) || !json.Valid(document) || len(document) > 1<<20 {
			return fmt.Errorf("invalid healthy snapshot %q", name)
		}
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
			return fmt.Errorf("unexpected healthy baseline entry %q", entry.Name())
		}
	}
	for name, document := range files {
		temporary, err := os.CreateTemp(directory, ".snapshot-*")
		if err != nil {
			return err
		}
		if _, err := temporary.Write(document); err != nil {
			temporary.Close()
			os.Remove(temporary.Name())
			return err
		}
		if err := temporary.Close(); err != nil {
			os.Remove(temporary.Name())
			return err
		}
		if err := os.Rename(temporary.Name(), filepath.Join(directory, name)); err != nil {
			os.Remove(temporary.Name())
			return err
		}
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") {
			if _, keep := files[entry.Name()]; !keep {
				if err := os.Remove(filepath.Join(directory, entry.Name())); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
