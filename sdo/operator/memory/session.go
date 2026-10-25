package memory

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sdo.dev/operator/apihistory"
	"strings"
)

// PrepareIncidentSession owns the physical storage location for one agent
// conversation. Workers receive only its narrow PVC subPath, never the
// accepted checkout or another incident's session directory.
func PrepareIncidentSession(repositoryMount, pvcSubPath, incidentID string) (string, error) {
	if !filepath.IsAbs(repositoryMount) || strings.TrimSpace(incidentID) == "" || len(incidentID) > 512 {
		return "", errors.New("incident session requires an absolute repository mount and bounded incident ID")
	}
	if pvcSubPath != "" {
		clean := path.Clean(pvcSubPath)
		if clean != pvcSubPath || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) || strings.Contains(clean, "\\") {
			return "", errors.New("incident session PVC subPath must stay below the repository volume")
		}
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(incidentID)))[:32]
	local := filepath.Join(repositoryMount, ".sdo-runtime", "sessions", digest)
	for _, directory := range []string{filepath.Join(repositoryMount, ".sdo-runtime"), filepath.Join(repositoryMount, ".sdo-runtime", "sessions"), local, filepath.Join(local, "api-history")} {
		if err := os.Mkdir(directory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("prepare incident session directory: %w", err)
		}
		info, err := os.Lstat(directory)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("incident session directory is not a real directory: %s", directory)
		}
	}
	if _, err := os.Stat(apihistory.GenerationPath(filepath.Join(local, "api-history"))); errors.Is(err, os.ErrNotExist) {
		if err := apihistory.BeginGeneration(filepath.Join(local, "api-history")); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}
	if _, err := apihistory.PrepareMutationGate(filepath.Join(local, "api-history")); err != nil {
		return "", err
	}
	return path.Join(pvcSubPath, ".sdo-runtime", "sessions", digest), nil
}

// RemoveIncidentSession deletes only the acknowledged incident's private
// agent session tree. Unacknowledged incidents retain it for durable retries.
func RemoveIncidentSession(repositoryMount, incidentID string) error {
	if !filepath.IsAbs(repositoryMount) || strings.TrimSpace(incidentID) == "" || len(incidentID) > 512 {
		return errors.New("incident session cleanup requires an absolute repository mount and bounded incident ID")
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(incidentID)))[:32]
	base := filepath.Join(repositoryMount, ".sdo-runtime")
	sessions := filepath.Join(base, "sessions")
	for _, directory := range []string{base, sessions} {
		info, err := os.Lstat(directory)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("incident session parent is not a real directory: %s", directory)
		}
	}
	return os.RemoveAll(filepath.Join(sessions, digest))
}
