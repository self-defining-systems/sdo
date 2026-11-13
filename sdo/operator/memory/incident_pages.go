package memory

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// IncidentPagesDir is the directory, inside an incident's session, that holds
// the page-time files the controller writes for the responder (timeline and
// changes). The agent sees it at /sdo/session/incident.
const IncidentPagesDir = "incident"

// WriteIncidentPages replaces the named page files in the incident's session.
// Each file is written to a temporary name and renamed, so a reader never sees
// a partial file. Names are plain file names inside IncidentPagesDir.
// It prepares the session when it does not exist yet (dispatch).
func WriteIncidentPages(repositoryMount, incidentID string, files map[string][]byte) error {
	return writeIncidentPages(repositoryMount, incidentID, files, true)
}

// RewriteIncidentPages replaces the page files of a session that already
// exists (each `sdo detect`); it never recreates a removed session.
func RewriteIncidentPages(repositoryMount, incidentID string, files map[string][]byte) error {
	return writeIncidentPages(repositoryMount, incidentID, files, false)
}

func writeIncidentPages(repositoryMount, incidentID string, files map[string][]byte, prepare bool) error {
	names := make([]string, 0, len(files))
	for name := range files {
		if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
			return fmt.Errorf("incident page name %q must be a plain file name", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	var relative string
	var err error
	if prepare {
		relative, err = PrepareIncidentSession(repositoryMount, "", incidentID)
	} else if relative, err = IncidentSessionSubPath("", incidentID); err == nil {
		if !filepath.IsAbs(repositoryMount) {
			err = errors.New("incident pages require an absolute repository mount")
		} else if info, statErr := os.Lstat(filepath.Join(repositoryMount, relative)); statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			err = fmt.Errorf("incident session for %s is not prepared", incidentID)
		}
	}
	if err != nil {
		return err
	}
	directory := filepath.Join(repositoryMount, relative, IncidentPagesDir)
	if err := os.Mkdir(directory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("prepare incident pages directory: %w", err)
	}
	if info, err := os.Lstat(directory); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("incident pages directory is not a real directory: %s", directory)
	}
	for _, name := range names {
		// A unique temporary name: concurrent `sdo detect` reads may rewrite at once.
		file, err := os.CreateTemp(directory, "."+name+".*")
		if err != nil {
			return fmt.Errorf("write incident page %s: %w", name, err)
		}
		_, err = file.Write(files[name])
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			err = os.Rename(file.Name(), filepath.Join(directory, name))
		}
		if err != nil {
			_ = os.Remove(file.Name())
			return fmt.Errorf("write incident page %s: %w", name, err)
		}
	}
	return nil
}
