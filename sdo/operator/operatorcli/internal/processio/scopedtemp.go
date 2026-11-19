package processio

import (
	"errors"
	"os"
)

// ScopedTemp points TMPDIR and GOTMPDIR (when set) at fresh directories inside their current values and returns the
// function that removes them and restores the environment. Whatever a Go build of this process leaves in them (its
// staged workspace, the toolchain's work directories of a build that was killed) goes with them, while the directories
// of a controller sharing the same volume are never touched.
func ScopedTemp(prefix string) (func(), error) {
	var undo []func()
	cleanup := func() {
		for i := len(undo) - 1; i >= 0; i-- {
			undo[i]()
		}
	}
	for _, name := range []string{"TMPDIR", "GOTMPDIR"} {
		base := os.Getenv(name)
		set := base != ""
		if !set && name == "GOTMPDIR" {
			continue // the toolchain then uses TMPDIR, already scoped
		}
		dir, err := os.MkdirTemp(base, prefix)
		if err != nil {
			cleanup()
			return nil, err
		}
		if err := os.Setenv(name, dir); err != nil {
			_ = os.RemoveAll(dir)
			cleanup()
			return nil, err
		}
		name, base, set, dir := name, base, set, dir
		undo = append(undo, func() {
			_ = os.RemoveAll(dir)
			if set {
				_ = os.Setenv(name, base)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
	if len(undo) == 0 {
		return nil, errors.New("no temporary directory to scope")
	}
	return cleanup, nil
}
