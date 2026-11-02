package runtime

import (
	"testing"

)

// recordingTB captures what guardInformers reports instead of failing the
// real test.
type recordingTB struct {
	testing.TB
	cleanups []func()
	errors   []string
}

func (r *recordingTB) Helper()          {}

func (r *recordingTB) Cleanup(f func()) { r.cleanups = append(r.cleanups, f) }

func (r *recordingTB) Errorf(format string, args ...any) {
	r.errors = append(r.errors, format)
}

func (r *recordingTB) runCleanups() {
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		r.cleanups[i]()
	}
}

