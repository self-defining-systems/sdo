package sandboxio

import "errors"

// SetExecutable replaces how the child's executable is found and returns the
// restore function, so a test can make the spawn itself fail.
func SetExecutable(find func() (string, error)) (restore func()) {
	previous := executableFn
	executableFn = find
	return func() { executableFn = previous }
}

// Classify exposes how a started child's failure is typed.
func Classify(stderr string, peak int64, limits Limits) error {
	return classify("t", errors.New("exit status 2"), stderr, peak, limits.withDefaults())
}
