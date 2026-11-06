package sandboxio

// SetExecutable replaces how the child's executable is found and returns the
// restore function, so a test can make the spawn itself fail.
func SetExecutable(find func() (string, error)) (restore func()) {
	previous := executableFn
	executableFn = find
	return func() { executableFn = previous }
}
