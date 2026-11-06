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
	return classify("t", errors.New("exit status 2"), stderr, peak, false, limits.withDefaults())
}

// HoldSlots takes n child slots (all of them when n is MaxChildren) and
// returns the function that gives them back.
func HoldSlots(n int) (release func()) {
	for i := 0; i < n; i++ {
		slots <- struct{}{}
	}
	return func() {
		for i := 0; i < n; i++ {
			<-slots
		}
	}
}

// MaxChildren is the number of child slots.
const MaxChildren = maxChildren

// ClassifyKill types a child that died of a SIGKILL the parent did not send.
func ClassifyKill(peak int64, limits Limits) error {
	return classify("t", errors.New("signal: killed"), "", peak, true, limits.withDefaults())
}
