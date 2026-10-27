package runtime

import (
	"fmt"
	"runtime/debug"
)

// ResponderStepPanicError reports a panic inside one step that ran with the
// responder loop locks released. The step fails with this error; the
// controller keeps running.
type ResponderStepPanicError struct {
	Step  string
	Value any
	Stack string
}

func (e *ResponderStepPanicError) Error() string {
	return fmt.Sprintf("responder step %s panicked: %v", e.Step, e.Value)
}

// withoutResponderLocks runs one step with the responder loop mutex released,
// and operationMu too when operation is set, and always reacquires them. It is
// the only way to open an unlock window (TestResponderLoopLocksAreDeferredOrHelped):
// without it, a panic in the step unwinds into the caller's deferred unlocks of
// already released mutexes, an unrecoverable fatal error.
func withoutResponderLocks(loop *ResponderLoop, step string, operation bool, run func()) (err error) {
	loop.mu.Unlock()
	if operation {
		loop.operationMu.Unlock()
	}
	defer func() {
		if operation {
			loop.operationMu.Lock()
		}
		loop.mu.Lock()
		if r := recover(); r != nil {
			err = &ResponderStepPanicError{Step: step, Value: r, Stack: string(debug.Stack())}
		}
	}()
	run()
	return nil
}

// locked runs fn while holding the responder loop mutex.
func (l *ResponderLoop) locked(fn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fn()
}
