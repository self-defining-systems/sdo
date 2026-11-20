package runtime

import "testing"

// A responder that polls `sdo detect --wait` holds operationMu for reading for
// most of every poll, for minutes. A read is not a write, so it must never make
// the closure pass unsafe: each unsafe pass restarts the stability window, and
// an incident whose alerts cleared would stay open until the responder returns.
func TestClosureGuardIsNotLostToResponderReads(t *testing.T) {
	loop := &ResponderLoop{}
	loop.operationMu.RLock()
	defer loop.operationMu.RUnlock()

	_, release, state := acquireClosureGuard(loop)
	if state != closureGuardHeld {
		t.Fatalf("a concurrent responder read made the closure pass state %d, want held", state)
	}
	release()
}

func TestClosureGuardIsUnsafeWhileAWriteRuns(t *testing.T) {
	loop := &ResponderLoop{}
	loop.operationMu.Lock()
	defer loop.operationMu.Unlock()
	if _, _, state := acquireClosureGuard(loop); state != closureGuardUnsafe {
		t.Fatalf("a running write left the closure pass state %d, want unsafe", state)
	}
}

// Brief contention on the loop's state lock observed nothing, so it must not
// be reported as unsafe (which restarts the stability window).
func TestClosureGuardContentionIsBusyNotUnsafe(t *testing.T) {
	loop := &ResponderLoop{}
	loop.mu.Lock()
	defer loop.mu.Unlock()
	if _, _, state := acquireClosureGuard(loop); state != closureGuardBusy {
		t.Fatalf("lock contention left the closure pass state %d, want busy", state)
	}
}

func TestClosureGuardWithoutALoopIsHeld(t *testing.T) {
	if _, _, state := acquireClosureGuard(nil); state != closureGuardHeld {
		t.Fatalf("no loop left the closure pass state %d, want held", state)
	}
}
