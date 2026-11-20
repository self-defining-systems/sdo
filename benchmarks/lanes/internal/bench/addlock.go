package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// addLockWait is how long an add waits for another add in the same root before it gives up with a named error.
var addLockWait = 2 * time.Minute

// lockAdds takes the root's add lock: `add` and `plan add` check that a name is free, store inputs and write the
// lock and spec files, which two at once would interleave (both pass the check, share a temporary file, or one's
// cleanup removes the other's directory). The lock is a flock, released when the process ends, killed or not.
func lockAdds(root Root) (release func(), err error) {
	if err := os.MkdirAll(string(root), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(string(root), ".add.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(addLockWait)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
		}
		if time.Now().After(deadline) {
			f.Close()
			return nil, fmt.Errorf("another add holds %s for more than %s", f.Name(), addLockWait)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
