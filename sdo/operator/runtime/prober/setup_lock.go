package prober

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// FileSetupLock serializes setup requests across the probe processes of one
// host (an overlapping memory host during a swap, a restart replaying setup)
// with one advisory file lock per request. It only orders the writes: the
// second process then sends its request after the first finished, and the
// application's own answer ("already exists") makes it a no-op. The lock holds
// no state across restarts, so a wiped application is seeded again.
type FileSetupLock struct{ Dir string }

// DefaultSetupLock is the host-wide lock of application namespace.
func DefaultSetupLock(namespace string) *FileSetupLock {
	return &FileSetupLock{Dir: filepath.Join(os.TempDir(), "sdo-setup-locks", namespace)}
}

const setupLockPoll = 10 * time.Millisecond

// Lock waits for the file lock of key or ctx.
func (l *FileSetupLock) Lock(ctx context.Context, key string) (func(), error) {
	if err := os.MkdirAll(l.Dir, 0o755); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(key))
	file, err := os.OpenFile(filepath.Join(l.Dir, hex.EncodeToString(sum[:16])), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN); _ = file.Close() }, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EINTR {
			_ = file.Close()
			return nil, fmt.Errorf("lock setup request: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-time.After(setupLockPoll):
		}
	}
}
