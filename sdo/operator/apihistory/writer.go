package apihistory

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"syscall"
)

// BeginWriter holds generation liveness independently of recording writes. The
// proxy keeps the returned directory descriptor open for its entire lifetime,
// including failed recorder initialization. Readers on read-only mounts use the
// same inode lock and never accept an old closed manifest while it is held.
func BeginWriter(directory string) (*os.File, error) {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	f, err := os.Open(directory)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	if err := BeginGeneration(directory); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
func writerPresent(directory string) bool {
	f, err := os.Open(directory)
	if err != nil {
		return true
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		return true
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}

// GenerationPath is a separate session-owned boundary, outside recorder storage.
// Kubernetes mounts this file separately so it stays visible on read-only readers.
func GenerationPath(directory string) string { return filepath.Clean(directory) + ".generation" }

// BeginGeneration durably invalidates prior manifests before a forwarding
// attempt. The controller calls this before launching a Job, so an unwritable
// recorder (or a failed sidecar lease) cannot restore an old complete history.
// Failure at this trusted launch boundary must prevent launching that attempt.
func BeginGeneration(directory string) error {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return err
	}
	f, err := os.OpenFile(GenerationPath(directory), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(hex.EncodeToString(token)); err != nil {
		return err
	}
	return f.Sync()
}

func generation(directory string) (string, error) {
	b, err := os.ReadFile(GenerationPath(directory))
	if err != nil {
		return "", err
	}
	if len(b) != 64 {
		return "", os.ErrInvalid
	}
	if _, err := hex.DecodeString(string(b)); err != nil {
		return "", err
	}
	return string(b), nil
}
