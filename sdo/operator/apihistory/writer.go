package apihistory

import (
	"os"
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
