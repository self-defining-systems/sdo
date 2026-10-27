//go:build linux

package worker_registry

import "golang.org/x/sys/unix"

// exchangeDirectories atomically swaps two directories (RENAME_EXCHANGE).
func exchangeDirectories(a, b string) error {
	return unix.Renameat2(unix.AT_FDCWD, a, unix.AT_FDCWD, b, unix.RENAME_EXCHANGE)
}
