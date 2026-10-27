//go:build !linux

package worker_registry

import "errors"

// exchangeDirectories is unsupported off Linux, so warm adoption stays disabled.
func exchangeDirectories(a, b string) error {
	return errors.New("RENAME_EXCHANGE requires Linux")
}
