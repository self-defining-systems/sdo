// Package pathio reads the process search path for test helpers that put a
// synthetic tool in front of the real ones.
package pathio

import (
	"os"
	"strings"
)

// Prepend returns PATH with directory searched first.
func Prepend(directory string) string {
	return strings.Join([]string{directory, os.Getenv("PATH")}, string(os.PathListSeparator))
}
