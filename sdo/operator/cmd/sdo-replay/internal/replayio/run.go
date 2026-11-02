// Package replayio owns the replay command's child-process launcher.
package replayio

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
)

// Run executes the built replay binary with the caller's standard streams and
// returns its exit status.
func Run(binary string, args []string) int {
	command := exec.Command(binary, args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return exit.ExitCode()
		}
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	return 0
}
