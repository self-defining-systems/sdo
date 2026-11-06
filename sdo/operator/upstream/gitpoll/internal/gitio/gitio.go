// Package gitio is the gitpoll process edge: it runs one git command to
// completion with a non-interactive, protocol-restricted environment. No
// process handle escapes.
package gitio

import (
	"bytes"
	"context"
	"os"
	"os/exec"
)

// AllowedProtocols are the transports a remote may use (file is permitted: the
// toy and a local path in sources.yaml use it); it excludes ext:: and
// every other transport that runs commands.
const AllowedProtocols = "file:git:http:https"

// IntegrityConfig makes every git command check object integrity on transfer.
var IntegrityConfig = []string{"-c", "transfer.fsckObjects=true", "-c", "fetch.fsckObjects=true"}

// Run executes git with args (prefixed by --git-dir when gitDir is set) and
// returns stdout and stderr.
func Run(ctx context.Context, gitDir string, args ...string) (stdout, stderr string, err error) {
	// The remote is unauthenticated: reject malformed or malicious objects
	// at fetch time instead of storing them in the accepted repository.
	args = append(append([]string(nil), IntegrityConfig...), args...)
	if gitDir != "" {
		args = append([]string{"--git-dir", gitDir}, args...)
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL="+AllowedProtocols, "LC_ALL=C")
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err = cmd.Run()
	return out.String(), errb.String(), err
}
