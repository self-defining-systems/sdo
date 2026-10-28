// Command linkcache is a `go -toolexec` wrapper that serves repeated links
// from a content-addressed cache. Every other tool runs unchanged.
//
// Go caches compiled packages but never linked binaries, and validating one
// detector workspace links the controller and several test binaries. Tests
// validate the same few workspaces hundreds of times, so identical links are
// the shared cost. The key covers every linker input: its arguments, the
// importcfg, the main archive and every archive the importcfg names, each
// identified by its stamped build ID (a content hash if unstamped). Without
// SDO_LINK_CACHE, or on
// any cache error, the real linker runs; the cache can only skip equal work.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: linkcache TOOL [ARGS...]")
		os.Exit(2)
	}
	tool, args := os.Args[1], os.Args[2:]
	dir := os.Getenv("SDO_LINK_CACHE")
	if dir != "" && filepath.Base(tool) == "link" && !isVersionProbe(args) {
		if out, key, ok := linkKey(tool, args); ok {
			if serve(filepath.Join(dir, key), out) {
				return
			}
			code := run(tool, args)
			if code == 0 {
				store(dir, key, out)
			}
			os.Exit(code)
		}
	}
	os.Exit(run(tool, args))
}

func isVersionProbe(args []string) bool {
	for _, arg := range args {
		if arg == "-V" || strings.HasPrefix(arg, "-V=") {
			return true
		}
	}
	return false
}

func run(tool string, args []string) int {
	command := exec.Command(tool, args...)
	command.Stdin, command.Stdout, command.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := command.Run(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			return exit.ExitCode()
		}
		fmt.Fprintln(os.Stderr, "linkcache:", err)
		return 1
	}
	return 0
}

// linkKey hashes everything the linker reads. ok is false when the inputs
// cannot be fully accounted for, in which case the link is not cached.
func linkKey(tool string, args []string) (out, key string, ok bool) {
	h := sha256.New()
	field := func(kind, value string) { fmt.Fprintf(h, "%s\x00%d\x00%s\x00", kind, len(value), value) }
	info, err := os.Stat(tool)
	if err != nil {
		return "", "", false
	}
	field("tool", fmt.Sprintf("%s %d %d", tool, info.Size(), info.ModTime().UnixNano()))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-o" {
			if i+1 >= len(args) {
				return "", "", false
			}
			i++
			out = args[i]
			continue
		}
		if arg == "-importcfg" && i+1 < len(args) {
			i++
			if !hashImportConfig(field, args[i]) {
				return "", "", false
			}
			continue
		}
		if !strings.HasPrefix(arg, "-") {
			if info, err := os.Stat(arg); err == nil && info.Mode().IsRegular() {
				sum, err := fileIdentity(arg)
				if err != nil {
					return "", "", false
				}
				field("file", sum)
				continue
			}
		}
		field("arg", arg)
	}
	if out == "" {
		return "", "", false
	}
	return out, hex.EncodeToString(h.Sum(nil)), true
}

func hashImportConfig(field func(kind, value string), path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		directive, rest, found := strings.Cut(line, " ")
		if !found || directive != "packagefile" {
			field("importcfg", line)
			continue
		}
		name, file, found := strings.Cut(rest, "=")
		if !found {
			return false
		}
		sum, err := fileIdentity(file)
		if err != nil {
			return false
		}
		field("packagefile", name+"="+sum)
	}
	return true
}

// buildIDHeader finds the build ID Go stamps in the first bytes of an archive.
var buildIDHeader = regexp.MustCompile(`build id "([^"\n]+)"`)

// fileIdentity names a file by what Go itself trusts. The same archive read
// from a temporary directory just after compilation and from Go's cache later
// has the same stamped build ID, whose content half identifies the bytes; so
// the identity is location independent. Files without a stamp are hashed.
func fileIdentity(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	header := make([]byte, 4096)
	n, _ := io.ReadFull(file, header)
	if match := buildIDHeader.FindSubmatch(header[:n]); match != nil {
		return "buildid:" + string(match[1]), nil
	}
	return fileSum(path)
}

func fileSum(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func serve(cached, out string) bool {
	if err := copyFile(cached, out); err != nil {
		_ = os.Remove(out)
		return false
	}
	return true
}

func store(dir, key, out string) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	temporary, err := os.CreateTemp(dir, ".link-*")
	if err != nil {
		return
	}
	temporary.Close()
	if copyFile(out, temporary.Name()) != nil || os.Rename(temporary.Name(), filepath.Join(dir, key)) != nil {
		_ = os.Remove(temporary.Name())
	}
}

// copyFile copies content and mode. A copy, never a link: Go rewrites the
// build ID inside the linked output in place.
func copyFile(from, to string) error {
	source, err := os.Open(from)
	if err != nil {
		return err
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return err
	}
	target, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(target, source); err != nil {
		target.Close()
		return err
	}
	// The destination may pre-exist with another mode.
	if err := target.Chmod(info.Mode().Perm()); err != nil {
		target.Close()
		return err
	}
	return target.Close()
}
