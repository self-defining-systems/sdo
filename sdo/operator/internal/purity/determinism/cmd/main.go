// Command determinism scans all first-party Go source regardless of build tags.
package main

import (
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/internal/purity/determinism"
)

func main() {
	root := flag.String("root", "../..", "repository root")
	baseline := flag.String("baseline", "", "committed base allowlist")
	update := flag.Bool("update", false, "remove fixed allowances (never add)")
	bootstrap := flag.Bool("bootstrap", false, "print initial inventory; does not write allowlist")
	flag.Parse()
	if err := run(*root, *baseline, *update, *bootstrap); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(root, baseline string, update, bootstrap bool) error {
	var actual []string
	for _, scope := range []string{"sdo/operator", "benchmarks"} {
		err := filepath.WalkDir(filepath.Join(root, scope), func(filename string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == ".git" || entry.Name() == "vendor" || entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(filename, ".go") {
				return nil
			}
			relative, err := filepath.Rel(root, filename)
			if err != nil {
				return err
			}
			file, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.ParseComments)
			if err != nil {
				return err
			}
			violations, err := determinism.Inspect(filepath.ToSlash(relative), file)
			actual = append(actual, violations...)
			return err
		})
		if err != nil {
			return err
		}
	}
	sort.Strings(actual)
	if bootstrap {
		fmt.Println(strings.Join(actual, "\n"))
		return nil
	}
	filename := filepath.Join(root, "scripts/go_determinism_allowlist.txt")
	allowed, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	committed, err := os.ReadFile(baseline)
	if err != nil {
		return err
	}
	lines := func(data []byte) []string { return strings.Split(strings.TrimSpace(string(data)), "\n") }
	if update {
		if err := determinism.Compare(actual, actual, lines(committed)); err != nil {
			return err
		}
		return os.WriteFile(filename, []byte(strings.Join(actual, "\n")+"\n"), 0644)
	}
	if err := determinism.Compare(actual, lines(allowed), lines(committed)); err != nil {
		return fmt.Errorf("%w\nInject clocks/RNGs/transports/launchers, or use an integration-tagged test. Remove fixed entries with scripts/check_go_determinism.sh --update", err)
	}
	fmt.Printf("Go determinism ratchet: %d allowances; no growth or stale entries\n", len(actual))
	return nil
}
