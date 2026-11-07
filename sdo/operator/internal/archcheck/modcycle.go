package archcheck

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ModuleCycles reports every dependency cycle among the Go modules under root
// (root's go.mod and every nested go.mod, testdata excluded), by the require
// directives of their go.mod files. A cycle (the root requiring a module that
// requires the root) makes the production module graph include everything the
// test-only dependency pulls in, and the replace directives hide it from go
// itself. Each cycle is one string naming the modules in order.
func ModuleCycles(root string) ([]string, error) {
	requires := map[string][]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == "testdata" || n == "vendor" || n == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() != "go.mod" {
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		module, deps := parseGoMod(string(data))
		if module != "" {
			requires[module] = deps
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Only edges between modules found here can close a cycle.
	graph := map[string][]string{}
	for m, deps := range requires {
		for _, d := range deps {
			if _, ok := requires[d]; ok {
				graph[m] = append(graph[m], d)
			}
		}
		sort.Strings(graph[m])
	}
	var names []string
	for m := range graph {
		names = append(names, m)
	}
	sort.Strings(names)
	const (
		visiting = 1
		done     = 2
	)
	state := map[string]int{}
	var stack, cycles []string
	var visit func(m string)
	visit = func(m string) {
		state[m] = visiting
		stack = append(stack, m)
		for _, d := range graph[m] {
			switch state[d] {
			case 0:
				visit(d)
			case visiting:
				i := len(stack) - 1
				for stack[i] != d {
					i--
				}
				cycles = append(cycles, strings.Join(append(append([]string{}, stack[i:]...), d), " -> "))
			}
		}
		stack = stack[:len(stack)-1]
		state[m] = done
	}
	for _, m := range names {
		if state[m] == 0 {
			visit(m)
		}
	}
	sort.Strings(cycles)
	return cycles, nil
}

// parseGoMod returns the module path and the required module paths of a
// go.mod, reading the single-line and block forms of require.
func parseGoMod(src string) (string, []string) {
	var module string
	var deps []string
	block := ""
	for _, line := range strings.Split(src, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch {
		case block != "":
			if fields[0] == ")" {
				block = ""
			} else if block == "require" {
				deps = append(deps, fields[0])
			}
		case fields[0] == "module" && len(fields) > 1:
			module = strings.Trim(fields[1], `"`)
		case len(fields) == 2 && fields[1] == "(":
			block = fields[0]
		case fields[0] == "require" && len(fields) >= 3:
			deps = append(deps, fields[1])
		}
	}
	return module, deps
}

func (c Config) moduleCycleViolations(root string) ([]Violation, error) {
	cycles, err := ModuleCycles(root)
	if err != nil {
		return nil, fmt.Errorf("archcheck: %w", err)
	}
	var out []Violation
	for _, cycle := range cycles {
		out = append(out, Violation{Rule: R7, File: "go.mod", Message: "go.mod require cycle " + cycle + "; a module may not require one that requires it (tests that need both belong in sdo/operator/integration)"})
	}
	return out, nil
}
