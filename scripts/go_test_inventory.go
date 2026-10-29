//go:build ignore

// Command go_test_inventory reads Go's generated test mains after compiling
// their archives, avoiding a discarded link of every test executable.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type listedPackage struct {
	ImportPath string
	Name       string
	Dir        string
	GoFiles    []string
}

type event struct {
	Action  string
	Package string
	Output  string
}

func inventory(directory string, output io.Writer) error {
	command := exec.Command("go", "list", "-race", "-test", "-export", "-json", "./...")
	command.Dir = directory
	command.Stderr = os.Stderr
	data, err := command.Output()
	if err != nil {
		return fmt.Errorf("compile test archives in %s: %w", directory, err)
	}
	// go test -list also vets the selected test packages. Reuse the exported
	// archives while preserving that rejection boundary, without any links.
	vet := exec.Command("go", "vet", "-race", "./...")
	vet.Dir = directory
	vet.Stdout, vet.Stderr = os.Stderr, os.Stderr
	if err := vet.Run(); err != nil {
		return fmt.Errorf("vet compiled test archives in %s: %w", directory, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	encoder := json.NewEncoder(output)
	mains := 0
	for {
		var selected listedPackage
		if err := decoder.Decode(&selected); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode compiled package inventory: %w", err)
		}
		if selected.Name != "main" || !strings.HasSuffix(selected.ImportPath, ".test") {
			continue
		}
		mains++
		found := 0
		for _, file := range selected.GoFiles {
			if !filepath.IsAbs(file) {
				file = filepath.Join(selected.Dir, file)
			}
			source, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
			if err != nil {
				return fmt.Errorf("parse generated test main %s: %w", file, err)
			}
			for _, declaration := range source.Decls {
				variables, ok := declaration.(*ast.GenDecl)
				if !ok || variables.Tok != token.VAR {
					continue
				}
				for _, spec := range variables.Specs {
					value, ok := spec.(*ast.ValueSpec)
					if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
						continue
					}
					switch value.Names[0].Name {
					case "tests", "fuzzTargets", "examples":
					default:
						continue
					}
					found++
					list, ok := value.Values[0].(*ast.CompositeLit)
					if !ok {
						return fmt.Errorf("%s: generated %s is not a test table", file, value.Names[0].Name)
					}
					for _, item := range list.Elts {
						entry, ok := item.(*ast.CompositeLit)
						if !ok || len(entry.Elts) == 0 {
							return fmt.Errorf("%s: malformed generated test entry", file)
						}
						literal, ok := entry.Elts[0].(*ast.BasicLit)
						if !ok || literal.Kind != token.STRING {
							return fmt.Errorf("%s: generated test name is not a string", file)
						}
						name, err := strconv.Unquote(literal.Value)
						if err != nil {
							return fmt.Errorf("%s: invalid generated test name: %w", file, err)
						}
						if err := encoder.Encode(event{"output", strings.TrimSuffix(selected.ImportPath, ".test"), name + "\n"}); err != nil {
							return err
						}
					}
				}
			}
		}
		if found != 3 {
			return fmt.Errorf("%s: expected Go's three generated test tables, found %d", selected.ImportPath, found)
		}
	}
	if mains == 0 {
		return fmt.Errorf("%s: no generated test mains in compiled inventory", directory)
	}
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go_test_inventory MODULE_DIRECTORY")
		os.Exit(2)
	}
	if err := inventory(os.Args[1], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
