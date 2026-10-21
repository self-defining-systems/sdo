package validate

import (
	"bytes"
	"context"
	"fmt"
	"go/format"
	"log"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory/layout"
	programvalidation "sdo.dev/operator/memory/validate/program"
	"strings"
)

func (s *run) programRules(ctx context.Context) error {
	// The program policy reads a projection of the physical memory layout.
	// Compile uses this same inspector before generating replay tests.
	moduleData, err := os.ReadFile(filepath.Join(s.req.Root, layout.DiagnosticsGoModFile))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	module := ""
	for _, line := range strings.Split(string(moduleData), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[0] == "module" {
			module = parts[1]
			break
		}
	}
	if _, err := programvalidation.InspectMemory(filepath.Join(s.req.Root, layout.Dir), module); err != nil {
		s.add(Semantic, Diagnostic{Rule: "program.invalid", Path: layout.PlaybooksDir, Message: err.Error()})
	}

	// Preserve branch gofmt enforcement for newly edited sources without rejecting legacy files.
	return filepath.WalkDir(filepath.Join(s.req.Root, layout.Dir), func(abs string, entry os.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(abs, ".go") {
			return nil
		}
		rel, err := filepath.Rel(s.req.Root, abs)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		data, err := os.ReadFile(abs)
		if err != nil {
			return err
		}
		formatted, err := format.Source(data)
		if s.req.Base != "" && !containsPath(s.changed, rel) {
			if err != nil || !bytes.Equal(data, formatted) {
				kind, _, _ := layout.Classify(rel)
				owner := "responder"
				if kind == layout.KindHealthDetectorSource {
					owner = "health_judge"
				}
				log.Printf("warning: pre-existing Go source %s is not gofmt formatted (owner: %s)", rel, owner)
			}
			return nil
		}
		if err == nil && !bytes.Equal(data, formatted) {
			s.add(Semantic, Diagnostic{Rule: "go.format", Path: rel, Message: fmt.Sprintf("Go source %s is not gofmt formatted", rel)})
		}
		return nil
	})
}
