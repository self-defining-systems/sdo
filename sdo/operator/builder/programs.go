package builder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	programvalidation "sdo.dev/operator/memory/validate/program"
	"strings"
)

func stagePrograms(ctx context.Context, config Config, workspace, module string) error {
	root := filepath.Join(config.ApplicationRoot, ".sdo")
	for _, part := range []string{"playbooks", "lib", "corpus"} {
		source := filepath.Join(root, part)
		if _, err := os.Stat(source); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		if err := copyTree(ctx, source, filepath.Join(workspace, part)); err != nil {
			return err
		}
	}
	// Keep retired READMEs so immutable corpus labels remain resolvable, but
	// exclude executable files before any ./... compilation or replay.
	entries, err := os.ReadDir(filepath.Join(workspace, "playbooks"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		base := filepath.Join(workspace, "playbooks", entry.Name())
		if _, err := os.Stat(filepath.Join(base, layout.PlaybookFileName)); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		retired, err := layout.ReadPlaybookRetired(base)
		if err != nil {
			return fmt.Errorf("playbook %s: %w", entry.Name(), err)
		}
		if !retired {
			continue
		}
		files, err := os.ReadDir(base)
		if err != nil {
			return err
		}
		for _, file := range files {
			if file.Name() != layout.PlaybookFileName {
				if err := os.RemoveAll(filepath.Join(base, file.Name())); err != nil {
					return err
				}
			}
		}
	}
	inspect := programvalidation.Inspect
	adopted, err := memory.AdoptedBuild(ctx, config.ApplicationRoot, "HEAD")
	if err != nil {
		return err
	}
	if config.CompileOnly || adopted {
		inspect = programvalidation.InspectForAdoption
	}
	programs, err := inspect(workspace, module)
	if err != nil {
		return err
	}
	if err := generatePrograms(workspace, module, programs); err != nil {
		return err
	}
	return nil
}

func generatePrograms(root, module string, programs []programvalidation.Spec) error {
	var source strings.Builder
	source.WriteString("package generated\n\nimport (\n\t\"sdo.dev/operator/sdk/program\"\n")
	for index, item := range programs {
		fmt.Fprintf(&source, "\tp%d %q\n", index, module+"/playbooks/"+item.ID)
	}
	source.WriteString(")\n\nfunc Programs() (program.ProgramRegistry, error) {\n\treturn program.NewRegistry(\n\t\t[]program.Diagnoser{")
	for index, item := range programs {
		fmt.Fprintf(&source, "new(p%d.%s),", index, item.Diagnoser)
	}
	source.WriteString("},\n\t\t[]program.Mitigator{")
	for index, item := range programs {
		if item.Mitigator != "" {
			fmt.Fprintf(&source, "new(p%d.%s),", index, item.Mitigator)
		}
	}
	source.WriteString("},\n\t)\n}\n")
	if err := os.WriteFile(filepath.Join(root, "generated", "programs.go"), []byte(source.String()), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, "generated", "programs_test.go"), []byte(programvalidation.ReplayTests(programs)), 0o644); err != nil {
		return err
	}
	// Declaration validation is structural policy, independent of replay results
	// and available corpus cases. Never make it an informational replay note.
	declarations := `package generated
import "testing"
func TestProgramRegistrationContracts(t *testing.T) {
 if _, err := Programs(); err != nil { t.Fatal(err) }
}
`
	if err := os.WriteFile(filepath.Join(root, "generated", "program_declarations_test.go"), []byte(declarations), 0o644); err != nil {
		return err
	}
	return nil
}
