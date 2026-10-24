package builder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory"
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
	return operatorReplayTests(workspace, programs)
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
	return os.WriteFile(filepath.Join(root, "generated", "programs_test.go"), []byte(programvalidation.ReplayTests(programs)), 0o644)
}
