package builder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	programvalidation "sdo.dev/operator/memory/validate/program"
)

// BuildWorkingProgram compiles one checkout playbook using the normal import
// policy, module setup and registry generator. It does not accept memory: the
// broker still requires the full independent corpus/replay check on submission.
func BuildWorkingProgram(ctx context.Context, config Config, id string) (_ *Workspace, _ string, err error) {
	if !regexp.MustCompile("^[a-z0-9][a-z0-9-]*$").MatchString(id) {
		return nil, "", fmt.Errorf("invalid playbook ID")
	}
	operator, err := filepath.Abs(config.OperatorRoot)
	if err != nil {
		return nil, "", err
	}
	directory, err := os.MkdirTemp("", "sdo-program-build-")
	if err != nil {
		return nil, "", err
	}
	workspace := &Workspace{Directory: directory}
	defer func() {
		if err != nil {
			_ = workspace.Close()
		}
	}()
	for _, part := range []string{"playbooks/" + id, "lib"} {
		source := filepath.Join(config.ApplicationRoot, ".sdo", part)
		if _, e := os.Stat(source); os.IsNotExist(e) && part == "lib" {
			continue
		}
		if err = copyTree(ctx, source, filepath.Join(directory, part)); err != nil {
			return nil, "", err
		}
	}
	module := config.ProgramModule
	if module == "" {
		source, readErr := os.ReadFile(filepath.Join(config.ApplicationRoot, ".sdo/diagnostics/go.mod"))
		if readErr != nil && !os.IsNotExist(readErr) {
			return nil, "", readErr
		}
		module = "app-diagnostics"
		if match := modulePattern.FindSubmatch(source); len(match) == 2 {
			module = string(match[1])
		}
	}
	source := []byte("module " + module + "\n\ngo 1.26.0\n")
	workspace.ModulePath = module
	if err = os.WriteFile(filepath.Join(directory, "go.mod"), []byte(workspaceModule(string(source), module, operator)), 0644); err != nil {
		return nil, "", err
	}
	specs, err := programvalidation.InspectForAdoption(directory, module)
	if err != nil {
		return nil, "", err
	}
	if len(specs) != 1 || specs[0].ID != id {
		return nil, "", fmt.Errorf("playbook %q is retired or has no Go program", id)
	}
	if err = os.MkdirAll(filepath.Join(directory, "generated"), 0755); err != nil {
		return nil, "", err
	}
	if err = generatePrograms(directory, module, specs); err != nil {
		return nil, "", err
	}
	main := fmt.Sprintf("package main\nimport(\"log\";\"sdo.dev/operator/runtime\";%q)\nfunc main(){r,e:=generated.Programs();if e!=nil{log.Fatal(e)};runtime.ServeWorkingProgram(r)}\n", module+"/generated")
	if err = os.MkdirAll(filepath.Join(directory, "cmd/program"), 0755); err != nil {
		return nil, "", err
	}
	if err = os.WriteFile(filepath.Join(directory, "cmd/program/main.go"), []byte(main), 0644); err != nil {
		return nil, "", err
	}
	if err = seedGoBuildCache(ctx); err != nil {
		return nil, "", err
	}
	binary := filepath.Join(directory, "program")
	for _, args := range [][]string{{"mod", "tidy"}, {"build", "-buildvcs=false", "-o", binary, "./cmd/program"}} {
		started := time.Now()
		err = workspace.runGo(ctx, args...)
		workspace.StageTimings = append(workspace.StageTimings, StageTiming{Stage: fmt.Sprint(args), Duration: time.Since(started)})
		if err != nil {
			return nil, "", err
		}
	}
	return workspace, binary, nil
}
