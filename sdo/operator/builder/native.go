package builder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sdo.dev/operator/builder/internal/buildio"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// Config identifies the application to build and the installed operator's Go
// modules. The returned workspace is independent of the application checkout.
type Config struct {
	ApplicationRoot string
	DiagnosticsRoot string
	OperatorRoot    string
	HealthyBaseline string
	MemoryHost      bool // Compile a stable driver with no accepted-memory imports.
}

type Workspace struct {
	Directory  string
	ModulePath string
	HasProber  bool
}

type StageError = buildio.StageError

func (workspace *Workspace) runGo(ctx context.Context, args ...string) error {
	return buildio.RunGo(ctx, workspace.Directory, args...)
}

// ValidateAndBuild compiles detector tests and the controller binary inside
// the isolated workspace. The returned path is valid until Close is called.
func (workspace *Workspace) ValidateAndBuild(ctx context.Context) (string, error) {
	return workspace.validate(ctx, false)
}

// Check is ValidateAndBuild plus a package build and go vet beforehand: the
// full detector acceptance gate.
// A rejected stage is reported as a *StageError.
func (workspace *Workspace) Check(ctx context.Context) error {
	if err := workspace.prepareMemoryHost(); err != nil {
		return err
	}
	_, err := workspace.validate(ctx, true)
	return err
}

func (workspace *Workspace) validate(ctx context.Context, vet bool) (string, error) {
	if workspace == nil || workspace.Directory == "" {
		return "", errors.New("workspace is required")
	}
	if err := seedGoBuildCache(ctx); err != nil {
		return "", err
	}
	target, name := "./cmd/controller", "sdo-controller"
	if vet {
		target, name = "./cmd/memory-host", "sdo-memory-host"
	}
	binary := filepath.Join(workspace.Directory, name)
	stages := [][]string{{"mod", "tidy"}}
	if vet {
		stages = append(stages, []string{"build", "-buildvcs=false", "./..."}, []string{"vet", "./..."})
	}
	stages = append(stages, []string{"test", "./..."}, []string{"build", "-buildvcs=false", "-o", binary, target})
	for _, args := range stages {
		if err := workspace.runGo(ctx, args...); err != nil {
			return "", err
		}
	}
	if workspace.HasProber {
		if err := buildio.BuildProber(ctx, workspace.Directory); err != nil {
			return "", err
		}
	}
	return binary, nil
}

func seedGoBuildCache(ctx context.Context) error { return buildio.SeedGoBuildCache(ctx) }

func (workspace *Workspace) Close() error {
	if workspace == nil || workspace.Directory == "" {
		return nil
	}
	return os.RemoveAll(workspace.Directory)
}

var modulePattern = regexp.MustCompile(`(?m)^module\s+(\S+)\s*$`)

// BuildWorkspace stages an opaque diagnostics projection and generates the
// controller and, when configured, its traffic prober in an isolated workspace.
func BuildWorkspace(ctx context.Context, config Config) (_ *Workspace, err error) {
	if config.ApplicationRoot == "" || config.DiagnosticsRoot == "" || config.OperatorRoot == "" {
		return nil, errors.New("application, diagnostics projection, and operator roots are required")
	}
	operatorRoot, err := filepath.Abs(config.OperatorRoot)
	if err != nil {
		return nil, err
	}
	for _, module := range []string{"sdk", "core", "runtime", "contracts"} {
		if _, err := os.Stat(filepath.Join(operatorRoot, module, "go.mod")); err != nil {
			return nil, fmt.Errorf("operator module %s: %w", module, err)
		}
	}
	diagnostics := config.DiagnosticsRoot
	detectors, err := layout.ReadDetectors(diagnostics)
	if err != nil {
		return nil, fmt.Errorf("read diagnostics: %w", err)
	}
	if err := validateDetectors(detectors); err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp("", "sdo-operator-build-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(directory)
		}
	}()
	if err := copyTree(ctx, diagnostics, directory); err != nil {
		return nil, err
	}
	goModPath := filepath.Join(directory, "go.mod")
	goMod, err := os.ReadFile(goModPath)
	if errors.Is(err, os.ErrNotExist) {
		goMod = []byte("module app-diagnostics\n\ngo 1.26.0\n")
	} else if err != nil {
		return nil, err
	}
	module := "app-diagnostics"
	if match := modulePattern.FindSubmatch(goMod); len(match) == 2 {
		module = string(match[1])
	}
	if err := os.WriteFile(goModPath, []byte(workspaceModule(string(goMod), module, operatorRoot)), 0o644); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(directory, "generated"), 0o755); err != nil {
		return nil, err
	}
	if err := stagePrograms(ctx, config, directory, module); err != nil {
		return nil, fmt.Errorf("validate playbook programs: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(directory, "cmd", "controller"), 0o755); err != nil {
		return nil, err
	}
	imports := []string{"\t\"sdo.dev/operator/sdk\""}
	constructors := []string{}
	for index, item := range detectors {
		alias := fmt.Sprintf("d%d", index)
		imports = append(imports, fmt.Sprintf("\t%s %q", alias, module+"/"+item.Package()))
		constructors = append(constructors, fmt.Sprintf("\t\t%s.%s(),", alias, item.Manifest.ConstructorName()))
	}
	source := "package generated\n\nimport (\n" + strings.Join(imports, "\n") + "\n)\n\nfunc All() []sdk.Detector {\n\treturn []sdk.Detector{\n" + strings.Join(constructors, "\n") + "\n\t}\n}\n"
	if err := os.WriteFile(filepath.Join(directory, "generated", "detectors.go"), []byte(source), 0o644); err != nil {
		return nil, err
	}
	if err := generateRegistrationTest(directory, detectors); err != nil {
		return nil, err
	}
	if err := generateExternalNameTest(config.ApplicationRoot, directory); err != nil {
		return nil, err
	}
	if err := generateMetricsTest(directory); err != nil {
		return nil, err
	}
	if config.HealthyBaseline != "" {
		if err := generateHealthyBaselineTest(config.HealthyBaseline, directory); err != nil {
			return nil, err
		}
	}
	main := fmt.Sprintf("package main\n\nimport (\n\t\"log\"\n\t\"sdo.dev/operator/runtime\"\n\t%q\n)\n\nfunc main() { programs, err := generated.Programs(); if err != nil { log.Fatal(err) }; runtime.RunWithPrograms(generated.All(), programs) }\n", module+"/generated")
	if config.MemoryHost {
		main = "package main\nimport \"sdo.dev/operator/runtime\"\nfunc main(){runtime.RunWithPrograms(nil,nil)}\n"
	}
	if err := os.WriteFile(filepath.Join(directory, "cmd", "controller", "main.go"), []byte(main), 0o644); err != nil {
		return nil, err
	}
	hasProber, err := generateTraffic(directory, module)
	if err != nil {
		return nil, err
	}
	return &Workspace{Directory: directory, ModulePath: module, HasProber: hasProber}, nil
}

func validateDetectors(detectors []layout.Detector) error {
	if len(detectors) == 0 {
		return errors.New("diagnostics register no detectors")
	}
	for _, detector := range detectors {
		hasGo := false
		for _, file := range detector.Files {
			hasGo = hasGo || (!strings.Contains(file.Name, "/") && strings.HasSuffix(file.Name, ".go"))
		}
		if !hasGo {
			return fmt.Errorf("detector %s package has no Go files", detector.Manifest.ID)
		}
	}
	return nil
}

func copyTree(ctx context.Context, source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("diagnostics path must not contain symlinks: %s", path)
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			_ = input.Close()
			return err
		}
		_, copyErr := io.Copy(output, input)
		closeOutputErr := output.Close()
		closeInputErr := input.Close()
		return errors.Join(copyErr, closeOutputErr, closeInputErr)
	})
}

func workspaceModule(source, module, operatorRoot string) string {
	lines := []string{}
	for _, line := range strings.Split(source, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "replace sdo.dev/operator/") || strings.HasPrefix(trimmed, "replace sdo.dev/operator =>") {
			continue
		}
		lines = append(lines, line)
	}
	content := strings.TrimSpace(strings.Join(lines, "\n"))
	if !strings.Contains(content, "module ") {
		content = "module " + module + "\n\n" + content
	}
	for _, dependency := range []string{"sdk", "core", "runtime", "contracts"} {
		path := "sdo.dev/operator/" + dependency
		if !moduleRequired(content, path) {
			content += "\nrequire " + path + " v0.0.0"
		}
	}
	if !moduleRequired(content, "sdo.dev/operator") {
		content += "\nrequire sdo.dev/operator v0.0.0"
	}
	for _, dependency := range []string{"sdk", "core", "runtime", "contracts"} {
		content += "\nreplace sdo.dev/operator/" + dependency + " => " + filepath.Join(operatorRoot, dependency)
	}
	content += "\nreplace sdo.dev/operator => " + operatorRoot + "\n"
	return content
}

func moduleRequired(content, path string) bool {
	pattern := regexp.MustCompile(`(?m)^\s*(?:require\s+)?` + regexp.QuoteMeta(path) + `\s+\S+`)
	return pattern.MatchString(content)
}

// DefaultOperatorRoot resolves the installed toolchain modules at the process edge.
func DefaultOperatorRoot() string { return buildio.DefaultOperatorRoot() }
