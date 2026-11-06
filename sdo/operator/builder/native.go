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
	"sdo.dev/operator/contracts/settings"
	"strings"
	"time"

	"sdo.dev/operator/memory/layout"
)

// Config identifies the application to build and the installed operator's Go
// modules. The returned workspace is independent of the application checkout.
type Config struct {
	// Replay is trusted run configuration: empty/on enables corpus and replay.
	Replay          string
	ApplicationRoot string
	DiagnosticsRoot string
	OperatorRoot    string
	HealthyBaseline string
	// ProgramModule pins JIT imports to the accepted diagnostics module name.
	// It never compiles checkout detector code.
	ProgramModule string
	// CompileOnly is reserved for an explicit, unvalidated adoption build.
	CompileOnly bool
}

var advisoryDuration = regexp.MustCompile(`(?m)(?:\t(?:[0-9]+\.[0-9]+s|\(cached\))| \([0-9]+\.[0-9]+s\))$`)

type Workspace struct {
	Directory     string
	ModulePath    string
	compileOnly   bool
	detectorCount int
	// StageTimings lists each Go toolchain stage run by Check or
	// ValidateAndBuild, including a rejected final stage.
	StageTimings        []StageTiming
	advisoryTestErrors  []string
	advisorySetupErrors []string
	// generatedClassified records that the generated package's tests already
	// ran once, classified per test, so the informational pass skips it.
	generatedClassified bool
	corpusAdvisory      string
	// healthyBaseline records that the healthy-baseline replay was generated.
	healthyBaseline bool
}

// StageTiming is the wall-clock cost of one Go toolchain stage.
type StageTiming struct {
	Stage    string
	Duration time.Duration
}

type StageError = buildio.StageError

// lean returns a Go build that links without symbol tables or DWARF. Linking a
// controller is the dominant fixed cost of every validation, it is never
// cached, and DWARF about doubles it. Only builds whose binary is discarded or
// executed locally use it; the controller image binary keeps its debug data.
// Test commands do not: go test skips the link entirely on a cached result,
// and any -ldflags forces it.
func lean(args ...string) []string {
	return append([]string{args[0], "-ldflags=-s -w"}, args[1:]...)
}

func (workspace *Workspace) runGo(ctx context.Context, args ...string) error {
	elapsed, err := buildio.RunGo(ctx, workspace.Directory, nil, args...)
	workspace.StageTimings = append(workspace.StageTimings, StageTiming{Stage: strings.Join(args, " "), Duration: elapsed})
	return err
}

// ValidateAndBuild compiles detector tests and the controller binary inside
// the isolated workspace. The returned path is valid until Close is called.
func (workspace *Workspace) ValidateAndBuild(ctx context.Context) (string, error) {
	binary, err := workspace.validate(ctx, false, !workspace.compileOnly)
	if err != nil {
		return "", err
	}
	if !workspace.compileOnly {
		if err := workspace.collectAdvisories(ctx); err != nil {
			return "", err
		}
	}
	return binary, nil
}

// BuildForLaunch retains every launch gate and emits the controller/prober
// artifacts. It omits informational tests already available from self-checks.
func (workspace *Workspace) BuildForLaunch(ctx context.Context) (string, error) {
	return workspace.validate(ctx, false, false)
}

// Check compiles and vets every package and enforces detector acceptance.
// Playbook tests and replay are informational. Check links no controller or
// memory-host artifact; BuildMemoryHost produces the accepted host.
// A rejected stage is reported as a *StageError.
func (workspace *Workspace) Check(ctx context.Context) error {
	if err := workspace.acceptance(ctx, true); err != nil {
		return err
	}
	return workspace.collectAdvisories(ctx)
}

// CheckAcceptance runs every required acceptance gate without the optional
// behavioral replay pass. Publication must not wait for advisory tests that
// cannot affect acceptance. Check additionally collects those notes for callers
// explicitly requesting the informational self-check.
func (workspace *Workspace) CheckAcceptance(ctx context.Context) error {
	return workspace.acceptance(ctx, false)
}

func (workspace *Workspace) acceptance(ctx context.Context, classifyGenerated bool) error {
	if workspace != nil && workspace.compileOnly {
		return errors.New("unvalidated adoption workspace cannot run the acceptance checker")
	}
	if err := workspace.prepareMemoryHost(); err != nil {
		return err
	}
	_, err := workspace.validate(ctx, true, classifyGenerated)
	return err
}

// validate runs the acceptance stages. With classifyGenerated the generated
// package's tests run once, including the advisory program corpus, and the
// informational pass later skips that package; otherwise the corpus is skipped.
func (workspace *Workspace) validate(ctx context.Context, vet, classifyGenerated bool) (string, error) {
	if workspace == nil || workspace.Directory == "" {
		return "", errors.New("workspace is required")
	}
	if err := seedGoBuildCache(ctx); err != nil {
		return "", err
	}
	binary := filepath.Join(workspace.Directory, "sdo-controller")
	stages := [][]string{{"mod", "tidy"}}
	if vet {
		stages = append(stages, lean("build", "-buildvcs=false", "./..."), []string{"vet", "./..."})
	}
	if workspace.compileOnly {
		stages = append(stages, lean("build", "-buildvcs=false", "./..."))
	} else {
		// Vet already compiles source and tests. Launch-only builds still
		// compile tests without executing their behavioral assertions.
		if !vet {
			stages = append(stages, []string{"test", "-exec=true", "-run", "^$", "./..."})
		}
		// Detector and generated safety contracts remain acceptance gates. Only
		// authored playbook tests and the program corpus replay are advisory.
		args := []string{"test"}
		if workspace.detectorCount > 0 {
			args = append(args, "./detectors/...")
		}
		if _, err := os.Stat(filepath.Join(workspace.Directory, "traffic", "generators")); err == nil {
			args = append(args, "./traffic/...")
		}
		if len(args) > 1 {
			stages = append(stages, args)
		}
		if !classifyGenerated {
			stages = append(stages, []string{"test", "-skip", separateStages, "./generated"})
		}
	}
	for _, args := range stages {
		if err := workspace.runGo(ctx, args...); err != nil {
			return "", err
		}
	}
	if classifyGenerated && !workspace.compileOnly {
		if err := workspace.runGeneratedTests(ctx); err != nil {
			return "", err
		}
	}
	if !workspace.compileOnly {
		if err := workspace.runHealthyBaseline(ctx); err != nil {
			return "", err
		}
	}
	if !vet {
		if err := workspace.runGo(ctx, "build", "-buildvcs=false", "-o", binary, "./cmd/controller"); err != nil {
			return "", err
		}
	}
	return binary, nil
}

func (workspace *Workspace) collectAdvisories(ctx context.Context) error {
	var err error
	if patterns := workspace.informationalPackages(); len(patterns) > 0 {
		err = workspace.runGo(ctx, append([]string{"test", "-timeout=30s"}, patterns...)...)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	workspace.advisoryTestErrors = nil
	if workspace.corpusAdvisory != "" {
		workspace.advisoryTestErrors = append(workspace.advisoryTestErrors, workspace.advisoryText(workspace.corpusAdvisory))
	}
	if err != nil {
		message := err.Error()
		var failed *StageError
		if errors.As(err, &failed) {
			message = failed.Output
		}
		workspace.advisoryTestErrors = append(workspace.advisoryTestErrors, workspace.advisoryText(message))
	}
	return nil
}

func (workspace *Workspace) advisoryText(message string) string {
	message = strings.ReplaceAll(message, workspace.Directory, "<workspace>")
	message = advisoryDuration.ReplaceAllString(message, "")
	if len(message) > 4096 {
		message = message[:4096] + " (truncated)"
	}
	return message
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
// stable controller and accepted memory host in an isolated workspace.
func BuildWorkspace(ctx context.Context, config Config) (_ *Workspace, err error) {
	if _, err := settings.Replay(config.Replay); err != nil {
		return nil, err
	}
	if config.Replay == "off" {
		config.HealthyBaseline = ""
	}
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
	if _, err := os.Lstat(diagnostics); err == nil {
		if err := copyTree(ctx, diagnostics, directory); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
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
	workspaceGoMod, err := workspaceModule(string(goMod), module, operatorRoot)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(goModPath, []byte(workspaceGoMod), 0o644); err != nil {
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
	if err := generateControllerStartTest(directory); err != nil {
		return nil, err
	}
	var setupErrors []string
	healthyBaseline := false
	if config.HealthyBaseline != "" {
		if err := generateHealthyBaselineTest(config.HealthyBaseline, directory); err != nil {
			setupErrors = append(setupErrors, "healthy replay unverified: "+strings.ReplaceAll(err.Error(), config.HealthyBaseline, "<healthy input>"))
		} else {
			healthyBaseline = true
		}
	}
	main := "package main\nimport (\n\t\"sdo.dev/operator/operatorcli/catalog/renderers\"\n\t\"sdo.dev/operator/operatorcli/catalog/telemetrysources\"\n\t\"sdo.dev/operator/runtime\"\n)\nfunc main(){runtime.Run(renderers.Catalog(), telemetrysources.Registry())}\n"
	if err := os.WriteFile(filepath.Join(directory, "cmd", "controller", "main.go"), []byte(main), 0o644); err != nil {
		return nil, err
	}
	_, err = generateTraffic(directory, module)
	if err != nil {
		return nil, err
	}
	if err := generateApplicationMapTest(config.ApplicationRoot, directory); err != nil {
		return nil, err
	}
	return &Workspace{Directory: directory, ModulePath: module, compileOnly: config.CompileOnly, detectorCount: len(detectors), advisorySetupErrors: setupErrors, healthyBaseline: healthyBaseline}, nil
}

func validateDetectors(detectors []layout.Detector) error {
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

func workspaceModule(source, module, operatorRoot string) (string, error) {
	dependencies, err := buildio.OperatorModules(operatorRoot)
	if err != nil {
		return "", err
	}
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
	for _, dependency := range dependencies {
		path := "sdo.dev/operator/" + dependency
		if !moduleRequired(content, path) {
			content += "\nrequire " + path + " v0.0.0"
		}
	}
	if !moduleRequired(content, "sdo.dev/operator") {
		content += "\nrequire sdo.dev/operator v0.0.0"
	}
	for _, dependency := range dependencies {
		content += "\nreplace sdo.dev/operator/" + dependency + " => " + filepath.Join(operatorRoot, dependency)
	}
	content += "\nreplace sdo.dev/operator => " + operatorRoot + "\n"
	return content, nil
}

func moduleRequired(content, path string) bool {
	pattern := regexp.MustCompile(`(?m)^\s*(?:require\s+)?` + regexp.QuoteMeta(path) + `\s+\S+`)
	return pattern.MatchString(content)
}

// DefaultOperatorRoot resolves the installed toolchain modules at the process edge.
func DefaultOperatorRoot() string { return buildio.DefaultOperatorRoot() }
