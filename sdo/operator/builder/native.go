package builder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config identifies the application to build and the installed operator's Go
// modules. The returned workspace is independent of the application checkout.
type Config struct {
	ApplicationRoot string
	DiagnosticsRoot string
	OperatorRoot    string
	HealthyBaseline string
}

type Workspace struct {
	Directory  string
	ModulePath string
	HasProber  bool
}

// ValidateAndBuild compiles detector tests and the controller binary inside
// the isolated workspace. The returned path is valid until Close is called.
func (workspace *Workspace) ValidateAndBuild(ctx context.Context) (string, error) {
	if workspace == nil || workspace.Directory == "" {
		return "", errors.New("workspace is required")
	}
	if err := seedGoBuildCache(ctx); err != nil {
		return "", err
	}
	binary := filepath.Join(workspace.Directory, "sdo-controller")
	for _, args := range [][]string{{"mod", "tidy"}, {"test", "./..."}, {"build", "-buildvcs=false", "-o", binary, "./cmd/controller"}} {
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = workspace.Directory
		if output, err := command.CombinedOutput(); err != nil {
			return "", fmt.Errorf("go %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
	}
	if workspace.HasProber {
		command := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-o", filepath.Join(workspace.Directory, "sdo-prober"), "./cmd/prober")
		command.Dir = workspace.Directory
		command.Env = append(os.Environ(), "CGO_ENABLED=0")
		if output, err := command.CombinedOutput(); err != nil {
			return "", fmt.Errorf("go build prober: %w: %s", err, strings.TrimSpace(string(output)))
		}
	}
	return binary, nil
}

func seedGoBuildCache(ctx context.Context) error {
	seed := os.Getenv("SDO_GO_CACHE_SEED")
	if seed == "" {
		return nil
	}
	if _, err := os.Stat(seed); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect Go build cache seed: %w", err)
	}
	cache := os.Getenv("GOCACHE")
	if cache == "" {
		return errors.New("GOCACHE is required when SDO_GO_CACHE_SEED is set")
	}
	marker := filepath.Join(cache, ".sdo-seeded")
	if _, err := os.Stat(marker); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect Go build cache: %w", err)
	}
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return fmt.Errorf("create Go build cache: %w", err)
	}
	command := exec.CommandContext(ctx, "cp", "-R", seed+string(os.PathSeparator)+".", cache)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("seed Go build cache: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		return fmt.Errorf("mark seeded Go build cache: %w", err)
	}
	return nil
}

func (workspace *Workspace) Close() error {
	if workspace == nil || workspace.Directory == "" {
		return nil
	}
	return os.RemoveAll(workspace.Directory)
}

type manifest struct {
	APIVersion string     `yaml:"apiVersion"`
	Kind       string     `yaml:"kind"`
	SDKVersion string     `yaml:"sdkVersion"`
	Detectors  []detector `yaml:"detectors"`
}
type detector struct {
	ID          string `yaml:"id"`
	Package     string `yaml:"package"`
	Constructor string `yaml:"constructor"`
	Class       string `yaml:"class"`
	Owner       string `yaml:"owner"`
	Watches     []struct {
		APIVersion string `yaml:"apiVersion"`
		Kind       string `yaml:"kind"`
		Namespace  string `yaml:"namespace"`
	} `yaml:"watches"`
	Interval    string `yaml:"interval"`
	Persistence struct {
		Firing   int `yaml:"firing"`
		Clearing int `yaml:"clearing"`
	} `yaml:"persistence"`
	Batching struct {
		Severity string `yaml:"severity"`
		Debounce string `yaml:"debounce"`
	} `yaml:"batching"`
	PossiblePlaybooks   []string `yaml:"possiblePlaybooks"`
	OriginatingIncident string   `yaml:"originatingIncident"`
	OriginatingCommit   string   `yaml:"originatingCommit"`
}

var modulePattern = regexp.MustCompile(`(?m)^module\s+(\S+)\s*$`)
var detectorIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

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
	data, err := os.ReadFile(filepath.Join(diagnostics, "manifest.yaml"))
	if err != nil {
		return nil, fmt.Errorf("read diagnostics manifest: %w", err)
	}
	var parsed manifest
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("parse diagnostics manifest: %w", err)
	}
	if err := validateManifest(parsed, diagnostics); err != nil {
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
	for index, item := range parsed.Detectors {
		alias := fmt.Sprintf("d%d", index)
		imports = append(imports, fmt.Sprintf("\t%s %q", alias, module+"/"+strings.TrimPrefix(item.Package, "./")))
		constructor := item.Constructor
		if constructor == "" {
			constructor = "New"
		}
		constructors = append(constructors, fmt.Sprintf("\t\t%s.%s(),", alias, constructor))
	}
	source := "package generated\n\nimport (\n" + strings.Join(imports, "\n") + "\n)\n\nfunc All() []sdk.Detector {\n\treturn []sdk.Detector{\n" + strings.Join(constructors, "\n") + "\n\t}\n}\n"
	if err := os.WriteFile(filepath.Join(directory, "generated", "detectors.go"), []byte(source), 0o644); err != nil {
		return nil, err
	}
	if err := generateRegistrationTest(directory, parsed); err != nil {
		return nil, err
	}
	if err := generateExternalNameTest(config.ApplicationRoot, directory); err != nil {
		return nil, err
	}
	if config.HealthyBaseline != "" {
		if err := generateHealthyBaselineTest(config.HealthyBaseline, directory); err != nil {
			return nil, err
		}
	}
	main := fmt.Sprintf("package main\n\nimport (\n\t\"log\"\n\t\"sdo.dev/operator/runtime\"\n\t%q\n)\n\nfunc main() { programs, err := generated.Programs(); if err != nil { log.Fatal(err) }; runtime.RunWithPrograms(generated.All(), programs) }\n", module+"/generated")
	if err := os.WriteFile(filepath.Join(directory, "cmd", "controller", "main.go"), []byte(main), 0o644); err != nil {
		return nil, err
	}
	hasProber, err := generateTraffic(directory, module)
	if err != nil {
		return nil, err
	}
	return &Workspace{Directory: directory, ModulePath: module, HasProber: hasProber}, nil
}

func validateManifest(value manifest, diagnostics string) error {
	if value.APIVersion != "sdo.dev/v1alpha1" || value.Kind != "DetectorManifest" || value.SDKVersion == "" || len(value.Detectors) == 0 {
		return errors.New("invalid diagnostics manifest header or empty detectors")
	}
	ids := map[string]bool{}
	for _, item := range value.Detectors {
		if !detectorIDPattern.MatchString(item.ID) || ids[item.ID] {
			return fmt.Errorf("invalid or duplicate detector id %q", item.ID)
		}
		ids[item.ID] = true
		constructor := item.Constructor
		if constructor == "" {
			constructor = "New"
		}
		if !identifierPattern.MatchString(constructor) {
			return fmt.Errorf("detector %s has invalid constructor", item.ID)
		}
		expectedOwner := "health_judge"
		if item.Class == "incident" {
			expectedOwner = "responder"
			if item.OriginatingIncident == "" {
				return fmt.Errorf("incident detector %s needs originatingIncident", item.ID)
			}
		} else if item.Class != "health" {
			return fmt.Errorf("detector %s has invalid class", item.ID)
		}
		if item.Owner != expectedOwner {
			return fmt.Errorf("detector %s owner must be %s", item.ID, expectedOwner)
		}
		if item.OriginatingCommit == "" {
			return fmt.Errorf("detector %s needs originatingCommit", item.ID)
		}
		if item.PossiblePlaybooks == nil {
			return fmt.Errorf("detector %s needs possiblePlaybooks", item.ID)
		}
		playbooks := map[string]bool{}
		for _, playbook := range item.PossiblePlaybooks {
			if playbooks[playbook] {
				return fmt.Errorf("detector %s has duplicate possiblePlaybooks", item.ID)
			}
			playbooks[playbook] = true
		}
		watches := map[string]bool{}
		for _, watch := range item.Watches {
			if watch.APIVersion == "" || watch.Kind == "" {
				return fmt.Errorf("detector %s has invalid watch", item.ID)
			}
			key := watch.APIVersion + "\x00" + watch.Kind + "\x00" + watch.Namespace
			if watches[key] {
				return fmt.Errorf("detector %s has duplicate watches", item.ID)
			}
			watches[key] = true
		}
		if duration, err := time.ParseDuration(item.Interval); err != nil || duration <= 0 {
			return fmt.Errorf("detector %s has invalid interval", item.ID)
		}
		if item.Persistence.Firing < 1 || item.Persistence.Clearing < 1 ||
			item.Batching.Severity != "info" && item.Batching.Severity != "warn" && item.Batching.Severity != "critical" {
			return fmt.Errorf("detector %s has invalid persistence or batching", item.ID)
		}
		if item.Batching.Debounce != "0" {
			if duration, err := time.ParseDuration(item.Batching.Debounce); err != nil || duration <= 0 {
				return fmt.Errorf("detector %s has invalid debounce", item.ID)
			}
		}
		packagePath := filepath.Clean(strings.TrimPrefix(item.Package, "./"))
		if item.Package == "" || filepath.IsAbs(packagePath) || packagePath == ".." || strings.HasPrefix(packagePath, ".."+string(filepath.Separator)) {
			return fmt.Errorf("detector %s package escapes diagnostics", item.ID)
		}
		path := filepath.Join(diagnostics, packagePath)
		files, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("detector %s package: %w", item.ID, err)
		}
		hasGo := false
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".go") {
				hasGo = true
			}
		}
		if !hasGo {
			return fmt.Errorf("detector %s package has no Go files", item.ID)
		}
	}
	return nil
}

func containsFiles(root string) bool {
	found := false
	_ = filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			found = true
		}
		return nil
	})
	return found
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
