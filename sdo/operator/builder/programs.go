package builder

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/sdktest"
)

type compiledProgram struct {
	id, diagnoser, mitigator string
	cases                    []string
}

var programID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
var shellLine = regexp.MustCompile(`(?i)^\s*(kubectl|curl|bash)(\s|$)`)
var shellFence = regexp.MustCompile(`(?i)^(?:` + "```" + `|~~~)[ \t]*(bash|sh|zsh|shell|console|terminal|powershell)(\s|$)`)
var incidentPort = regexp.MustCompile(`\b[0-9]{1,5}\b`)
var incidentImage = regexp.MustCompile(`(?i)\bimage\s*(?:=|:|\s)\s*["']?([a-z0-9./:_-]+)`)

var ignoredIncidentLiterals = map[string]bool{
	"service": true, "deployment": true, "pod": true, "statefulset": true,
	"daemonset": true, "replicaset": true, "configmap": true, "secret": true,
	"ingress": true, "job": true, "cronjob": true, "namespace": true,
	"node": true, "persistentvolumeclaim": true, "http": true, "https": true,
	"tcp": true, "udp": true, "grpc": true, "port": true, "image": true,
	"name": true, "kind": true,
}

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
	programs, err := validatePrograms(ctx, root, workspace, module)
	if err != nil {
		return err
	}
	return generatePrograms(workspace, module, programs)
}

func validatePrograms(ctx context.Context, root, workspace, module string) ([]compiledProgram, error) {
	for _, part := range []string{"detectors", "playbooks", "lib"} {
		if err := checkProgramImports(filepath.Join(workspace, part), module); err != nil {
			return nil, err
		}
	}
	if err := validateLibraries(ctx, root, filepath.Join(workspace, "lib")); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(workspace, "playbooks"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var programs []compiledProgram
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		base := filepath.Join(workspace, "playbooks", id)
		files, err := filepath.Glob(filepath.Join(base, "*.go"))
		if err != nil {
			return nil, err
		}
		if len(files) == 0 {
			continue
		}
		if _, err := os.Stat(filepath.Join(base, "diagnose.go")); os.IsNotExist(err) {
			return nil, fmt.Errorf("playbook %s has Go sources but no diagnose.go", id)
		} else if err != nil {
			return nil, err
		}
		if !programID.MatchString(id) {
			return nil, fmt.Errorf("playbook program %q has invalid fault class", id)
		}
		contents, err := os.ReadDir(base)
		if err != nil {
			return nil, err
		}
		for _, file := range contents {
			if file.IsDir() {
				return nil, fmt.Errorf("playbook %s program cannot persist shell scripts or subdirectories", id)
			}
			name := file.Name()
			if name != "README.md" && name != "diagnose.go" && name != "mitigate.go" && !strings.HasSuffix(name, "_test.go") {
				return nil, fmt.Errorf("playbook %s has unexpected program file %q", id, name)
			}
		}
		readme, err := os.ReadFile(filepath.Join(base, "README.md"))
		if err != nil {
			return nil, fmt.Errorf("playbook %s README: %w", id, err)
		}
		if err := checkProgramReadme(string(readme)); err != nil {
			return nil, fmt.Errorf("playbook %s README: %w", id, err)
		}
		var metadata struct {
			FaultClass          string `yaml:"fault_class"`
			OriginatingIncident string `yaml:"originating_incident"`
		}
		if err := parseProgramFrontmatter(readme, &metadata); err != nil {
			return nil, fmt.Errorf("playbook %s README: %w", id, err)
		}
		if metadata.FaultClass != id {
			return nil, fmt.Errorf("playbook %s fault_class must match its ID", id)
		}
		outcome, err := memory.OpenReader(configRoot(root)).Get(ctx, metadata.OriginatingIncident)
		if err != nil {
			return nil, fmt.Errorf("playbook %s needs accepted outcome for incident %q: %w", id, metadata.OriginatingIncident, err)
		}
		literals := outcomeLiterals(outcome.Record)
		if err := checkIncidentLiterals(filepath.Join(workspace, "playbooks", id), filepath.Join(workspace, "lib"), id, literals); err != nil {
			return nil, err
		}
		methods := map[string]map[string]bool{}
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return nil, err
			}
			for _, declaration := range file.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
					continue
				}
				typeName := receiverName(fn.Recv.List[0].Type)
				if typeName == "" || !ast.IsExported(typeName) {
					continue
				}
				if methods[typeName] == nil {
					methods[typeName] = map[string]bool{}
				}
				methods[typeName][fn.Name.Name] = true
			}
		}
		var diagnose, mitigate []string
		for typeName, methods := range methods {
			if methods["Spec"] && methods["Diagnose"] {
				diagnose = append(diagnose, typeName)
			}
			if methods["Spec"] && methods["Mitigate"] {
				mitigate = append(mitigate, typeName)
			}
		}
		if len(diagnose) != 1 {
			return nil, fmt.Errorf("playbook %s must export exactly one Diagnoser; found %d", id, len(diagnose))
		}
		if len(mitigate) > 1 {
			return nil, fmt.Errorf("playbook %s may export at most one Mitigator; found %d", id, len(mitigate))
		}
		item := compiledProgram{id: id, diagnoser: diagnose[0]}
		if len(mitigate) == 1 {
			item.mitigator = mitigate[0]
		}
		cases, err := corpusCasesFor(filepath.Join(workspace, "corpus"), id)
		if err != nil {
			return nil, err
		}
		if len(cases) == 0 {
			return nil, fmt.Errorf("playbook %s needs a labeled corpus case", id)
		}
		item.cases = cases
		programs = append(programs, item)
	}
	sort.Slice(programs, func(i, j int) bool { return programs[i].id < programs[j].id })
	if err := checkCorpusLabels(filepath.Join(workspace, "corpus"), programs); err != nil {
		return nil, err
	}
	return programs, nil
}

func checkCorpusLabels(root string, programs []compiledProgram) error {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, item := range programs {
		known[item.id] = true
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			return fmt.Errorf("corpus entry %q must be a directory", entry.Name())
		}
		files, err := os.ReadDir(filepath.Join(root, entry.Name()))
		if err != nil {
			return err
		}
		allowed := map[string]bool{"open.json": true, "fixed.json": true, "healthy.json": true, "recordings.json": true, "labels.json": true}
		for _, file := range files {
			if file.IsDir() || !allowed[file.Name()] {
				return fmt.Errorf("unexpected corpus file %s; only snapshots, recordings, and labels are allowed", filepath.Join(entry.Name(), file.Name()))
			}
		}
		caseData, err := sdktest.LoadCorpusCase(filepath.Join(root, entry.Name()))
		if err != nil {
			return err
		}
		if len(caseData.RootCauseProgramIDs) == 0 {
			return fmt.Errorf("corpus %s needs root_cause_program_ids", entry.Name())
		}
		for _, id := range caseData.RootCauseProgramIDs {
			if !known[id] {
				return fmt.Errorf("corpus %s labels unknown program %q", entry.Name(), id)
			}
		}
	}
	return nil
}

func validateLibraries(ctx context.Context, memoryRoot, libRoot string) error {
	return filepath.WalkDir(libRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if os.IsNotExist(walkErr) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			if filepath.Dir(path) == libRoot || entry.Name() != "metadata.yaml" && !strings.HasSuffix(entry.Name(), ".go") {
				return fmt.Errorf("unexpected shared library file %s; only Go source and metadata.yaml are allowed", path)
			}
			return nil
		}
		files, err := filepath.Glob(filepath.Join(path, "*.go"))
		if err != nil || len(files) == 0 {
			return err
		}
		data, err := os.ReadFile(filepath.Join(path, "metadata.yaml"))
		if err != nil {
			return fmt.Errorf("shared library %s needs originating_incident metadata: %w", path, err)
		}
		var metadata struct {
			OriginatingIncident string `yaml:"originating_incident"`
		}
		if err := yaml.Unmarshal(data, &metadata); err != nil || metadata.OriginatingIncident == "" {
			return fmt.Errorf("shared library %s needs valid originating_incident metadata: %v", path, err)
		}
		outcome, err := memory.OpenReader(configRoot(memoryRoot)).Get(ctx, metadata.OriginatingIncident)
		if err != nil {
			return fmt.Errorf("shared library %s needs accepted outcome for incident %q: %w", path, metadata.OriginatingIncident, err)
		}
		return checkSourceLiterals(path, outcomeLiterals(outcome.Record))
	})
}

func configRoot(memoryRoot string) string { return filepath.Dir(memoryRoot) }

func receiverName(value ast.Expr) string {
	if pointer, ok := value.(*ast.StarExpr); ok {
		value = pointer.X
	}
	if name, ok := value.(*ast.Ident); ok {
		return name.Name
	}
	return ""
}

func checkProgramImports(root, module string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if os.IsNotExist(walkErr) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for _, imported := range file.Imports {
			name, err := strconv.Unquote(imported.Path.Value)
			if err != nil || !allowedProgramImport(name, module) {
				return fmt.Errorf("%s: authored Go may not import %q; use the read-only SDK or a shared library", path, name)
			}
		}
		return nil
	})
}

func allowedProgramImport(path, module string) bool {
	if strings.HasPrefix(path, module+"/lib/") || path == "sdo.dev/operator/sdk" || strings.HasPrefix(path, "sdo.dev/operator/sdk/") || strings.HasPrefix(path, "k8s.io/api/") {
		return true
	}
	if map[string]bool{"k8s.io/apimachinery/pkg/apis/meta/v1": true, "k8s.io/apimachinery/pkg/util/intstr": true, "k8s.io/apimachinery/pkg/labels": true, "k8s.io/apimachinery/pkg/api/resource": true, "k8s.io/apimachinery/pkg/types": true, "k8s.io/apimachinery/pkg/selection": true, "k8s.io/apimachinery/pkg/util/sets": true}[path] {
		return true
	}
	return map[string]bool{"bytes": true, "context": true, "encoding/json": true, "errors": true, "fmt": true, "io": true, "math": true, "math/rand/v2": true, "reflect": true, "regexp": true, "sort": true, "strconv": true, "strings": true, "testing": true, "time": true, "unicode": true, "unicode/utf8": true}[path]
}

func checkProgramReadme(body string) error {
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if shellFence.MatchString(trimmed) || shellLine.MatchString(line) {
			return fmt.Errorf("shell command or fenced shell block is forbidden; keep judgement guidance only")
		}
	}
	return nil
}

func parseProgramFrontmatter(body []byte, target any) error {
	parts := strings.SplitN(string(body), "---\n", 3)
	if len(parts) != 3 || parts[0] != "" {
		return fmt.Errorf("front matter is required")
	}
	if err := yaml.Unmarshal([]byte(parts[1]), target); err != nil {
		return err
	}
	return nil
}

func outcomeLiterals(record *contracts.OutcomeRecord) []string {
	values := map[string]bool{}
	add := func(value string) {
		if relevantIncidentLiteral(value) {
			values[value] = true
		}
	}
	addRef := func(ref *contracts.ObjectRef) {
		if ref != nil {
			add(ref.GetName())
		}
	}
	scan := func(value string) {
		for _, port := range incidentPort.FindAllString(value, -1) {
			add(port)
		}
		for _, match := range incidentImage.FindAllStringSubmatch(value, -1) {
			add(match[1])
		}
	}
	for _, finding := range record.GetFindings() {
		addRef(finding.GetPrimaryResource())
		for _, ref := range finding.GetRelatedResources() {
			addRef(ref)
		}
		scan(finding.GetSummary())
		scan(finding.GetEvidence())
		if finding.GetMetadata() != nil {
			var visit func(string, any)
			visit = func(key string, value any) {
				switch value := value.(type) {
				case map[string]any:
					for name, child := range value {
						visit(name, child)
					}
				case []any:
					for _, child := range value {
						visit(key, child)
					}
				case string:
					scan(value)
					lower := strings.ToLower(key)
					if strings.Contains(lower, "image") || strings.Contains(lower, "name") {
						add(value)
					}
				case float64:
					if strings.Contains(strings.ToLower(key), "port") {
						add(strconv.FormatInt(int64(value), 10))
					}
				}
			}
			visit("", finding.GetMetadata().AsMap())
		}
	}
	for _, action := range record.GetRepairActions() {
		for _, ref := range action.GetResources() {
			addRef(ref)
		}
		add(action.GetTarget())
		scan(action.GetSummary())
		scan(action.GetDetails())
		scan(action.GetTarget())
	}
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func checkIncidentLiterals(programRoot, libRoot, faultClass string, literals []string) error {
	for _, literal := range literals {
		if relevantIncidentLiteral(literal) && containsIncidentToken(faultClass, literal) {
			return fmt.Errorf("fault class %q contains incident literal %q; use a role-based name", faultClass, literal)
		}
	}
	for _, root := range []string{programRoot, libRoot} {
		if err := checkSourceLiterals(root, literals); err != nil {
			return err
		}
	}
	return nil
}

func checkSourceLiterals(root string, literals []string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if os.IsNotExist(walkErr) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return fmt.Errorf("parse %s for incident literals: %w", path, err)
		}
		var found string
		ast.Inspect(file, func(node ast.Node) bool {
			value, ok := node.(*ast.BasicLit)
			if !ok {
				return true
			}
			for _, literal := range literals {
				if !relevantIncidentLiteral(literal) {
					continue
				}
				switch value.Kind {
				case token.STRING:
					unquoted, err := strconv.Unquote(value.Value)
					if err == nil && containsIncidentToken(unquoted, literal) {
						found = literal
					}
				case token.INT:
					if isPortLiteral(literal) {
						port, _ := strconv.ParseInt(literal, 10, 64)
						integer, err := strconv.ParseInt(value.Value, 0, 64)
						if err == nil && integer == port {
							found = literal
						}
					}
				}
				if found != "" {
					return false
				}
			}
			return true
		})
		if found != "" {
			return fmt.Errorf("%s contains incident literal %q; bind resources and derive ports or images from the ReadView", path, found)
		}
		return nil
	})
}

func relevantIncidentLiteral(value string) bool {
	return (len(value) >= 3 || isPortLiteral(value)) && !ignoredIncidentLiterals[strings.ToLower(value)]
}

func isPortLiteral(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	port, err := strconv.ParseUint(value, 10, 16)
	return err == nil && port > 0
}

func containsIncidentToken(value, literal string) bool {
	for offset := 0; offset < len(value); {
		index := strings.Index(value[offset:], literal)
		if index < 0 {
			return false
		}
		index += offset
		end := index + len(literal)
		if (index == 0 || !incidentTokenChar(value[index-1])) && (end == len(value) || !incidentTokenChar(value[end])) {
			return true
		}
		offset = index + 1
	}
	return false
}

func incidentTokenChar(character byte) bool {
	return character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
}

func corpusCasesFor(root, programID string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var result []string
	for _, entry := range entries {
		if !entry.IsDir() {
			return nil, fmt.Errorf("corpus entry %s must be a directory", entry.Name())
		}
		caseData, err := sdktest.LoadCorpusCase(filepath.Join(root, entry.Name()))
		if err != nil {
			return nil, err
		}
		for _, id := range caseData.RootCauseProgramIDs {
			if id == programID {
				result = append(result, entry.Name())
				break
			}
		}
	}
	return result, nil
}

func generatePrograms(root, module string, programs []compiledProgram) error {
	var source strings.Builder
	source.WriteString("package generated\n\nimport (\n\t\"sdo.dev/operator/sdk/program\"\n")
	for index, item := range programs {
		fmt.Fprintf(&source, "\tp%d %q\n", index, module+"/playbooks/"+item.id)
	}
	source.WriteString(")\n\nfunc Programs() (program.ProgramRegistry, error) {\n\treturn program.NewRegistry(\n\t\t[]program.Diagnoser{")
	for index, item := range programs {
		fmt.Fprintf(&source, "new(p%d.%s),", index, item.diagnoser)
	}
	source.WriteString("},\n\t\t[]program.Mitigator{")
	for index, item := range programs {
		if item.mitigator != "" {
			fmt.Fprintf(&source, "new(p%d.%s),", index, item.mitigator)
		}
	}
	source.WriteString("},\n\t)\n}\n")
	if err := os.WriteFile(filepath.Join(root, "generated", "programs.go"), []byte(source.String()), 0o644); err != nil {
		return err
	}
	var test strings.Builder
	test.WriteString("package generated\n\nimport (\n\t\"testing\"\n\t\"path/filepath\"\n\t\"sdo.dev/operator/sdk/program\"\n\t\"sdo.dev/operator/sdk/sdktest\"\n)\n\nfunc TestProgramCorpus(t *testing.T) {\n\tregistry, err := Programs()\n\tif err != nil { t.Fatal(err) }\n")
	for _, item := range programs {
		fmt.Fprintf(&test, "\t{\n\t\tp, ok := registry.Diagnoser(%q)\n\t\tif !ok { t.Fatal(%q) }\n\t\tif p.Spec().ID != %q { t.Fatalf(\"program ID mismatch: %%s\", p.Spec().ID) }\n", item.id, "missing diagnoser "+item.id, item.id)
		for _, caseID := range item.cases {
			fmt.Fprintf(&test, "\t\t{\n\t\t\tc, err := sdktest.LoadCorpusCase(filepath.Join(\"..\", \"corpus\", %q))\n\t\t\tif err != nil { t.Fatal(err) }\n\t\t\tif err := program.ValidateBindings(p.Spec(), c.Bindings); err != nil { t.Fatalf(\"case %s binding: %%v\", err) }\n\t\t\tif err := program.ValidateBindings(p.Spec(), c.PeerBindings); err != nil { t.Fatalf(\"case %s peer binding: %%v\", err) }\n\t\t\tsdktest.CheckGeneralization(t, p, c, c.PeerBindings)\n\t\t\tif m, ok := registry.Mitigator(%q); ok { sdktest.CheckMitigatorGeneralization(t, m, c) }\n\t\t}\n", caseID, caseID, caseID, item.id)
		}
		test.WriteString("\t}\n")
	}
	test.WriteString("}\n")
	if len(programs) == 0 {
		test.Reset()
		test.WriteString("package generated\n")
	}
	return os.WriteFile(filepath.Join(root, "generated", "programs_test.go"), []byte(test.String()), 0o644)
}
