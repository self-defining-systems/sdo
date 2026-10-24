// Package program owns the static policy for authored playbook programs.
package program

import (
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
	"sdo.dev/operator/sdk/sdktest"
)

// Spec is a validated program constructor and its replay cases.
type Spec struct {
	ID, Diagnoser, Mitigator string
	Cases, NegativeCases     []string
	OperatorCases            []string
}

var programID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
var shellLine = regexp.MustCompile(`(?i)^\s*(kubectl|curl|bash)(\s|$)`)
var shellFence = regexp.MustCompile(`(?i)^(?:` + "```" + `|~~~)[ \t]*(bash|sh|zsh|shell|console|terminal|powershell)(\s|$)`)

// Inspect validates a compiler workspace containing detectors and program memory.
func Inspect(workspace, module string) ([]Spec, error) {
	return inspect(workspace, module, filepath.Join(workspace, "detectors"), false)
}

// InspectMemory validates an application memory root before it is projected into a compiler workspace.
func InspectMemory(memoryRoot, module string) ([]Spec, error) {
	return inspect(memoryRoot, module, filepath.Join(memoryRoot, "diagnostics", "detectors"), false)
}

// InspectForAdoption discovers constructors without accepting corpus or replay evidence.
// It is only used by explicit initial adoption and identical accepted-memory builds.
func InspectMemoryForAdoption(root, module string) ([]Spec, error) {
	return inspect(root, module, filepath.Join(root, "diagnostics", "detectors"), true)
}

func InspectForAdoption(workspace, module string) ([]Spec, error) {
	return inspect(workspace, module, filepath.Join(workspace, "detectors"), true)
}

func inspect(workspace, module, detectors string, adoption bool) ([]Spec, error) {
	for _, part := range []string{detectors, filepath.Join(workspace, "playbooks"), filepath.Join(workspace, "lib")} {
		if err := checkProgramImports(part, module); err != nil {
			return nil, err
		}
	}
	if err := validateLibraries(filepath.Join(workspace, "lib")); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(workspace, "playbooks"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var programs []Spec
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		ID := entry.Name()
		base := filepath.Join(workspace, "playbooks", ID)
		files, err := filepath.Glob(filepath.Join(base, "*.go"))
		if err != nil {
			return nil, err
		}
		if len(files) == 0 {
			continue
		}
		if _, err := os.Stat(filepath.Join(base, "diagnose.go")); os.IsNotExist(err) {
			return nil, fmt.Errorf("playbook %s has Go sources but no diagnose.go", ID)
		} else if err != nil {
			return nil, err
		}
		if !programID.MatchString(ID) {
			return nil, fmt.Errorf("playbook program %q has invalid fault class", ID)
		}
		contents, err := os.ReadDir(base)
		if err != nil {
			return nil, err
		}
		for _, file := range contents {
			if file.IsDir() {
				return nil, fmt.Errorf("playbook %s program cannot persist shell scripts or subdirectories", ID)
			}
			name := file.Name()
			if name != "README.md" && name != "diagnose.go" && name != "mitigate.go" && !strings.HasSuffix(name, "_test.go") {
				return nil, fmt.Errorf("playbook %s has unexpected program file %q", ID, name)
			}
		}
		readme, err := os.ReadFile(filepath.Join(base, "README.md"))
		if err != nil {
			return nil, fmt.Errorf("playbook %s README: %w", ID, err)
		}
		if err := checkProgramReadme(string(readme)); err != nil {
			return nil, fmt.Errorf("playbook %s README: %w", ID, err)
		}
		var metadata struct {
			FaultClass string `yaml:"fault_class"`
		}
		if err := parseProgramFrontmatter(readme, &metadata); err != nil {
			return nil, fmt.Errorf("playbook %s README: %w", ID, err)
		}
		if metadata.FaultClass != ID {
			return nil, fmt.Errorf("playbook %s fault_class must match its ID", ID)
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
			return nil, fmt.Errorf("playbook %s must export exactly one Diagnoser; found %d", ID, len(diagnose))
		}
		if len(mitigate) > 1 {
			return nil, fmt.Errorf("playbook %s may export at most one Mitigator; found %d", ID, len(mitigate))
		}
		item := Spec{ID: ID, Diagnoser: diagnose[0]}
		if len(mitigate) == 1 {
			item.Mitigator = mitigate[0]
		}
		if adoption {
			programs = append(programs, item)
			continue
		}
		Cases, err := corpusCasesFor(filepath.Join(workspace, "corpus"), ID)
		if err != nil {
			return nil, err
		}
		if len(Cases) == 0 {
			return nil, fmt.Errorf("playbook %s needs a labeled corpus case", ID)
		}
		item.Cases = Cases
		for _, caseID := range Cases {
			marked, err := OperatorCorpus(filepath.Join(workspace, "corpus", caseID))
			if err != nil {
				return nil, err
			}
			if marked {
				item.OperatorCases = append(item.OperatorCases, caseID)
			}
		}
		item.NegativeCases, err = noMatchCasesFor(filepath.Join(workspace, "corpus"), ID)
		if err != nil {
			return nil, err
		}
		programs = append(programs, item)
	}
	sort.Slice(programs, func(i, j int) bool { return programs[i].ID < programs[j].ID })
	if !adoption {
		if err := checkCorpusLabels(filepath.Join(workspace, "corpus"), programs); err != nil {
			return nil, err
		}
	}
	return programs, nil
}

func checkCorpusLabels(root string, programs []Spec) error {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, item := range programs {
		known[item.ID] = true
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			return fmt.Errorf("corpus entry %q must be a directory", entry.Name())
		}
		files, err := os.ReadDir(filepath.Join(root, entry.Name()))
		if err != nil {
			return err
		}
		marked, err := OperatorCorpus(filepath.Join(root, entry.Name()))
		if err != nil {
			return err
		}
		allowed := map[string]bool{"open.json": true, "fixed.json": true, "healthy.json": true, "recordings.json": true, "labels.json": true}
		if marked {
			allowed["operator.json"] = true
			allowed["accepted.json"] = true
		}
		for _, file := range files {
			if file.IsDir() || !allowed[file.Name()] {
				return fmt.Errorf("unexpected corpus file %s; only snapshots, recordings, and labels are allowed", filepath.Join(entry.Name(), file.Name()))
			}
		}
		caseData, err := sdktest.LoadCorpusCase(filepath.Join(root, entry.Name()))
		if err != nil {
			return err
		}
		if len(caseData.RootCauseProgramIDs) == 0 && len(caseData.NoMatchProgramIDs) == 0 {
			return fmt.Errorf("corpus %s needs root_cause_program_ids or no_match_program_ids", entry.Name())
		}
		if caseData.SyntheticNoMatchReason != "" {
			if err := sdktest.ValidateCorpusProvenance(filepath.Join(root, entry.Name())); err != nil {
				return fmt.Errorf("corpus %s: %w", entry.Name(), err)
			}
		}
		for _, ID := range append(caseData.RootCauseProgramIDs, caseData.NoMatchProgramIDs...) {
			if !known[ID] {
				return fmt.Errorf("corpus %s labels unknown program %q", entry.Name(), ID)
			}
		}
	}
	return nil
}

func validateLibraries(libRoot string) error {
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
		return nil
	})
}

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
			// Detectors can promote an application's diagnoser without copying its
			// fault predicate. Tests may exercise the same public program contract.
			playbookImport := (filepath.Base(root) == "detectors" || strings.HasSuffix(path, "_test.go")) && strings.HasPrefix(name, module+"/playbooks/")
			if err != nil || !allowedProgramImport(name, module) && !playbookImport {
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
		for _, ID := range caseData.RootCauseProgramIDs {
			if ID == programID {
				result = append(result, entry.Name())
				break
			}
		}
	}
	return result, nil
}

func noMatchCasesFor(root, programID string) ([]string, error) {
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
			continue
		}
		caseData, err := sdktest.LoadCorpusCase(filepath.Join(root, entry.Name()))
		if err != nil {
			return nil, err
		}
		for _, ID := range caseData.NoMatchProgramIDs {
			if ID == programID {
				result = append(result, entry.Name())
				break
			}
		}
	}
	return result, nil
}

// ReplayTests generates mandatory corpus, rename-invariance and mitigation generalization tests.
func ReplayTests(programs []Spec) string {
	var test strings.Builder
	test.WriteString("package generated\n\nimport (\n\t\"encoding/json\"\n\t\"os\"\n\t\"testing\"\n\t\"sdo.dev/operator/sdk/sdktest\"\n")
	for _, item := range programs {
		if len(item.Cases)+len(item.NegativeCases) > 0 {
			test.WriteString("\t\"path/filepath\"\n\t\"sdo.dev/operator/sdk/program\"\n")
			break
		}
	}
	test.WriteString(")\n\nfunc TestProgramCorpus(t *testing.T) {\n var notes []sdktest.MitigationReplayResult\n defer func(){ data,err:=json.Marshal(notes);if err!=nil {t.Fatal(err)};if err:=os.WriteFile(\"mitigation-replay.json\",data,0600);err!=nil {t.Fatal(err)} }()\n\tregistry, err := Programs()\n\tif err != nil { t.Fatal(err) }\n")
	for _, item := range programs {
		fmt.Fprintf(&test, "\t{\n\t\tp, ok := registry.Diagnoser(%q)\n\t\tif !ok { t.Fatal(%q) }\n\t\tif p.Spec().ID != %q { t.Fatalf(\"program ID mismatch: %%s\", p.Spec().ID) }\n", item.ID, "missing diagnoser "+item.ID, item.ID)
		for _, caseID := range item.Cases {
			fmt.Fprintf(&test, "\t\t{\n\t\t\tc, err := sdktest.LoadCorpusCase(filepath.Join(\"..\", \"corpus\", %q))\n\t\t\tif err != nil { t.Fatal(err) }\n\t\t\tif err := program.ValidateBindings(p.Spec(), c.Bindings); err != nil { t.Fatalf(\"case %s binding: %%v\", err) }\n\t\t\tif err := program.ValidateBindings(p.Spec(), c.PeerBindings); err != nil { t.Fatalf(\"case %s peer binding: %%v\", err) }\n\t\t\tsdktest.CheckGeneralization(t, p, c, c.PeerBindings)\n\t\t\tsdktest.CheckRenameInvariance(t, p, c)\n\t\t\tif m, ok := registry.Mitigator(%q); ok { sdktest.CheckMitigatorGeneralization(t, m, c); result,err:=sdktest.CheckMitigation(p,m,c); if err!=nil {t.Error(err)} else {result.CaseID=%q;notes=append(notes,result)} }\n\t\t}\n", caseID, caseID, caseID, item.ID, caseID)
		}
		for _, caseID := range item.NegativeCases {
			fmt.Fprintf(&test, "\t\t{\n\t\t\tc, err := sdktest.LoadCorpusCase(filepath.Join(\"..\", \"corpus\", %q))\n\t\t\tif err != nil { t.Fatal(err) }\n\t\t\tif err := program.ValidateBindings(p.Spec(), c.Bindings); err != nil { t.Fatal(err) }\n\t\t\tsdktest.CheckNoMatchCase(t, p, c)\n\t\t}\n", caseID)
		}
		test.WriteString("\t}\n")
	}
	test.WriteString("}\n")
	if len(programs) == 0 {
		test.Reset()
		test.WriteString("package generated\n")
	}
	return test.String()
}
