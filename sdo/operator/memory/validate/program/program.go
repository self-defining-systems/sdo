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
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/sdktest"
)

// Spec is a validated program constructor and its replay cases.
type Spec struct {
	ID, Diagnoser, Mitigator string
	Cases                    []string
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
	retired := map[string]bool{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		ID := entry.Name()
		base := filepath.Join(workspace, "playbooks", ID)
		if _, err := os.Stat(filepath.Join(base, "README.md")); err == nil {
			inactive, err := layout.ReadPlaybookRetired(base)
			if err != nil {
				return nil, fmt.Errorf("playbook %s: %w", ID, err)
			}
			if inactive {
				retired[ID] = true
				continue
			}
		} else if !os.IsNotExist(err) {
			return nil, err
		}
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
		item.Cases = Cases
		if err := validateScriptDigests(base, filepath.Join(workspace, "corpus"), item); err != nil {
			return nil, fmt.Errorf("playbook %s: %w", ID, err)
		}
		programs = append(programs, item)
	}
	sort.Slice(programs, func(i, j int) bool { return programs[i].ID < programs[j].ID })
	if !adoption {
		allCases := map[string]bool{}
		for _, item := range programs {
			for _, id := range item.Cases {
				allCases[id] = true
			}
		}
		var shared Spec
		for id := range allCases {
			shared.Cases = append(shared.Cases, id)
		}
		sort.Strings(shared.Cases)
		if err := filepath.WalkDir(filepath.Join(workspace, "lib"), func(path string, entry os.DirEntry, walkErr error) error {
			if os.IsNotExist(walkErr) {
				return nil
			}
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return validateScriptDigests(path, filepath.Join(workspace, "corpus"), shared)
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("shared script provenance: %w", err)
		}
		if err := checkCorpusLabels(filepath.Join(workspace, "corpus"), programs, retired); err != nil {
			return nil, err
		}
	}
	return programs, nil
}

func checkCorpusLabels(root string, programs []Spec, retired map[string]bool) error {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for id := range retired {
		known[id] = true
	}
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
		allowed := map[string]bool{"open.json": true, "fixed.json": true, "healthy.json": true, "recordings.json": true, "labels.json": true, "accepted.json": true, "accepted.yaml": true}
		if marked {
			allowed["operator.json"] = true
			allowed["accepted.json"] = true
		}
		for _, file := range files {
			if file.IsDir() || !allowed[file.Name()] {
				return fmt.Errorf("unexpected corpus file %s; only snapshots, recordings, and labels are allowed", filepath.Join(entry.Name(), file.Name()))
			}
		}
		caseData, err := sdktest.LoadCorpusMetadata(filepath.Join(root, entry.Name()))
		if err != nil {
			return err
		}
		if len(caseData.RootCauseProgramIDs) == 0 && len(caseData.NoMatchProgramIDs) == 0 {
			return fmt.Errorf("corpus %s needs root_cause_program_ids or no_match_program_ids", entry.Name())
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
		if entry.IsDir() && filepath.Base(root) == "playbooks" && filepath.Dir(path) == root {
			if _, err := os.Stat(filepath.Join(path, "README.md")); err == nil {
				retired, err := layout.ReadPlaybookRetired(path)
				if err != nil {
					return err
				}
				if retired {
					return filepath.SkipDir
				}
			} else if !os.IsNotExist(err) {
				return err
			}
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
			// Only tests may import executable playbooks. Always-on detectors use
			// independent detection entry points; shared predicates belong in lib.
			playbookImport := strings.HasSuffix(path, "_test.go") && strings.HasPrefix(name, module+"/playbooks/")
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
	if map[string]bool{"k8s.io/metrics/pkg/apis/metrics/v1beta1": true, "k8s.io/apimachinery/pkg/apis/meta/v1": true, "k8s.io/apimachinery/pkg/util/intstr": true, "k8s.io/apimachinery/pkg/labels": true, "k8s.io/apimachinery/pkg/api/resource": true, "k8s.io/apimachinery/pkg/types": true, "k8s.io/apimachinery/pkg/runtime/schema": true, "k8s.io/apimachinery/pkg/selection": true, "k8s.io/apimachinery/pkg/util/sets": true}[path] {
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
		caseData, err := sdktest.LoadCorpusMetadata(filepath.Join(root, entry.Name()))
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

// ReplayTests records replay observations without using their verdicts as acceptance gates.
func ReplayTests(programs []Spec) string {
	if len(programs) == 0 {
		return "package generated\n"
	}
	hasCases := false
	for _, item := range programs {
		hasCases = hasCases || len(item.Cases) > 0
	}
	imports := []string{"encoding/json", "os", "path/filepath", "testing", "sdo.dev/operator/runtime/replay", "sdo.dev/operator/sdk/sdktest"}
	if hasCases {
		imports = append(imports, "context")
	}
	var test strings.Builder
	test.WriteString("package generated\nimport(")
	for _, name := range imports {
		fmt.Fprintf(&test, "%q;", name)
	}
	test.WriteString(`)
func TestProgramSourceSafety(t *testing.T){
 for _, pattern := range []string{"../corpus/*/accepted.json", "../corpus/*/accepted.yaml", "../corpus/*/revisions/*.json", "../corpus/*/revisions/*.yaml"} {
  paths,err:=filepath.Glob(pattern);if err!=nil{t.Fatal(err)}
  for _,path:=range paths {if _,err:=replay.LoadManifests(path);err!=nil{t.Fatal(err)}}
 }
}
func TestProgramCorpus(t *testing.T){
 var notes []sdktest.MitigationReplayResult
 var gaps []string
 defer func(){data,err:=json.Marshal(notes);if err!=nil{t.Fatal(err)};if err:=os.WriteFile("mitigation-replay.json",data,0600);err!=nil{t.Fatal(err)};data,err=json.Marshal(gaps);if err!=nil{t.Fatal(err)};if err:=os.WriteFile("coverage-gaps.json",data,0600);err!=nil{t.Fatal(err)}}()
`)
	if hasCases {
		test.WriteString("registry,err:=Programs();if err!=nil{t.Fatal(err)}\n")
	}
	for _, item := range programs {
		if len(item.Cases) == 0 {
			fmt.Fprintf(&test, `notes=append(notes,sdktest.MitigationReplayResult{ProgramID:%q,CaseID:"unavailable",Status:sdktest.MitigationReplaySkippedUnreplayableEffect,Effects:[]string{"replay unverified: no recorded case"}})
`, item.ID)
		}
		for _, id := range item.Cases {
			fmt.Fprintf(&test, `t.Run(%q,func(t *testing.T){
 c,err:=replay.LoadRepairCase(filepath.Join("..","corpus",%q))
 if err!=nil{notes=append(notes,sdktest.MitigationReplayResult{ProgramID:%q,CaseID:%q,Status:sdktest.MitigationReplaySkippedUnreplayableEffect,Effects:[]string{"replay unverified: "+err.Error()}});return}
 r:=replay.CheckRepeat(context.Background(),registry,All(),%q,c)
 t.Logf("replay observation: status=%%s errors=%%v faulted=%%s post=%%s",r.Status,r.Errors,r.Faulted.Result,r.Post.Result)
 gaps=append(gaps,r.Notes...)
 notes=append(notes,ReplayNote(%q,%q,r))
 })
`, item.ID+"/"+id, id, item.ID, id, item.ID, item.ID, id)
		}
	}
	test.WriteString("}\nfunc TestProgramRegistrationContracts(t *testing.T){if _,err:=Programs();err!=nil{t.Fatal(err)}}\n")
	test.WriteString(scriptReplayNoteHelper)
	return test.String()
}
