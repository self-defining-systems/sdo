package memory

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type ArtifactRole string

var detectorIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)

const (
	HealthJudgeArtifacts ArtifactRole = "health_judge"
	ResponderArtifacts   ArtifactRole = "responder"
)

type artifactManifest struct {
	APIVersion string             `yaml:"apiVersion"`
	Kind       string             `yaml:"kind"`
	SDKVersion string             `yaml:"sdkVersion"`
	Detectors  []artifactDetector `yaml:"detectors"`
}

type artifactDetector struct {
	ID                  string              `yaml:"id"`
	Package             string              `yaml:"package"`
	Constructor         string              `yaml:"constructor"`
	Class               string              `yaml:"class"`
	Owner               string              `yaml:"owner"`
	Watches             []artifactWatch     `yaml:"watches"`
	Interval            string              `yaml:"interval"`
	Persistence         artifactPersistence `yaml:"persistence"`
	Batching            artifactBatching    `yaml:"batching"`
	PossiblePlaybooks   []string            `yaml:"possiblePlaybooks"`
	OriginatingIncident string              `yaml:"originatingIncident,omitempty"`
	OriginatingCommit   string              `yaml:"originatingCommit,omitempty"`
}

type artifactWatch struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Namespace  string `yaml:"namespace,omitempty"`
}
type artifactPersistence struct {
	Firing   int32 `yaml:"firing"`
	Clearing int32 `yaml:"clearing"`
}
type artifactBatching struct {
	Severity string `yaml:"severity"`
	Debounce string `yaml:"debounce"`
}

// MaterializeArtifacts is the only edge that maps worker-owned logical artifacts
// to the current repository layout. Broker validation still decides acceptance.
func MaterializeArtifacts(root string, proposal *contracts.OperationalArtifactProposal, role ArtifactRole) error {
	if root == "" || proposal == nil || (role != HealthJudgeArtifacts && role != ResponderArtifacts) {
		return errors.New("checkout, typed artifacts, and known role are required")
	}
	if err := validateArtifactProposal(proposal, role); err != nil {
		return err
	}
	if err := rejectArtifactSymlinks(filepath.Join(root, ".sdo")); err != nil {
		return err
	}
	manifestPath := filepath.Join(root, ".sdo", "diagnostics", "manifest.yaml")
	data, err := os.ReadFile(manifestPath)
	if os.IsNotExist(err) && role == HealthJudgeArtifacts {
		data = []byte("apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\ndetectors: []\n")
	} else if err != nil {
		return fmt.Errorf("read accepted detector manifest: %w", err)
	}
	var manifest artifactManifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("decode detector manifest: %w", err)
	}
	if manifest.APIVersion != "sdo.dev/v1alpha1" || manifest.Kind != "DetectorManifest" {
		return errors.New("invalid accepted detector manifest")
	}
	byID := make(map[string]artifactDetector, len(manifest.Detectors))
	for _, item := range manifest.Detectors {
		if _, exists := byID[item.ID]; exists {
			return fmt.Errorf("duplicate accepted detector %q", item.ID)
		}
		byID[item.ID] = item
	}
	for _, id := range proposal.GetDeleteDetectorIds() {
		prior, exists := byID[id]
		if !exists || prior.Owner != string(role) || !safeDetectorPackage(prior.Package) {
			return fmt.Errorf("cannot delete detector %q outside %s-owned diagnostics", id, role)
		}
	}
	for _, item := range proposal.GetDetectors() {
		if prior, exists := byID[item.GetId()]; exists && (prior.Owner != string(role) || !safeDetectorPackage(prior.Package)) {
			return fmt.Errorf("cannot replace detector %q outside %s-owned diagnostics", item.GetId(), role)
		}
	}
	for _, id := range proposal.GetDeleteDetectorIds() {
		prior, exists := byID[id]
		if !exists {
			return fmt.Errorf("cannot delete unknown detector %q", id)
		}
		if err := os.RemoveAll(filepath.Join(root, ".sdo", "diagnostics", filepath.Clean(prior.Package))); err != nil {
			return err
		}
		delete(byID, id)
	}
	for _, item := range proposal.GetDetectors() {
		classDir := "incidents"
		if role == HealthJudgeArtifacts {
			classDir = "health"
		}
		packagePath := filepath.Join("detectors", classDir, item.GetId())
		if prior, exists := byID[item.GetId()]; exists {
			packagePath = strings.TrimPrefix(filepath.Clean(prior.Package), "./")
		}
		entry := artifactDetector{ID: item.GetId(), Package: "./" + filepath.ToSlash(packagePath), Constructor: item.GetConstructor(), Class: item.GetDetectorClass(), Owner: item.GetOwner(), Interval: item.GetInterval(), Persistence: artifactPersistence{item.GetFiring(), item.GetClearing()}, Batching: artifactBatching{item.GetSeverity(), item.GetDebounce()}, OriginatingCommit: item.GetOriginatingCommit(), OriginatingIncident: item.GetOriginatingIncident()}
		for _, watch := range item.GetWatches() {
			entry.Watches = append(entry.Watches, artifactWatch{APIVersion: watch.GetApiVersion(), Kind: watch.GetKind()})
		}
		for _, id := range item.GetPossiblePlaybookIds() {
			entry.PossiblePlaybooks = append(entry.PossiblePlaybooks, id)
		}
		base := filepath.Join(root, ".sdo", "diagnostics", packagePath)
		if err := os.MkdirAll(base, 0o755); err != nil {
			return err
		}
		for _, source := range item.GetSources() {
			if err := os.WriteFile(filepath.Join(base, source.GetName()), []byte(source.GetContent()), 0o644); err != nil {
				return err
			}
		}
		byID[item.GetId()] = entry
	}
	if role == HealthJudgeArtifacts {
		if err := installGenericHealth(root, byID, proposal.GetTrafficWorkloads()); err != nil {
			return err
		}
	}
	manifest.Detectors = manifest.Detectors[:0]
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		manifest.Detectors = append(manifest.Detectors, byID[id])
	}
	encoded, err := yaml.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(manifestPath, encoded, 0o644); err != nil {
		return err
	}
	playbookRoot := filepath.Join(root, ".sdo", "playbooks")
	for _, id := range proposal.GetDeletePlaybookIds() {
		if err := os.RemoveAll(filepath.Join(playbookRoot, id)); err != nil {
			return err
		}
	}
	for _, item := range proposal.GetPlaybooks() {
		base := filepath.Join(playbookRoot, item.GetId())
		if err := os.MkdirAll(base, 0o755); err != nil {
			return err
		}
		body := strings.TrimSpace(item.GetMarkdown())
		frontmatter := fmt.Sprintf("---\nschema_version: 1\nowner: %s\nfault_class: %s\noriginating_incident: %s\n---\n\n%s\n", strconv.Quote(item.GetOwner()), strconv.Quote(item.GetFaultClass()), strconv.Quote(item.GetOriginatingIncident()), body)
		if err := os.WriteFile(filepath.Join(base, "README.md"), []byte(frontmatter), 0o644); err != nil {
			return err
		}
		for _, script := range item.GetScripts() {
			scripts := filepath.Join(base, "scripts")
			if err := os.MkdirAll(scripts, 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(scripts, script.GetName()), []byte(script.GetContent()), 0o755); err != nil {
				return err
			}
		}
		if program := item.GetProgram(); program != nil {
			prior, err := filepath.Glob(filepath.Join(base, "*.go"))
			if err != nil {
				return err
			}
			for _, path := range prior {
				if err := os.Remove(path); err != nil {
					return err
				}
			}
			for _, source := range program.GetSources() {
				if err := os.WriteFile(filepath.Join(base, source.GetName()), []byte(source.GetContent()), 0o644); err != nil {
					return err
				}
			}
			for _, corpus := range program.GetCorpusCases() {
				caseRoot := filepath.Join(root, ".sdo", "corpus", corpus.GetId())
				if err := os.MkdirAll(caseRoot, 0o755); err != nil {
					return err
				}
				for name, content := range map[string]string{"open.json": corpus.GetOpenJson(), "fixed.json": corpus.GetFixedJson(), "healthy.json": corpus.GetHealthyJson(), "recordings.json": corpus.GetRecordingsJson(), "labels.json": corpus.GetLabelsJson()} {
					if err := os.WriteFile(filepath.Join(caseRoot, name), []byte(content), 0o644); err != nil {
						return err
					}
				}
			}
		}
	}
	for _, library := range proposal.GetLibraries() {
		base := filepath.Join(root, ".sdo", "lib", filepath.FromSlash(library.GetPackage()))
		if err := os.MkdirAll(base, 0o755); err != nil {
			return err
		}
		prior, err := filepath.Glob(filepath.Join(base, "*.go"))
		if err != nil {
			return err
		}
		for _, path := range prior {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
		for _, source := range library.GetSources() {
			if err := os.WriteFile(filepath.Join(base, source.GetName()), []byte(source.GetContent()), 0o644); err != nil {
				return err
			}
		}
		origin := proposal.GetPlaybooks()[0].GetOriginatingIncident()
		if err := os.WriteFile(filepath.Join(base, "metadata.yaml"), []byte("originating_incident: "+strconv.Quote(origin)+"\n"), 0o644); err != nil {
			return err
		}
	}
	for _, generator := range proposal.GetTrafficGenerators() {
		base := filepath.Join(root, ".sdo", "diagnostics", "traffic", "generators")
		if generator.GetId() != "application" {
			base = filepath.Join(base, generator.GetId())
		}
		if err := os.MkdirAll(base, 0o755); err != nil {
			return err
		}
		for _, source := range generator.GetSources() {
			if err := os.WriteFile(filepath.Join(base, source.GetName()), []byte(source.GetContent()), 0o644); err != nil {
				return err
			}
		}
	}
	for _, workload := range proposal.GetTrafficWorkloads() {
		data, err := protojson.Marshal(workload)
		if err != nil {
			return err
		}
		var fields map[string]any
		if err := json.Unmarshal(data, &fields); err != nil {
			return err
		}
		encoded, err := yaml.Marshal(fields)
		if err != nil {
			return err
		}
		base := filepath.Join(root, ".sdo", "diagnostics", "traffic", "workloads")
		if err := os.MkdirAll(base, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(base, workload.GetName()+".yaml"), encoded, 0o644); err != nil {
			return err
		}
	}
	return refreshPlaybookIndex(playbookRoot)
}

func validateArtifactProposal(proposal *contracts.OperationalArtifactProposal, role ArtifactRole) error {
	seen := map[string]bool{}
	corpusIDs := map[string]bool{}
	checkID := func(id string) error {
		if !playbookIDPattern.MatchString(id) {
			return fmt.Errorf("invalid logical artifact ID %q", id)
		}
		if seen[id] {
			return fmt.Errorf("duplicate artifact ID %q", id)
		}
		seen[id] = true
		return nil
	}
	for _, item := range proposal.GetDetectors() {
		if item == nil {
			return errors.New("nil detector proposal")
		}
		if !detectorIDPattern.MatchString(item.GetId()) {
			return fmt.Errorf("invalid logical detector ID %q", item.GetId())
		}
		if seen[item.GetId()] {
			return fmt.Errorf("duplicate detector ID %q", item.GetId())
		}
		seen[item.GetId()] = true
		if role == HealthJudgeArtifacts && (item.GetId() == "service-endpoints" || strings.HasPrefix(item.GetId(), "traffic-")) {
			return fmt.Errorf("detector %q is operator-owned", item.GetId())
		}
		class := "incident"
		if role == HealthJudgeArtifacts {
			class = "health"
		}
		if item.GetOwner() != string(role) || item.GetDetectorClass() != class || item.GetConstructor() == "" || len(item.GetSources()) == 0 || item.GetOriginatingCommit() == "" || (role == ResponderArtifacts && (item.GetOriginatingIncident() == "" || item.GetFiring() != 1)) {
			return fmt.Errorf("detector %q violates %s ownership or registration", item.GetId(), role)
		}
		foundSource := false
		for _, id := range item.GetPossiblePlaybookIds() {
			if !playbookIDPattern.MatchString(id) {
				return fmt.Errorf("invalid playbook ID %q", id)
			}
		}
		for _, source := range item.GetSources() {
			if err := validateGoSource(source); err != nil {
				return err
			}
			foundSource = foundSource || source.GetName() == "detector.go"
		}
		if !foundSource {
			return fmt.Errorf("detector %q needs detector.go", item.GetId())
		}
	}
	seen = map[string]bool{}
	for _, item := range proposal.GetPlaybooks() {
		if item == nil {
			return errors.New("nil playbook proposal")
		}
		if err := checkID(item.GetId()); err != nil {
			return err
		}
		if err := rejectPhysicalMemoryPath(item.GetMarkdown()); err != nil {
			return fmt.Errorf("playbook %q: %w", item.GetId(), err)
		}
		if item.GetOwner() != "responder" || !playbookIDPattern.MatchString(item.GetFaultClass()) || strings.TrimSpace(item.GetOriginatingIncident()) == "" || strings.ContainsAny(item.GetOriginatingIncident(), "\r\n") || strings.TrimSpace(item.GetMarkdown()) == "" || strings.HasPrefix(strings.TrimSpace(item.GetMarkdown()), "---") {
			return fmt.Errorf("invalid playbook %q", item.GetId())
		}
		if role == HealthJudgeArtifacts && item.GetId() != "health-objective" {
			return fmt.Errorf("health judge cannot author playbook %q", item.GetId())
		}
		for _, script := range item.GetScripts() {
			if script != nil {
				if err := rejectPhysicalMemoryPath(script.GetContent()); err != nil {
					return fmt.Errorf("playbook %q script %q: %w", item.GetId(), script.GetName(), err)
				}
			}
			if script == nil || filepath.Base(script.GetName()) != script.GetName() || !strings.HasSuffix(script.GetName(), ".sh") || strings.TrimSpace(script.GetContent()) == "" {
				return fmt.Errorf("invalid script in playbook %q", item.GetId())
			}
		}
		if program := item.GetProgram(); program != nil {
			if role != ResponderArtifacts || len(item.GetScripts()) != 0 {
				return fmt.Errorf("playbook %q program requires responder ownership and no scripts", item.GetId())
			}
			found := false
			for _, source := range program.GetSources() {
				if err := validateGoSource(source); err != nil {
					return fmt.Errorf("playbook %q: %w", item.GetId(), err)
				}
				name := source.GetName()
				if name != "diagnose.go" && name != "mitigate.go" && !strings.HasSuffix(name, "_test.go") {
					return fmt.Errorf("playbook %q: program source %q must be diagnose.go, mitigate.go, or *_test.go", item.GetId(), name)
				}
				found = found || name == "diagnose.go"
			}
			if !found {
				return fmt.Errorf("playbook %q program needs diagnose.go", item.GetId())
			}
			for _, corpus := range program.GetCorpusCases() {
				if corpus == nil || !playbookIDPattern.MatchString(corpus.GetId()) || corpusIDs[corpus.GetId()] {
					return fmt.Errorf("playbook %q has invalid or duplicate corpus case %q", item.GetId(), corpus.GetId())
				}
				corpusIDs[corpus.GetId()] = true
				for name, value := range map[string]string{"open": corpus.GetOpenJson(), "fixed": corpus.GetFixedJson(), "healthy": corpus.GetHealthyJson(), "recordings": corpus.GetRecordingsJson(), "labels": corpus.GetLabelsJson()} {
					if !json.Valid([]byte(value)) {
						return fmt.Errorf("playbook %q corpus %q has invalid %s JSON", item.GetId(), corpus.GetId(), name)
					}
				}
			}
		}
	}
	for _, library := range proposal.GetLibraries() {
		if len(proposal.GetPlaybooks()) == 0 {
			return errors.New("shared library needs an originating playbook proposal")
		}
		origin := proposal.GetPlaybooks()[0].GetOriginatingIncident()
		for _, playbook := range proposal.GetPlaybooks() {
			if playbook.GetOriginatingIncident() != origin {
				return errors.New("shared libraries in one proposal need one originating incident")
			}
		}
		if library == nil || role != ResponderArtifacts || !safeLibraryPackage(library.GetPackage()) || len(library.GetSources()) == 0 {
			return fmt.Errorf("invalid responder library package %q", library.GetPackage())
		}
		for _, source := range library.GetSources() {
			if err := validateGoSource(source); err != nil {
				return fmt.Errorf("library %q: %w", library.GetPackage(), err)
			}
		}
	}
	for _, id := range append(append([]string{}, proposal.GetDeleteDetectorIds()...), proposal.GetDeletePlaybookIds()...) {
		if !playbookIDPattern.MatchString(id) {
			return fmt.Errorf("invalid deletion ID %q", id)
		}
	}
	if role == HealthJudgeArtifacts && len(proposal.GetDeletePlaybookIds()) != 0 {
		return errors.New("health judge cannot delete playbooks")
	}
	if role == ResponderArtifacts {
		for _, id := range proposal.GetDeletePlaybookIds() {
			if id == "health-objective" {
				return errors.New("health objective playbook is protected")
			}
		}
	}
	if role == ResponderArtifacts && (len(proposal.GetTrafficGenerators()) != 0 || len(proposal.GetTrafficWorkloads()) != 0) {
		return errors.New("responder cannot author traffic")
	}
	for _, item := range proposal.GetTrafficGenerators() {
		if item == nil || item.GetId() != "application" || role != HealthJudgeArtifacts {
			return errors.New("invalid traffic generator ID")
		}
		for _, source := range item.GetSources() {
			if err := validateGoSource(source); err != nil {
				return err
			}
		}
	}
	for _, item := range proposal.GetTrafficWorkloads() {
		if item == nil || !playbookIDPattern.MatchString(item.GetName()) || item.GetApiVersion() != "sdo.dev/v1alpha1" || item.GetKind() != "TrafficWorkload" {
			return errors.New("invalid typed traffic workload")
		}
	}
	return nil
}

func safeLibraryPackage(path string) bool {
	if path == "" || strings.Contains(path, "\\") || filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	for _, part := range strings.Split(path, "/") {
		if !playbookIDPattern.MatchString(part) {
			return false
		}
	}
	return true
}

func validateGoSource(source *contracts.ProposedGoSource) error {
	if source == nil || source.GetName() == "" || filepath.Base(source.GetName()) != source.GetName() || !strings.HasSuffix(source.GetName(), ".go") || len(source.GetContent()) > 256*1024 {
		return errors.New("invalid Go source name or size")
	}
	if err := rejectPhysicalMemoryPath(source.GetContent()); err != nil {
		return fmt.Errorf("Go source %q: %w", source.GetName(), err)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), source.GetName(), source.GetContent(), parser.AllErrors); err != nil {
		return fmt.Errorf("invalid Go source %q: %w", source.GetName(), err)
	}
	return nil
}

func rejectPhysicalMemoryPath(content string) error {
	if strings.Contains(content, ".sdo/") || strings.Contains(content, `.sdo\`) {
		return errors.New("physical operational memory path is forbidden in worker content; use logical artifact IDs")
	}
	return nil
}

func refreshPlaybookIndex(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	lines := []string{"# Operational playbooks", ""}
	for _, entry := range entries {
		if entry.IsDir() {
			if !playbookIDPattern.MatchString(entry.Name()) {
				return fmt.Errorf("invalid playbook directory %q", entry.Name())
			}
			lines = append(lines, fmt.Sprintf("- [%s](%s/README.md)", entry.Name(), entry.Name()))
		}
	}
	return os.WriteFile(filepath.Join(root, "README.md"), []byte(strings.Join(lines, "\n")+"\n"), 0o644)
}

func safeDetectorPackage(value string) bool {
	clean := filepath.Clean(value)
	return clean != "." && !filepath.IsAbs(clean) && clean != ".." && !strings.HasPrefix(clean, ".."+string(filepath.Separator)) && strings.HasPrefix(filepath.ToSlash(clean), "detectors/")
}

func rejectArtifactSymlinks(root string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("operational artifact symlink is forbidden: %s", path)
		}
		return nil
	})
}
