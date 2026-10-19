package memory

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

type ArtifactRole string

const (
	HealthJudgeArtifacts ArtifactRole = "health_judge"
	ResponderArtifacts   ArtifactRole = "responder"
)

// MaterializeArtifacts is the only edge that maps worker-owned logical artifacts
// to the current repository layout. Broker validation still decides acceptance.
func MaterializeArtifacts(root string, proposal *contracts.OperationalArtifactProposal, role ArtifactRole) error {
	if root == "" || proposal == nil || (role != HealthJudgeArtifacts && role != ResponderArtifacts) {
		return errors.New("checkout, typed artifacts, and known role are required")
	}
	if err := validateArtifactProposal(proposal, role); err != nil {
		return err
	}
	if err := rejectArtifactSymlinks(filepath.Join(root, layout.Dir)); err != nil {
		return err
	}
	detectors, err := layout.ReadDetectors(filepath.Join(root, layout.DiagnosticsDir))
	if err != nil {
		return fmt.Errorf("read accepted detectors: %w", err)
	}
	byID := make(map[string]layout.Detector, len(detectors))
	for _, item := range detectors {
		if _, exists := byID[item.Manifest.ID]; exists {
			return fmt.Errorf("duplicate accepted detector %q", item.Manifest.ID)
		}
		byID[item.Manifest.ID] = item
	}
	for _, id := range proposal.GetDeleteDetectorIds() {
		prior, exists := byID[id]
		if !exists || prior.Manifest.Owner != string(role) {
			return fmt.Errorf("cannot delete detector %q outside %s-owned diagnostics", id, role)
		}
	}
	for _, item := range proposal.GetDetectors() {
		if prior, exists := byID[item.GetId()]; exists && prior.Manifest.Owner != string(role) {
			return fmt.Errorf("cannot replace detector %q outside %s-owned diagnostics", item.GetId(), role)
		}
	}
	for _, id := range proposal.GetDeleteDetectorIds() {
		prior := byID[id]
		if err := layout.RemoveDetector(root, prior.Class, id); err != nil {
			return err
		}
		delete(byID, id)
	}
	class := layout.ClassIncident
	if role == HealthJudgeArtifacts {
		class = layout.ClassHealth
	}
	for _, item := range proposal.GetDetectors() {
		manifest := layout.DetectorManifest{
			APIVersion: layout.APIVersion, Kind: layout.DetectorManifestKind, SDKVersion: layout.SDKVersion,
			ID: item.GetId(), Constructor: item.GetConstructor(), Class: item.GetDetectorClass(), Owner: item.GetOwner(), Interval: item.GetInterval(),
			Persistence:       layout.Persistence{Firing: int(item.GetFiring()), Clearing: int(item.GetClearing())},
			Batching:          layout.Batching{Severity: item.GetSeverity(), Debounce: item.GetDebounce()},
			OriginatingCommit: item.GetOriginatingCommit(), OriginatingIncident: item.GetOriginatingIncident(),
			PossiblePlaybooks: append([]string{}, item.GetPossiblePlaybookIds()...),
		}
		for _, watch := range item.GetWatches() {
			manifest.Watches = append(manifest.Watches, layout.Watch{APIVersion: watch.GetApiVersion(), Kind: watch.GetKind()})
		}
		entry := layout.Detector{Class: class, Manifest: manifest}
		for _, source := range item.GetSources() {
			entry.Files = append(entry.Files, layout.File{Name: source.GetName(), Content: []byte(source.GetContent())})
		}
		if err := layout.WriteDetector(root, entry); err != nil {
			return err
		}
		byID[item.GetId()] = entry
	}
	if role == HealthJudgeArtifacts {
		if err := installGenericHealth(root, byID, proposal.GetTrafficWorkloads()); err != nil {
			return err
		}
	}
	playbookRoot := filepath.Join(root, layout.PlaybooksDir)
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
	}
	for _, generator := range proposal.GetTrafficGenerators() {
		base := filepath.Join(root, layout.TrafficGeneratorsDir)
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
		base := filepath.Join(root, layout.TrafficWorkloadsDir)
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
	checkID := func(id string) error {
		if !layout.ValidPlaybookID(id) {
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
		if !layout.ValidDetectorID(item.GetId()) {
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
			if !layout.ValidPlaybookID(id) {
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
		if item.GetOwner() != "responder" || !layout.ValidPlaybookID(item.GetFaultClass()) || strings.TrimSpace(item.GetOriginatingIncident()) == "" || strings.ContainsAny(item.GetOriginatingIncident(), "\r\n") || strings.TrimSpace(item.GetMarkdown()) == "" || strings.HasPrefix(strings.TrimSpace(item.GetMarkdown()), "---") {
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
	}
	for _, id := range append(append([]string{}, proposal.GetDeleteDetectorIds()...), proposal.GetDeletePlaybookIds()...) {
		if !layout.ValidPlaybookID(id) {
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
		if item == nil || !layout.ValidPlaybookID(item.GetName()) || item.GetApiVersion() != "sdo.dev/v1alpha1" || item.GetKind() != "TrafficWorkload" {
			return errors.New("invalid typed traffic workload")
		}
	}
	return nil
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
	if layout.ContainsPhysicalPath(content) {
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
			if !layout.ValidPlaybookID(entry.Name()) {
				return fmt.Errorf("invalid playbook directory %q", entry.Name())
			}
			lines = append(lines, fmt.Sprintf("- [%s](%s/README.md)", entry.Name(), entry.Name()))
		}
	}
	return os.WriteFile(filepath.Join(root, "README.md"), []byte(strings.Join(lines, "\n")+"\n"), 0o644)
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
