// Package layout is the only Go code that knows where operational memory lives
// inside a repository. Everything else addresses memory by logical kind and ID.
//
// Paths here are slash-separated and relative to the repository root, which is
// how Git reports them. Callers join them to a checkout with filepath.Join.
package layout

import (
	"crypto/sha256"
	"fmt"
	"path"
	"regexp"
	"strings"
)

const (
	// Dir is the operational-memory root.
	Dir = ".sdo"

	SchemaVersionFile = Dir + "/schema-version"
	GoalFile          = Dir + "/goal.md"
	ArchitectureFile  = Dir + "/arch.md"
	// LifecycleProvenanceFile is Upkeep/controller validation metadata.
	LifecycleProvenanceFile = Dir + "/lifecycle-provenance.yaml"

	DiagnosticsDir         = Dir + "/diagnostics"
	DiagnosticsGoModFile   = DiagnosticsDir + "/go.mod"
	DiagnosticsGoSumFile   = DiagnosticsDir + "/go.sum"
	DetectorsDir           = DiagnosticsDir + "/detectors"
	TrafficGeneratorsDir   = DiagnosticsDir + "/traffic/generators"
	TrafficWorkloadsDir    = DiagnosticsDir + "/traffic/workloads"
	ManifestFileName       = "manifest.yaml"
	DetectorFileName       = "detector.go"
	DetectorTestFileName   = "detector_test.go"
	PlaybooksDir           = Dir + "/playbooks"
	PlaybookIndexFile      = PlaybooksDir + "/README.md"
	PlaybookFileName       = "README.md"
	PlaybookScriptsDir     = "scripts"
	OutcomesDir            = Dir + "/outcomes"
	LegacyOutcomesFile     = Dir + "/outcomes.jsonl"
	ApplicationGenerator   = "application"
	IncidentGenerator      = "incident"
	IncidentWorkloadPrefix = "incident-"

	// LegacyDetectorManifestFile is the schema-version 1 shared detector index.
	LegacyDetectorManifestFile = DiagnosticsDir + "/" + ManifestFileName

	// CurrentSchemaVersion is the operational-memory schema this package reads
	// and writes. Version 1 kept every detector in LegacyDetectorManifestFile;
	// version 2 keeps one manifest per detector; version 3 refers to playbooks by
	// logical ID only.
	CurrentSchemaVersion = 3
)

// Kind names a class of file in operational memory.
type Kind string

const (
	KindSchemaVersion            Kind = "schema-version"
	KindGoal                     Kind = "goal"
	KindArchitecture             Kind = "architecture"
	KindLifecycleProvenance      Kind = "lifecycle-provenance"
	KindDiagnosticsModule        Kind = "diagnostics-module"
	KindLegacyDetectorManifest   Kind = "legacy-detector-manifest"
	KindHealthDetectorManifest   Kind = "health-detector-manifest"
	KindIncidentDetectorManifest Kind = "incident-detector-manifest"
	KindHealthDetectorSource     Kind = "health-detector-source"
	KindIncidentDetectorSource   Kind = "incident-detector-source"
	KindTrafficGenerator         Kind = "traffic-generator"
	KindTrafficWorkload          Kind = "traffic-workload"
	KindPlaybookIndex            Kind = "playbook-index"
	KindPlaybook                 Kind = "playbook"
	KindPlaybookScript           Kind = "playbook-script"
	KindOutcome                  Kind = "outcome"
	KindLegacyOutcomes           Kind = "legacy-outcomes"
)

// IsDetectorManifest reports whether the kind is a per-detector manifest.
func (k Kind) IsDetectorManifest() bool {
	return k == KindHealthDetectorManifest || k == KindIncidentDetectorManifest
}

// IsDetectorSource reports whether the kind is a file of a detector package.
func (k Kind) IsDetectorSource() bool {
	return k == KindHealthDetectorSource || k == KindIncidentDetectorSource
}

// Class returns the detector class a detector kind belongs to.
func (k Kind) Class() (Class, bool) {
	switch k {
	case KindHealthDetectorManifest, KindHealthDetectorSource:
		return ClassHealth, true
	case KindIncidentDetectorManifest, KindIncidentDetectorSource:
		return ClassIncident, true
	}
	return "", false
}

// Class is a detector class. Its string form is the value of a manifest's
// class field; the owning directory name is Dir.
type Class string

const (
	ClassHealth   Class = "health"
	ClassIncident Class = "incident"
)

// Dir is the directory name below DetectorsDir that holds the class.
func (c Class) Dir() string {
	if c == ClassIncident {
		return "incidents"
	}
	return "health"
}

// Owner is the only actor that may own detectors of the class.
func (c Class) Owner() string {
	if c == ClassIncident {
		return "responder"
	}
	return "health_judge"
}

func (c Class) valid() bool { return c == ClassHealth || c == ClassIncident }

func (c Class) ManifestKind() Kind {
	if c == ClassIncident {
		return KindIncidentDetectorManifest
	}
	return KindHealthDetectorManifest
}

func (c Class) SourceKind() Kind {
	if c == ClassIncident {
		return KindIncidentDetectorSource
	}
	return KindHealthDetectorSource
}

func classOfDir(name string) (Class, bool) {
	switch name {
	case "health":
		return ClassHealth, true
	case "incidents":
		return ClassIncident, true
	}
	return "", false
}

const (
	// PlaybookIDPattern and DetectorIDPattern are the logical ID grammars.
	PlaybookIDPattern = `^[a-z][a-z0-9_-]*$`
	DetectorIDPattern = `^[a-z0-9][a-z0-9_.-]*$`
)

var (
	playbookIDPattern = regexp.MustCompile(PlaybookIDPattern)
	detectorIDPattern = regexp.MustCompile(DetectorIDPattern)
	outcomeFile       = regexp.MustCompile(`^([0-9a-f]{64})\.json$`)
)

// ValidPlaybookID reports whether id is a logical playbook ID.
func ValidPlaybookID(id string) bool { return playbookIDPattern.MatchString(id) }

// ValidDetectorID reports whether id is a logical detector ID.
func ValidDetectorID(id string) bool { return detectorIDPattern.MatchString(id) }

// Classify maps a repository-relative path to the kind of memory file it is
// and its logical ID. Detector files, playbooks, and traffic files carry their
// owner's ID; a playbook script's ID is "<playbook>/<name>"; an outcome's ID is
// its file stem. Paths outside the layout, unsafe paths, and malformed IDs are
// not classified.
func Classify(p string) (Kind, string, bool) {
	if p == "" || path.Clean(p) != p || !strings.HasPrefix(p, Dir+"/") {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(p, Dir+"/"), "/")
	switch parts[0] {
	case "schema-version":
		return only(parts, KindSchemaVersion)
	case "goal.md":
		return only(parts, KindGoal)
	case "arch.md":
		return only(parts, KindArchitecture)
	case "lifecycle-provenance.yaml":
		return only(parts, KindLifecycleProvenance)
	case "outcomes.jsonl":
		return only(parts, KindLegacyOutcomes)
	case "outcomes":
		if len(parts) == 2 {
			if match := outcomeFile.FindStringSubmatch(parts[1]); match != nil {
				return KindOutcome, match[1], true
			}
		}
	case "playbooks":
		return classifyPlaybook(parts[1:])
	case "diagnostics":
		return classifyDiagnostics(parts[1:])
	}
	return "", "", false
}

func only(parts []string, kind Kind) (Kind, string, bool) {
	if len(parts) == 1 {
		return kind, "", true
	}
	return "", "", false
}

func classifyPlaybook(parts []string) (Kind, string, bool) {
	switch {
	case len(parts) == 1 && parts[0] == PlaybookFileName:
		return KindPlaybookIndex, "", true
	case len(parts) == 2 && parts[1] == PlaybookFileName && ValidPlaybookID(parts[0]):
		return KindPlaybook, parts[0], true
	case len(parts) == 3 && parts[1] == PlaybookScriptsDir && ValidPlaybookID(parts[0]) && strings.HasSuffix(parts[2], ".sh") && len(parts[2]) > len(".sh"):
		return KindPlaybookScript, parts[0] + "/" + parts[2], true
	}
	return "", "", false
}

func classifyDiagnostics(parts []string) (Kind, string, bool) {
	switch {
	case len(parts) == 1 && (parts[0] == "go.mod" || parts[0] == "go.sum"):
		return KindDiagnosticsModule, parts[0], true
	case len(parts) == 1 && parts[0] == ManifestFileName:
		return KindLegacyDetectorManifest, "", true
	case len(parts) >= 4 && parts[0] == "detectors":
		class, ok := classOfDir(parts[1])
		if !ok || !ValidDetectorID(parts[2]) {
			return "", "", false
		}
		if len(parts) == 4 && parts[3] == ManifestFileName {
			return class.ManifestKind(), parts[2], true
		}
		return class.SourceKind(), parts[2], true
	case len(parts) >= 3 && parts[0] == "traffic" && parts[1] == "generators":
		if len(parts) == 3 {
			return KindTrafficGenerator, ApplicationGenerator, true
		}
		return KindTrafficGenerator, parts[2], true
	case len(parts) == 3 && parts[0] == "traffic" && parts[1] == "workloads" && strings.HasSuffix(parts[2], ".yaml"):
		return KindTrafficWorkload, strings.TrimSuffix(parts[2], ".yaml"), true
	}
	return "", "", false
}

// IncidentTraffic reports whether a traffic file belongs to incident responders
// rather than the health judge.
func IncidentTraffic(kind Kind, id string) bool {
	switch kind {
	case KindTrafficGenerator:
		return id == IncidentGenerator
	case KindTrafficWorkload:
		return strings.HasPrefix(id, IncidentWorkloadPrefix)
	}
	return false
}

// IsMemoryPath reports whether a repository-relative path is inside
// operational memory, classified or not.
func IsMemoryPath(p string) bool { return strings.HasPrefix(p, Dir+"/") }

// DetectorDir is the repository-relative directory of a detector package.
func DetectorDir(class Class, id string) string {
	return path.Join(DetectorsDir, class.Dir(), id)
}

// DetectorManifestPath is the repository-relative manifest of one detector.
func DetectorManifestPath(class Class, id string) string {
	return path.Join(DetectorDir(class, id), ManifestFileName)
}

// DetectorPackage is a detector package path relative to the diagnostics
// module, which is also its Go import path suffix.
func DetectorPackage(class Class, id string) string {
	return path.Join("detectors", class.Dir(), id)
}

// PlaybookDir is the repository-relative directory of a playbook.
func PlaybookDir(id string) string { return path.Join(PlaybooksDir, id) }

// PlaybookPath is the repository-relative playbook document of a logical ID.
func PlaybookPath(id string) (string, error) {
	if !ValidPlaybookID(id) {
		return "", fmt.Errorf("invalid playbook ID %q", id)
	}
	return path.Join(PlaybookDir(id), PlaybookFileName), nil
}

// OutcomePath is the repository-relative document of an incident's outcome.
func OutcomePath(incidentID string) string {
	return path.Join(OutcomesDir, OutcomeFileName(incidentID))
}

// OutcomeFileName is the file name of an incident's outcome document.
func OutcomeFileName(incidentID string) string {
	return fmt.Sprintf("%x.json", sha256.Sum256([]byte(incidentID)))
}

// PhysicalPathMarker is the substring that marks a physical memory path in text.
const PhysicalPathMarker = Dir + "/"

// ContainsPhysicalPath reports whether text names a physical memory path, which
// worker-visible logical content must not.
func ContainsPhysicalPath(text string) bool {
	return strings.Contains(text, PhysicalPathMarker) || strings.Contains(text, Dir+`\`)
}

var (
	embeddedPlaybookPath = regexp.MustCompile(`\.sdo/playbooks/([a-z][a-z0-9_-]*)/README\.md`)
	embeddedScriptPath   = regexp.MustCompile(`\.sdo/playbooks/([a-z][a-z0-9_-]*)/scripts/([a-z][a-z0-9_-]*\.sh)`)
)

// LogicalText rewrites embedded playbook and script paths to logical
// references and rejects any other physical memory path.
func LogicalText(value string) (string, error) {
	value = embeddedScriptPath.ReplaceAllString(value, "script:$1/$2")
	value = embeddedPlaybookPath.ReplaceAllString(value, "playbook:$1")
	if strings.Contains(value, Dir+"/") {
		return "", fmt.Errorf("physical operational memory path remains in logical worker content")
	}
	return value, nil
}

// PlaybookID translates a physical playbook reference to its logical ID. A
// logical ID is returned unchanged.
func PlaybookID(reference string) (string, error) {
	if ValidPlaybookID(reference) {
		return reference, nil
	}
	if rest, ok := strings.CutPrefix(reference, PlaybooksDir+"/"); ok {
		if id, ok := strings.CutSuffix(rest, "/"+PlaybookFileName); ok && ValidPlaybookID(id) {
			return id, nil
		}
		if id, ok := strings.CutSuffix(rest, ".md"); ok && ValidPlaybookID(id) {
			return id, nil
		}
	}
	return "", fmt.Errorf("invalid playbook reference %q", reference)
}

// PlaybookIDs translates a list of playbook references.
func PlaybookIDs(references []string) ([]string, error) {
	ids := make([]string, 0, len(references))
	for _, reference := range references {
		id, err := PlaybookID(reference)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// PlaybookScriptName removes the physical prefix from a script reference while
// keeping it bound to its logical playbook.
func PlaybookScriptName(playbookID, reference string) (string, error) {
	if !ValidPlaybookID(playbookID) {
		return "", fmt.Errorf("invalid playbook ID %q", playbookID)
	}
	name := reference
	if strings.Contains(reference, "/") {
		prefix := path.Join(PlaybookDir(playbookID), PlaybookScriptsDir) + "/"
		if !strings.HasPrefix(reference, prefix) {
			return "", fmt.Errorf("script %q does not belong to playbook %q", reference, playbookID)
		}
		name = strings.TrimPrefix(reference, prefix)
	}
	if name == "" || strings.Contains(name, "/") || !strings.HasSuffix(name, ".sh") {
		return "", fmt.Errorf("invalid playbook script %q", reference)
	}
	return name, nil
}

// ArtifactID translates a proposed artifact reference (a logical ID, a playbook
// path, or a detector source path) to its logical ID.
func ArtifactID(reference string) (string, error) {
	if id, err := PlaybookID(reference); err == nil {
		return id, nil
	}
	if kind, id, ok := Classify(reference); ok && kind.IsDetectorSource() {
		base := path.Base(reference)
		if base == DetectorFileName || base == DetectorTestFileName {
			return id, nil
		}
	}
	return "", fmt.Errorf("unsupported operational artifact reference %q", reference)
}

// RequiresControllerRollout reports whether accepted operational-memory edits
// change the detector program that a running controller must reload.
func RequiresControllerRollout(changedPaths []string) bool {
	for _, p := range changedPaths {
		if kind, _, ok := Classify(p); ok && (kind == KindLegacyDetectorManifest || kind == KindIncidentDetectorManifest || kind == KindIncidentDetectorSource) {
			return true
		}
	}
	return false
}
