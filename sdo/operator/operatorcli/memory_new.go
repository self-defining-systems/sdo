package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"sdo.dev/operator/memory/layout"
)

const unspecifiedOrigin = "unspecified"

type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

func memoryNew(ctx context.Context, args []string, getenv func(string) string, stdout io.Writer) error {
	flags := flag.NewFlagSet("memory new", flag.ContinueOnError)
	root := flags.String("root", ".", "repository checkout")
	class := flags.String("class", string(layout.ClassIncident), "detector class: incident or health")
	incident := flags.String("incident", getenv(memoryIncidentEnv), "originating incident (default $"+memoryIncidentEnv+")")
	var playbooks stringList
	flags.Var(&playbooks, "playbook", "playbook ID the detector may surface (repeatable)")
	positional, err := parseFlags(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		return errors.New("new takes an artifact kind (playbook or detector) and an ID")
	}
	kind, id := positional[0], positional[1]
	absolute, err := absRoot(*root)
	if err != nil {
		return err
	}
	if *incident == "" {
		*incident = unspecifiedOrigin
	}
	var created []string
	switch kind {
	case "playbook":
		created, err = scaffoldPlaybook(absolute, id, *incident)
	case "detector":
		created, err = scaffoldDetector(ctx, absolute, id, layout.Class(*class), playbooks, *incident)
	default:
		err = fmt.Errorf("unknown artifact kind %q (playbook or detector)", kind)
	}
	if err != nil {
		return err
	}
	for _, path := range created {
		fmt.Fprintln(stdout, path)
	}
	return nil
}

func exists(root, rel string) (bool, error) {
	_, err := os.Lstat(filepath.Join(root, filepath.FromSlash(rel)))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func scaffoldPlaybook(root, id, incident string) ([]string, error) {
	readme, err := layout.PlaybookPath(id)
	if err != nil {
		return nil, err
	}
	if taken, err := exists(root, layout.PlaybookDir(id)); err != nil || taken {
		return nil, errors.Join(err, fmt.Errorf("playbook %s already exists", id))
	}
	source := fmt.Sprintf(`---
schema_version: 1
owner: responder
fault_class: %s
originating_incident: %s
---
# %s

Symptoms: describe how the fault shows up, in terms of roles rather than concrete names.

Diagnosis: state how to confirm the fault on <RESOURCE_NAME>.

Repair: state the minimal change that restores <RESOURCE_NAME>, and how to verify it.
`, layout.PlaybookFaultClass(id), incident, id)
	document, err := layout.ParseDocument([]byte(source))
	if err != nil {
		return nil, err
	}
	if err := layout.WritePlaybook(root, layout.Playbook{ID: id, Document: document}); err != nil {
		return nil, err
	}
	if _, err := layout.WritePlaybookIndex(root); err != nil {
		return nil, err
	}
	return []string{readme, layout.PlaybookIndexFile}, nil
}

func scaffoldDetector(ctx context.Context, root, id string, class layout.Class, playbooks []string, incident string) ([]string, error) {
	if class != layout.ClassIncident && class != layout.ClassHealth {
		return nil, fmt.Errorf("unknown detector class %q (incident or health)", class)
	}
	if !layout.ValidDetectorID(id) {
		return nil, fmt.Errorf("invalid detector ID %q (pattern %s)", id, layout.DetectorIDPattern)
	}
	for _, other := range []layout.Class{layout.ClassIncident, layout.ClassHealth} {
		if taken, err := exists(root, layout.DetectorDir(other, id)); err != nil || taken {
			return nil, errors.Join(err, fmt.Errorf("detector %s already exists", id))
		}
	}
	manifest := layout.DetectorManifest{
		APIVersion: layout.APIVersion, Kind: layout.DetectorManifestKind, SDKVersion: layout.SDKVersion,
		ID: id, Constructor: "New", Class: string(class), Owner: class.Owner(),
		Watches:           []layout.Watch{{APIVersion: "v1", Kind: "Pod"}},
		Interval:          "30s",
		Persistence:       layout.Persistence{Firing: 2, Clearing: 2},
		Batching:          layout.Batching{Severity: "warn", Debounce: "0"},
		PossiblePlaybooks: append([]string{}, playbooks...),
		OriginatingCommit: headCommit(ctx, root),
	}
	if class == layout.ClassIncident {
		manifest.OriginatingIncident = incident
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	pkg := goPackage(id)
	var source, test strings.Builder
	data := detectorTemplateData{Package: pkg, Manifest: manifest}
	if err := detectorSource.Execute(&source, data); err != nil {
		return nil, err
	}
	if err := detectorTest.Execute(&test, data); err != nil {
		return nil, err
	}
	formattedSource, err := format.Source([]byte(source.String()))
	if err != nil {
		return nil, fmt.Errorf("format detector scaffold: %w", err)
	}
	formattedTest, err := format.Source([]byte(test.String()))
	if err != nil {
		return nil, fmt.Errorf("format detector test scaffold: %w", err)
	}
	detector := layout.Detector{Class: class, Manifest: manifest, Files: []layout.File{
		{Name: layout.DetectorFileName, Content: formattedSource},
		{Name: layout.DetectorTestFileName, Content: formattedTest},
	}}
	if err := layout.WriteDetector(root, detector); err != nil {
		return nil, err
	}
	dir := layout.DetectorDir(class, id)
	return []string{layout.DetectorManifestPath(class, id), dir + "/" + layout.DetectorFileName, dir + "/" + layout.DetectorTestFileName}, nil
}

func headCommit(ctx context.Context, root string) string {
	output, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD").Output()
	if commit := strings.TrimSpace(string(output)); err == nil && commit != "" {
		return commit
	}
	return unspecifiedOrigin
}

var nonIdentifier = regexp.MustCompile(`[^a-z0-9]+`)

// goPackage derives a Go package name from a detector ID.
func goPackage(id string) string {
	name := strings.Trim(nonIdentifier.ReplaceAllString(id, "_"), "_")
	if name == "" || name[0] >= '0' && name[0] <= '9' {
		name = "detector_" + name
	}
	return name
}

type detectorTemplateData struct {
	Package  string
	Manifest layout.DetectorManifest
}

func (d detectorTemplateData) ClassConstant() string {
	if d.Manifest.DetectorClass() == layout.ClassHealth {
		return "sdk.DetectorClassHealth"
	}
	return "sdk.DetectorClassIncident"
}

func (d detectorTemplateData) OwnerConstant() string {
	if d.Manifest.DetectorClass() == layout.ClassHealth {
		return "sdk.DetectorOwnerHealthJudge"
	}
	return "sdk.DetectorOwnerResponder"
}

func (d detectorTemplateData) SeverityConstant() string {
	switch d.Manifest.Batching.Severity {
	case "info":
		return "sdk.SeverityInfo"
	case "critical":
		return "sdk.SeverityCritical"
	}
	return "sdk.SeverityWarning"
}

var detectorSource = template.Must(template.New("detector").Parse(`package {{.Package}}

import (
	"context"
	"time"

	"sdo.dev/operator/sdk"
)

type Detector struct{}

func New() sdk.Detector { return Detector{} }

// Spec must agree with manifest.yaml.
func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID:          {{printf "%q" .Manifest.ID}},
		Class:       {{.ClassConstant}},
		Owner:       {{.OwnerConstant}},
		Description: "TODO: describe the fault this detector finds",
		Watches:     []sdk.WatchKind{ {{- range $i, $w := .Manifest.Watches}}{{if $i}}, {{end}}{ {{- printf "APIVersion: %q, Kind: %q" $w.APIVersion $w.Kind}} }{{end -}} },
		Interval:    30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: {{.Manifest.Persistence.Firing}}, Clearing: {{.Manifest.Persistence.Clearing}}},
		Batching:    sdk.BatchingPolicy{Severity: {{.SeverityConstant}}},
		Playbooks:   []string{ {{- range $i, $p := .Manifest.PossiblePlaybooks}}{{if $i}}, {{end}}{{printf "%q" $p}}{{end -}} },
{{- if .Manifest.OriginatingIncident}}
		OriginatingIncident: {{printf "%q" .Manifest.OriginatingIncident}},
{{- end}}
		OriginatingCommit: {{printf "%q" .Manifest.OriginatingCommit}},
	}
}

// Detect must be deterministic: it reads only the snapshot.
func (Detector) Detect(_ context.Context, _ sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}
`))

var detectorTest = template.Must(template.New("test").Parse(`package {{.Package}}

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk/sdktest"
)

func TestDetectorIsQuietOnAnEmptySnapshot(t *testing.T) {
	findings, err := New().Detect(context.Background(), sdktest.Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("unexpected findings: %v", findings)
	}
}
`))
