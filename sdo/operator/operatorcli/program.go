package operatorcli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type repeatedFlags []string

func (v *repeatedFlags) String() string         { return strings.Join(*v, ",") }
func (v *repeatedFlags) Set(value string) error { *v = append(*v, value); return nil }

type ProgramParam struct{ Name, Kind string }
type ProgramScaffoldConfig struct {
	FaultClass string
	Params     []ProgramParam
	Mitigate   bool
	Out        string
}

type ProgramProposeConfig struct {
	Dir                string
	Libraries          []string
	Corpus             []string
	Incident           string
	App                string
	OperatorRoot       string
	CorpusFromIncident string
	Bindings           []string
	PeerBindings       []string
	CaptureDir         string
}

var programID = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
var paramName = regexp.MustCompile(`^[a-z][a-zA-Z0-9_]*$`)
var paramKind = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*$`)

func ParseProgramScaffold(args []string) (ProgramScaffoldConfig, error) {
	if len(args) < 2 || args[0] != "program" || args[1] != "scaffold" {
		return ProgramScaffoldConfig{}, errors.New("expected program scaffold")
	}
	flags := flag.NewFlagSet("program scaffold", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var params repeatedFlags
	flags.Var(&params, "param", "required name=Kind binding")
	faultClass := flags.String("fault-class", "", "fault class and program ID")
	mitigate := flags.Bool("mitigate", false, "include mitigation skeleton")
	out := flags.String("out", "", "output playbook directory")
	if err := flags.Parse(args[2:]); err != nil {
		return ProgramScaffoldConfig{}, err
	}
	if flags.NArg() != 0 {
		return ProgramScaffoldConfig{}, errors.New("unexpected scaffold arguments")
	}
	config := ProgramScaffoldConfig{FaultClass: *faultClass, Mitigate: *mitigate, Out: *out}
	for _, raw := range params {
		name, kind, ok := strings.Cut(raw, "=")
		if !ok {
			return ProgramScaffoldConfig{}, fmt.Errorf("invalid parameter %q: expected name=Kind", raw)
		}
		config.Params = append(config.Params, ProgramParam{Name: name, Kind: kind})
	}
	return config, nil
}

func RunProgramScaffold(config ProgramScaffoldConfig) error {
	if !programID.MatchString(config.FaultClass) || config.Out == "" || len(config.Params) == 0 {
		return errors.New("scaffold requires a valid fault class, parameter, and output directory")
	}
	names := map[string]bool{}
	for _, param := range config.Params {
		if !paramName.MatchString(param.Name) || !paramKind.MatchString(param.Kind) || names[param.Name] {
			return fmt.Errorf("invalid or duplicate parameter %q=%q", param.Name, param.Kind)
		}
		names[param.Name] = true
	}
	if err := os.Mkdir(config.Out, 0o755); err != nil {
		return fmt.Errorf("create playbook directory: %w", err)
	}
	pkg := strings.ReplaceAll(config.FaultClass, "-", "_")
	var diagnose strings.Builder
	fmt.Fprintf(&diagnose, "package %s\n\nimport (\n\t\"context\"\n\t\"sdo.dev/operator/sdk/program\"\n)\n\ntype Program struct{}\n\nfunc (Program) Spec() program.ProgramSpec {\n\treturn program.ProgramSpec{ID: %q, Params: []program.ParamSpec{\n", pkg, config.FaultClass)
	for _, param := range config.Params {
		fmt.Fprintf(&diagnose, "\t\t{Name: %q, Kind: %q, Required: true},\n", param.Name, param.Kind)
	}
	diagnose.WriteString("\t}}\n}\n\nfunc (Program) Diagnose(_ context.Context, _ program.ReadView, _ program.Bindings) (program.Verdict, error) {\n\treturn program.Verdict{Result: program.Unknown}, nil\n}\n")
	var test strings.Builder
	fmt.Fprintf(&test, "package %s\n\nimport (\n\t\"testing\"\n\t\"sdo.dev/operator/sdk/program\"\n)\n\nfunc TestRequiredBindings(t *testing.T) {\n\tif err := program.ValidateBindings(Program{}.Spec(), nil); err == nil {\n\t\tt.Fatal(\"unbound program accepted\")\n\t}\n}\n", pkg)
	readme := "---\nfault_class: " + config.FaultClass + "\n---\n\n# " + config.FaultClass + "\n\nDescribe the judgement needed after diagnosis.\n"
	files := map[string]string{"README.md": readme, "diagnose.go": diagnose.String(), "diagnose_test.go": test.String()}
	if config.Mitigate {
		files["mitigate.go"] = fmt.Sprintf("package %s\n\nimport (\n\t\"context\"\n\t\"sdo.dev/operator/sdk/program\"\n)\n\nfunc (Program) Mitigate(_ context.Context, _ program.ReadView, _ program.Bindings) (program.Plan, error) {\n\treturn program.Plan{}, nil\n}\n", pkg)
	}
	for name, content := range files {
		if err := writeNew(filepath.Join(config.Out, name), content); err != nil {
			return err
		}
	}
	return nil
}

func writeNew(path, content string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.WriteString(file, content)
	return err
}

func ParseProgramPropose(args []string) (ProgramProposeConfig, error) {
	if len(args) < 2 || args[0] != "program" || args[1] != "propose" {
		return ProgramProposeConfig{}, errors.New("expected program propose")
	}
	flags := flag.NewFlagSet("program propose", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var libs, corpus, bindings, peerBindings repeatedFlags
	flags.Var(&libs, "lib", "shared library directory")
	flags.Var(&corpus, "corpus", "corpus case directory")
	flags.Var(&bindings, "bind", "incident binding name=Kind/namespace/name")
	flags.Var(&peerBindings, "peer-bind", "healthy peer binding name=Kind/namespace/name")
	corpusIncident := flags.String("corpus-from-incident", "", "capture incident ID")
	dir := flags.String("dir", "", "playbook directory")
	incident := flags.String("incident", "", "originating incident ID")
	app := flags.String("app", ".", "accepted application checkout")
	operatorRoot := flags.String("operator-root", "/opt/sdo/sdo/operator", "operator sources")
	if err := flags.Parse(args[2:]); err != nil {
		return ProgramProposeConfig{}, err
	}
	if flags.NArg() != 0 || *dir == "" || *incident == "" {
		return ProgramProposeConfig{}, errors.New("propose requires --dir and --incident")
	}
	return ProgramProposeConfig{Dir: *dir, Libraries: libs, Corpus: corpus, Incident: *incident, App: *app, OperatorRoot: *operatorRoot, CorpusFromIncident: *corpusIncident, Bindings: bindings, PeerBindings: peerBindings}, nil
}

func AssembleProgramProposal(config ProgramProposeConfig) (*contracts.OperationalArtifactProposal, error) {
	readme, err := os.ReadFile(filepath.Join(config.Dir, "README.md"))
	if err != nil {
		return nil, err
	}
	id := filepath.Base(filepath.Clean(config.Dir))
	markdown := string(readme)
	if strings.HasPrefix(markdown, "---\n") {
		if end := strings.Index(markdown[4:], "\n---\n"); end >= 0 {
			for _, line := range strings.Split(markdown[4:4+end], "\n") {
				if value, ok := strings.CutPrefix(line, "fault_class:"); ok {
					id = strings.Trim(strings.TrimSpace(value), "\"'")
				}
			}
			markdown = strings.TrimSpace(markdown[end+9:])
		}
	}
	if !programID.MatchString(id) || strings.TrimSpace(config.Incident) == "" {
		return nil, errors.New("proposal requires a valid fault class and incident ID")
	}
	sources, err := goSources(config.Dir)
	if err != nil {
		return nil, err
	}
	proposal := &contracts.OperationalArtifactProposal{Playbooks: []*contracts.ProposedPlaybook{{Id: id, Owner: "responder", FaultClass: id, OriginatingIncident: config.Incident, Markdown: markdown, Program: &contracts.ProposedProgram{Sources: sources}}}}
	if len(sources) == 0 {
		return nil, errors.New("playbook has no Go sources")
	}
	cases := append([]string{}, config.Corpus...)
	if config.CorpusFromIncident == "" {
		generated, err := filepath.Glob(filepath.Join(config.Dir, "corpus", "*"))
		if err != nil {
			return nil, err
		}
		cases = append(cases, generated...)
	}
	seen := map[string]bool{}
	for _, dir := range cases {
		caseID := filepath.Base(filepath.Clean(dir))
		if !programID.MatchString(caseID) || seen[caseID] {
			return nil, fmt.Errorf("invalid or duplicate corpus case %q", caseID)
		}
		seen[caseID] = true
		values := make([]string, 5)
		for i, name := range []string{"open.json", "fixed.json", "healthy.json", "recordings.json", "labels.json"} {
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				return nil, fmt.Errorf("corpus %s: %w", caseID, err)
			}
			values[i] = string(data)
		}
		proposal.Playbooks[0].Program.CorpusCases = append(proposal.Playbooks[0].Program.CorpusCases, &contracts.ProposedCorpusCase{Id: caseID, OpenJson: values[0], FixedJson: values[1], HealthyJson: values[2], RecordingsJson: values[3], LabelsJson: values[4]})
	}
	if config.CorpusFromIncident != "" {
		captured, err := capturedCorpusCase(config, id)
		if err != nil {
			return nil, err
		}
		proposal.Playbooks[0].Program.CorpusCases = append(proposal.Playbooks[0].Program.CorpusCases, captured)
	}
	for _, dir := range config.Libraries {
		pkg := filepath.Base(filepath.Clean(dir))
		if !programID.MatchString(pkg) {
			return nil, fmt.Errorf("invalid library package %q", pkg)
		}
		sources, err := goSources(dir)
		if err != nil {
			return nil, err
		}
		proposal.Libraries = append(proposal.Libraries, &contracts.ProposedLibrary{Package: pkg, Sources: sources})
	}
	return proposal, nil
}

func capturedCorpusCase(config ProgramProposeConfig, programID string) (*contracts.ProposedCorpusCase, error) {
	if config.CorpusFromIncident != config.Incident {
		return nil, errors.New("captured corpus incident must match the originating incident")
	}
	dir := config.CaptureDir
	if dir == "" {
		dir = os.Getenv("SDO_CAPTURED_CORPUS_DIR")
	}
	if dir == "" {
		dir = "/sdo/session/corpus"
	}
	open, err := os.ReadFile(filepath.Join(dir, "open.json"))
	if err != nil {
		return nil, fmt.Errorf("read captured incident open snapshot: %w", err)
	}
	fixed, err := os.ReadFile(filepath.Join(dir, "fixed.json"))
	if err != nil {
		return nil, fmt.Errorf("read captured incident fixed snapshot: %w", err)
	}
	recordings, err := os.ReadFile(filepath.Join(dir, "recordings.json"))
	if err != nil {
		return nil, fmt.Errorf("read captured incident recordings: %w", err)
	}
	var openView, fixedView sdktest.Snapshot
	if err := json.Unmarshal(open, &openView); err != nil {
		return nil, fmt.Errorf("decode captured open snapshot: %w", err)
	}
	if err := json.Unmarshal(fixed, &fixedView); err != nil {
		return nil, fmt.Errorf("decode captured fixed snapshot: %w", err)
	}
	if openView.NamespaceName == "" || openView.NamespaceName != fixedView.NamespaceName {
		return nil, errors.New("captured snapshots must share an application namespace")
	}
	bindings, err := parseProgramBindings(config.Bindings, openView)
	if err != nil {
		return nil, fmt.Errorf("incident bindings: %w", err)
	}
	peer, err := parseProgramBindings(config.PeerBindings, fixedView)
	if err != nil {
		return nil, fmt.Errorf("peer bindings: %w", err)
	}
	if len(bindings) == 0 || len(bindings) != len(peer) {
		return nil, errors.New("captured corpus requires incident and same-kind peer bindings")
	}
	for name, ref := range bindings {
		if peer[name].Kind != ref.Kind {
			return nil, fmt.Errorf("peer binding %q must have kind %s", name, ref.Kind)
		}
	}
	labels, err := json.Marshal(struct {
		RootCauseProgramIDs []string         `json:"root_cause_program_ids"`
		Bindings            program.Bindings `json:"bindings"`
		PeerBindings        program.Bindings `json:"peer_bindings"`
		Provenance          struct {
			IncidentID  string `json:"incident_id"`
			OpenSHA256  string `json:"open_sha256"`
			FixedSHA256 string `json:"fixed_sha256"`
		} `json:"provenance"`
	}{RootCauseProgramIDs: []string{programID}, Bindings: bindings, PeerBindings: peer, Provenance: struct {
		IncidentID  string `json:"incident_id"`
		OpenSHA256  string `json:"open_sha256"`
		FixedSHA256 string `json:"fixed_sha256"`
	}{config.CorpusFromIncident, fmt.Sprintf("%x", sha256.Sum256(open)), fmt.Sprintf("%x", sha256.Sum256(fixed))}})
	if err != nil {
		return nil, err
	}
	caseID := programID + "-" + fmt.Sprintf("%x", sha256.Sum256([]byte(config.CorpusFromIncident)))[:8]
	return &contracts.ProposedCorpusCase{Id: caseID, OpenJson: string(open), FixedJson: string(fixed), HealthyJson: string(fixed), RecordingsJson: string(recordings), LabelsJson: string(labels)}, nil
}

func parseProgramBindings(values []string, snapshot sdktest.Snapshot) (program.Bindings, error) {
	bindings := make(program.Bindings)
	for _, value := range values {
		name, object, ok := strings.Cut(value, "=")
		parts := strings.Split(object, "/")
		if !ok || !paramName.MatchString(name) || len(parts) != 3 || parts[1] != snapshot.NamespaceName || parts[2] == "" || bindings[name].Kind != "" {
			return nil, fmt.Errorf("invalid binding %q; use name=Kind/namespace/name", value)
		}
		ref := sdk.ObjectRef{Kind: parts[0], Namespace: parts[1], Name: parts[2]}
		if !snapshotHasRef(snapshot, ref) {
			return nil, fmt.Errorf("binding %q has no object in captured snapshot", value)
		}
		bindings[name] = ref
	}
	return bindings, nil
}

func snapshotHasRef(snapshot sdktest.Snapshot, ref sdk.ObjectRef) bool {
	switch ref.Kind {
	case "Service":
		for _, object := range snapshot.Services() {
			if object.Namespace == ref.Namespace && object.Name == ref.Name {
				return true
			}
		}
	case "Pod":
		for _, object := range snapshot.Pods() {
			if object.Namespace == ref.Namespace && object.Name == ref.Name {
				return true
			}
		}
	case "Deployment":
		for _, object := range snapshot.Deployments() {
			if object.Namespace == ref.Namespace && object.Name == ref.Name {
				return true
			}
		}
	case "ConfigMap":
		for _, object := range snapshot.ConfigMaps() {
			if object.Namespace == ref.Namespace && object.Name == ref.Name {
				return true
			}
		}
	case "ReplicaSet":
		for _, object := range snapshot.ReplicaSets() {
			if object.Namespace == ref.Namespace && object.Name == ref.Name {
				return true
			}
		}
	case "Endpoints":
		for _, object := range snapshot.Endpoints() {
			if object.Namespace == ref.Namespace && object.Name == ref.Name {
				return true
			}
		}
	case "EndpointSlice":
		for _, object := range snapshot.EndpointSlices() {
			if object.Namespace == ref.Namespace && object.Name == ref.Name {
				return true
			}
		}
	case "NetworkPolicy":
		for _, object := range snapshot.NetworkPolicies() {
			if object.Namespace == ref.Namespace && object.Name == ref.Name {
				return true
			}
		}
	}
	return false
}

func goSources(dir string) ([]*contracts.ProposedGoSource, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	sources := make([]*contracts.ProposedGoSource, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		formatted, err := format.Source(data)
		if err != nil {
			return nil, fmt.Errorf("format Go source %s: %w", path, err)
		}
		sources = append(sources, &contracts.ProposedGoSource{Name: filepath.Base(path), Content: string(formatted)})
	}
	return sources, nil
}

func RunProgramPropose(ctx context.Context, config ProgramProposeConfig, output io.Writer) error {
	proposal, err := AssembleProgramProposal(config)
	if err != nil {
		return err
	}
	data, err := protojson.MarshalOptions{Indent: "  "}.Marshal(proposal)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp("", "sdo-program-proposal-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	app := config.App
	if app == "" {
		app = "."
	}
	root := config.OperatorRoot
	if root == "" {
		root = "/opt/sdo/sdo/operator"
	}
	if err := RunDraftTest(ctx, DraftTestConfig{ApplicationRoot: app, OperatorRoot: root, ProposalJSON: file.Name()}); err != nil {
		return fmt.Errorf("program draft-test: %w", err)
	}
	_, err = output.Write(append(data, '\n'))
	return err
}
