package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
)

// MemoryUsage is the help text of the memory subcommand.
const MemoryUsage = `usage: sdo-operator memory <command> [flags]

commands:
  check [--root .] [--json]
        Run structural, semantic, ownership, build, vet, and detector tests.
        SDO_ACTOR, SDO_PHASE, and SDO_BASE_COMMIT are required. Operator sources
        default to $SDO_OPERATOR_ROOT or /opt/sdo/sdo/operator. Optional recorded
        healthy snapshots come from SDO_HEALTHY_BASELINE. Exit 0 when clean,
        1 for diagnostics, 2 for missing inputs or infrastructure/usage errors.
  ls [--root .] [--kind goal|architecture|playbook|script|detector] [--json]
        List artifacts by logical ID.
  new playbook <id> [--root .] [--incident I]
  new detector <id> [--root .] [--class incident|health] [--playbook ID]... [--incident I]
        Scaffold a minimal artifact that passes "memory check".
  outcomes [--root .] [--json]
        Print the stored outcome records of every incident.`

const (
	memoryActorEnv    = "SDO_ACTOR"
	memoryPhaseEnv    = "SDO_PHASE"
	memoryIncidentEnv = "SDO_INCIDENT_ID"
	memoryOperatorEnv = "SDO_OPERATOR_ROOT"

	exitClean       = 0
	exitDiagnostics = 1
	exitInfra       = 2
)

// RunMemory runs "sdo-operator memory <args>" and returns the process exit
// code. getenv supplies SDO_ACTOR, SDO_PHASE, and the other defaults.
func RunMemory(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, MemoryUsage)
		return exitInfra
	}
	var code int
	var err error
	switch args[0] {
	case "check":
		code, err = memoryCheck(ctx, args[1:], getenv, stdout)
	case "ls":
		err = memoryList(args[1:], stdout)
	case "new":
		err = memoryNew(ctx, args[1:], getenv, stdout)
	case "outcomes":
		err = memoryOutcomes(ctx, args[1:], stdout)
	case "help", "-h", "--help":
		fmt.Fprintln(stdout, MemoryUsage)
		return exitClean
	default:
		err = fmt.Errorf("unknown memory command %q\n%s", args[0], MemoryUsage)
	}
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(stdout, MemoryUsage)
		return exitClean
	}
	if err != nil {
		fmt.Fprintln(stderr, "sdo-operator memory:", err)
		return exitInfra
	}
	return code
}

// parseFlags parses flags that may follow positional arguments, as in
// "new playbook <id> --incident X", and returns the positionals.
func parseFlags(flags *flag.FlagSet, args []string) ([]string, error) {
	flags.SetOutput(io.Discard)
	var positional []string
	for {
		if err := flags.Parse(args); err != nil {
			return nil, err
		}
		args = flags.Args()
		if len(args) == 0 {
			return positional, nil
		}
		positional = append(positional, args[0])
		args = args[1:]
	}
}

func absRoot(root string) (string, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", fmt.Errorf("root: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("root %s is not a directory", absolute)
	}
	return absolute, nil
}

func marshalJSON(message proto.Message) ([]byte, error) {
	return protojson.MarshalOptions{UseProtoNames: true, EmitDefaultValues: true, Multiline: true}.Marshal(message)
}

func writeJSON(stdout io.Writer, message proto.Message) error {
	encoded, err := marshalJSON(message)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "%s\n", encoded)
	return err
}

func memoryCheck(ctx context.Context, args []string, getenv func(string) string, stdout io.Writer) (int, error) {
	flags := flag.NewFlagSet("memory check", flag.ContinueOnError)
	root := flags.String("root", ".", "repository checkout to validate")
	asJSON := flags.Bool("json", false, "print a MemoryCheckReport as ProtoJSON")
	positional, err := parseFlags(flags, args)
	if err != nil {
		return exitInfra, err
	}
	if len(positional) != 0 {
		return exitInfra, fmt.Errorf("check takes no arguments, got %q", positional)
	}
	absolute, err := absRoot(*root)
	if err != nil {
		return exitInfra, err
	}
	actor, phase, base := getenv(memoryActorEnv), getenv(memoryPhaseEnv), getenv("SDO_BASE_COMMIT")
	diagnostics, err := validate.Check(ctx, validate.Request{
		Root: absolute, Base: base, Actor: validate.Actor(actor), Phase: validate.Phase(phase),
		OperatorRoot: operatorRootDefault(getenv), HealthyBaseline: getenv("SDO_HEALTHY_BASELINE"),
	}, validate.CompileCandidate)
	if err != nil {
		return exitInfra, err
	}
	if *asJSON {
		report := &contracts.MemoryCheckReport{Diagnostics: validate.ProtoDiagnostics(diagnostics), Root: absolute, Actor: actor, Phase: phase, Base: base}
		if err := writeJSON(stdout, report); err != nil {
			return exitInfra, err
		}
	} else {
		for _, diagnostic := range diagnostics {
			fmt.Fprintln(stdout, diagnosticLine(diagnostic))
		}
	}
	if len(diagnostics) > 0 {
		return exitDiagnostics, nil
	}
	return exitClean, nil
}

func operatorRootDefault(getenv func(string) string) string {
	if root := getenv(memoryOperatorEnv); root != "" {
		return root
	}
	return "/opt/sdo/sdo/operator"
}

// diagnosticLine formats "path:line: rule: message (hint)", omitting the parts
// a diagnostic does not have.
func diagnosticLine(d validate.Diagnostic) string {
	var line strings.Builder
	if d.Path != "" {
		line.WriteString(d.Path)
		if d.Line > 0 {
			fmt.Fprintf(&line, ":%d", d.Line)
		}
		line.WriteString(": ")
	}
	fmt.Fprintf(&line, "%s: %s", d.Rule, d.Message)
	if d.Hint != "" {
		fmt.Fprintf(&line, " (%s)", d.Hint)
	}
	return line.String()
}

var listKinds = []string{"goal", "architecture", "playbook", "script", "detector"}

func memoryList(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("memory ls", flag.ContinueOnError)
	root := flags.String("root", ".", "repository checkout")
	kind := flags.String("kind", "", "only list this kind: "+strings.Join(listKinds, ", "))
	asJSON := flags.Bool("json", false, "print a MemoryListing as ProtoJSON")
	positional, err := parseFlags(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return fmt.Errorf("ls takes no arguments, got %q", positional)
	}
	if *kind != "" && !containsString(listKinds, *kind) {
		return fmt.Errorf("unknown kind %q (%s)", *kind, strings.Join(listKinds, ", "))
	}
	absolute, err := absRoot(*root)
	if err != nil {
		return err
	}
	tree, err := layout.ReadTree(absolute)
	if err != nil {
		return err
	}
	listing := &contracts.MemoryListing{Artifacts: []*contracts.MemoryArtifact{}}
	add := func(kindName, id, path, class string) {
		if *kind == "" || *kind == kindName {
			listing.Artifacts = append(listing.Artifacts, &contracts.MemoryArtifact{Kind: kindName, Id: id, Path: path, DetectorClass: class})
		}
	}
	if tree.Goal != nil {
		add("goal", "", layout.GoalFile, "")
	}
	if tree.Architecture != nil {
		add("architecture", "", layout.ArchitectureFile, "")
	}
	for _, detector := range tree.Detectors {
		add("detector", detector.Manifest.ID, layout.DetectorManifestPath(detector.Class, detector.Manifest.ID), string(detector.Class))
	}
	for _, playbook := range tree.Playbooks {
		readme, err := layout.PlaybookPath(playbook.ID)
		if err != nil {
			return err
		}
		add("playbook", playbook.ID, readme, "")
		for _, script := range playbook.Scripts {
			add("script", playbook.ID+"/"+script.Name, layout.PlaybookScriptPath(playbook.ID, script.Name), "")
		}
	}
	if *asJSON {
		return writeJSON(stdout, listing)
	}
	for _, artifact := range listing.Artifacts {
		fmt.Fprintf(stdout, "%s\t%s\t%s\n", artifact.Kind, artifact.Id, artifact.Path)
	}
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func memoryOutcomes(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("memory outcomes", flag.ContinueOnError)
	root := flags.String("root", ".", "repository checkout")
	asJSON := flags.Bool("json", false, "print MemoryOutcomes as ProtoJSON")
	positional, err := parseFlags(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return fmt.Errorf("outcomes takes no arguments, got %q", positional)
	}
	absolute, err := absRoot(*root)
	if err != nil {
		return err
	}
	stored, err := memory.OpenReader(absolute).List(ctx)
	if err != nil {
		return err
	}
	outcomes := &contracts.MemoryOutcomes{Outcomes: []*contracts.OutcomeRecord{}}
	for _, outcome := range stored {
		outcomes.Outcomes = append(outcomes.Outcomes, outcome.Record)
	}
	if *asJSON {
		return writeJSON(stdout, outcomes)
	}
	for _, record := range outcomes.Outcomes {
		fmt.Fprintf(stdout, "%s\t%s\n", record.GetIncidentId(), record.GetClassification())
	}
	return nil
}
