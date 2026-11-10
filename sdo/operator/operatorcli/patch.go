package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/planfile"
)

// PatchUsage documents "sdo-operator patch".
const PatchUsage = `usage: sdo-operator patch <command> [flags]

App files (everything outside .sdo/) are never published. You may edit them in
your checkout as a scratch view, to read, run and test: the checkout already
holds the repository's patch series applied. To keep a change, capture it as a
patch; a publish is rejected (patch.uncaptured) while an app file differs from
what the patches produce. A patch no image build source or deployment input
covers is kept but reported patch.undeployable: it cannot take effect.

  create --reason TEXT [--name SLUG] [--owner deployer|responder] [--incident ID] [--root .] [paths...]
        Capture the scratch edits of the given paths (default: all) as the next
        numbered patch under .sdo/deployment/patches/. Owner defaults to
        SDO_ACTOR and, for a responder, the incident to SDO_INCIDENT_ID. The reason
        (40 to 2000 characters) must say what fails without the patch. The patch
        is written only if the extended series applies to the base tree and
        reproduces your files exactly.
  list [--root .] [--json]
        List the patches in application order with owner, paths and reason.
  check [--root .] [--json]
        Apply the series to the base tree and report patches that do not apply
        (conflict, upstreamed, missing_file, malformed, forbidden) and app files
        no patch reproduces (patch.uncaptured).
Exit 0 when clean, 1 for diagnostics, 2 for usage or infrastructure errors.`

// RunPatch runs "sdo-operator patch <args>" and returns the exit code.
func RunPatch(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if getenv == nil {
		getenv = processio.Environment
	}
	if len(args) == 0 {
		fmt.Fprintln(stderr, PatchUsage)
		return exitInfra
	}
	var code int
	var err error
	switch args[0] {
	case "create":
		code, err = patchCreate(ctx, args[1:], getenv, stdout)
	case "list":
		err = patchList(ctx, args[1:], stdout)
	case "check":
		code, err = patchCheck(ctx, args[1:], stdout)
	case "help", "-h", "--help":
		fmt.Fprintln(stdout, PatchUsage)
		return exitClean
	default:
		err = fmt.Errorf("unknown patch command %q\n%s", args[0], PatchUsage)
	}
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(stdout, PatchUsage)
		return exitClean
	}
	var perr *patchset.PatchError
	if errors.As(err, &perr) {
		fmt.Fprintln(stdout, patchDiagnostic(perr))
		return exitDiagnostics
	}
	if err != nil {
		fmt.Fprintln(stderr, "sdo-operator patch:", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return exitTimeout
		}
		return exitInfra
	}
	return code
}

func patchDiagnostic(e *patchset.PatchError) string {
	var hint string
	switch e.Kind {
	case patchset.KindConflict:
		hint = "the context no longer matches; re-create the patch against the current files"
	case patchset.KindUpstreamed:
		hint = "the change is already in the base; delete this patch"
	case patchset.KindMissingFile:
		hint = "a file the patch changes does not exist in the base tree"
	}
	line := "patch." + string(e.Kind) + ": " + e.Error()
	if hint != "" {
		line += " (" + hint + ")"
	}
	return line
}

func uncapturedLines(paths []string) []string {
	var lines []string
	for _, p := range paths {
		lines = append(lines, fmt.Sprintf("%s: patch.uncaptured: the file differs from what the patches produce (capture it with `sdo-operator patch create --reason ... %s` or revert it)", p, p))
	}
	return lines
}

// undeployableLines is the typed signal for a patch no deployment reads: it
// stays recorded, but the plan has no image build source or renderer input
// covering its files, so it cannot take effect on a running workload. A
// checkout without a plan declares nothing to judge against.
func undeployableLines(ctx context.Context, root string) ([]string, error) {
	plan, err := planfile.Read(root, planCatalogs())
	if errors.Is(err, planfile.ErrAbsent) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	patches, err := patchset.List(ctx, root)
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, p := range patches {
		if unbuilt := planfile.Unbuilt(plan, p.Paths()); len(unbuilt) > 0 {
			lines = append(lines, fmt.Sprintf("%s: patch.undeployable: recorded, but no image build source or deployment input covers %s, so it will not take effect and does not mitigate the incident; use a runtime mitigation", p.Name, strings.Join(unbuilt, ", ")))
		}
	}
	return lines, nil
}

func patchCreate(ctx context.Context, args []string, getenv func(string) string, stdout io.Writer) (int, error) {
	flags := flag.NewFlagSet("patch create", flag.ContinueOnError)
	reason := flags.String("reason", "", "why the patch exists: what fails without it")
	name := flags.String("name", "", "slug after the number (default: from the reason)")
	owner := flags.String("owner", getenv(memoryActorEnv), "deployer or responder")
	incident := flags.String("incident", getenv(memoryIncidentEnv), "incident ID (responder patches)")
	root := flags.String("root", ".", "repository checkout")
	paths, err := parseFlags(flags, args)
	if err != nil {
		return exitInfra, err
	}
	if strings.TrimSpace(*reason) == "" {
		return exitInfra, errors.New("create needs --reason")
	}
	switch patchset.Owner(*owner) {
	case patchset.OwnerDeployer:
		*incident = ""
	case patchset.OwnerResponder:
	default:
		return exitInfra, fmt.Errorf("owner %q must be deployer or responder (set SDO_ACTOR or pass --owner)", *owner)
	}
	absolute, err := absRoot(*root)
	if err != nil {
		return exitInfra, err
	}
	created, err := patchset.Create(ctx, absolute, patchset.CreateRequest{Owner: patchset.Owner(*owner), Incident: *incident, Reason: *reason, Name: *name, Paths: paths})
	if err != nil {
		return exitInfra, err
	}
	fmt.Fprintf(stdout, "created %s/%s capturing %s\n", patchset.Directory, created.Name, strings.Join(created.Paths, ", "))
	report, err := patchset.Check(ctx, absolute)
	if err != nil {
		return exitInfra, err
	}
	if len(report.Uncaptured) > 0 {
		fmt.Fprintf(stdout, "%d scratch file(s) remain uncaptured:\n", len(report.Uncaptured))
		for _, line := range uncapturedLines(report.Uncaptured) {
			fmt.Fprintln(stdout, line)
		}
	}
	lines, err := undeployableLines(ctx, absolute)
	if err != nil {
		return exitInfra, err
	}
	for _, line := range lines {
		fmt.Fprintln(stdout, line)
	}
	return exitClean, nil
}

type patchEntry struct {
	Name     string   `json:"name"`
	Owner    string   `json:"owner"`
	Incident string   `json:"incident,omitempty"`
	Reason   string   `json:"reason"`
	Paths    []string `json:"paths"`
}

func patchList(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("patch list", flag.ContinueOnError)
	root := flags.String("root", ".", "repository checkout")
	asJSON := flags.Bool("json", false, "print JSON")
	rest, err := parseFlags(flags, args)
	if err != nil {
		return err
	}
	if len(rest) != 0 {
		return fmt.Errorf("list takes no arguments, got %q", rest)
	}
	absolute, err := absRoot(*root)
	if err != nil {
		return err
	}
	patches, err := patchset.List(ctx, absolute)
	if err != nil {
		return err
	}
	entries := make([]patchEntry, 0, len(patches))
	for _, p := range patches {
		entries = append(entries, patchEntry{Name: p.Name, Owner: string(p.Header.Owner), Incident: p.Header.Incident, Reason: p.Header.Reason, Paths: p.Paths()})
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(entries)
	}
	if len(entries) == 0 {
		fmt.Fprintln(stdout, "no patches")
	}
	for _, e := range entries {
		fmt.Fprintf(stdout, "%s  %s  %s\n    %s\n", e.Name, e.Owner, strings.Join(e.Paths, ", "), firstLine(e.Reason))
	}
	return nil
}

func firstLine(s string) string {
	first, _, _ := strings.Cut(s, "\n")
	return first
}

func patchCheck(ctx context.Context, args []string, stdout io.Writer) (int, error) {
	flags := flag.NewFlagSet("patch check", flag.ContinueOnError)
	root := flags.String("root", ".", "repository checkout")
	asJSON := flags.Bool("json", false, "print JSON")
	rest, err := parseFlags(flags, args)
	if err != nil {
		return exitInfra, err
	}
	if len(rest) != 0 {
		return exitInfra, fmt.Errorf("check takes no arguments, got %q", rest)
	}
	absolute, err := absRoot(*root)
	if err != nil {
		return exitInfra, err
	}
	report, err := patchset.Check(ctx, absolute)
	if err != nil {
		return exitInfra, err
	}
	undeployable, err := undeployableLines(ctx, absolute)
	if err != nil {
		return exitInfra, err
	}
	if *asJSON {
		if report.Uncaptured == nil {
			report.Uncaptured = []string{}
		}
		err = json.NewEncoder(stdout).Encode(struct {
			Patches    int      `json:"patches"`
			Digest     string   `json:"digest"`
			Effective  string   `json:"effective_tree"`
			Uncaptured []string `json:"uncaptured"`
			// Undeployable is one line per patch that cannot take effect.
			Undeployable []string `json:"undeployable"`
		}{report.Patches, report.Digest, report.Effective, report.Uncaptured, orEmpty(undeployable)})
	} else {
		fmt.Fprintf(stdout, "%d patch(es) apply; effective tree %s\n", report.Patches, report.Effective)
		for _, line := range uncapturedLines(report.Uncaptured) {
			fmt.Fprintln(stdout, line)
		}
		for _, line := range undeployable {
			fmt.Fprintln(stdout, line)
		}
	}
	if err != nil {
		return exitInfra, err
	}
	if len(report.Uncaptured) > 0 {
		return exitDiagnostics, nil
	}
	return exitClean, nil
}

func orEmpty(lines []string) []string {
	if lines == nil {
		return []string{}
	}
	return lines
}
