package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

// DraftTestConfig identifies an agent's typed artifact draft and its accepted
// application checkout. The draft is compiled in a disposable clone.
type DraftTestConfig struct {
	ApplicationRoot string
	OperatorRoot    string
	ProposalJSON    string
}

func ParseDraftTestLaunch(args []string) (DraftTestConfig, error) {
	if len(args) == 0 || args[0] != "draft-test" {
		return DraftTestConfig{}, errors.New("expected draft-test subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator draft-test", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	app := flags.String("app", "", "accepted application checkout")
	operatorRoot := flags.String("operator-root", "/opt/sdo/sdo/operator", "baked operator sources")
	proposalJSON := flags.String("proposal-json", "", "typed OperationalArtifactProposal ProtoJSON file")
	if err := flags.Parse(args[1:]); err != nil {
		return DraftTestConfig{}, err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*app) == "" || strings.TrimSpace(*proposalJSON) == "" {
		return DraftTestConfig{}, errors.New("draft-test requires application checkout and typed proposal JSON")
	}
	return DraftTestConfig{ApplicationRoot: *app, OperatorRoot: *operatorRoot, ProposalJSON: *proposalJSON}, nil
}

// RunDraftTest gives an agent local compiler feedback without publishing or
// changing accepted operational memory. Broker validation remains authoritative.
func RunDraftTest(ctx context.Context, config DraftTestConfig) error {
	if config.ApplicationRoot == "" || config.OperatorRoot == "" || config.ProposalJSON == "" {
		return errors.New("application, operator, and typed draft are required")
	}
	identity, err := builder.SchemaIdentity(config.OperatorRoot)
	if err != nil {
		return err
	}
	if identity != sdkSchemaIdentity {
		return fmt.Errorf("stale draft validator: it embeds schema %q but this operator expects %q", identity, sdkSchemaIdentity)
	}
	info, err := os.Stat(config.ProposalJSON)
	if err != nil {
		return fmt.Errorf("read typed draft: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return errors.New("typed draft must be a regular JSON file of at most 1 MiB")
	}
	data, err := os.ReadFile(config.ProposalJSON)
	if err != nil {
		return fmt.Errorf("read typed draft: %w", err)
	}
	proposal := &contracts.OperationalArtifactProposal{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, proposal); err != nil {
		return fmt.Errorf("decode typed artifact draft: %w", err)
	}
	applyArtifactSchemaDefaults(proposal)
	app, err := filepath.Abs(config.ApplicationRoot)
	if err != nil {
		return err
	}
	if err := memory.RequireCleanProposalCheckout(ctx, app); err != nil {
		return fmt.Errorf("draft baseline is not an accepted checkout: %w", err)
	}
	commit, err := gitDraft(ctx, app, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	parent, err := os.MkdirTemp("", "sdo-artifact-draft-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(parent)
	scratch := filepath.Join(parent, "application")
	command := exec.CommandContext(ctx, "git", "clone", "--quiet", "--no-hardlinks", "--no-checkout", "--", app, scratch)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("clone draft baseline: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if _, err := gitDraft(ctx, scratch, "checkout", "--quiet", "--detach", commit); err != nil {
		return err
	}
	if err := memory.MaterializeArtifacts(scratch, proposal, memory.ResponderArtifacts); err != nil {
		return fmt.Errorf("materialize typed artifact draft: %w", err)
	}
	if err := broker.ValidateNewProgramCorpus(scratch, app); err != nil {
		return fmt.Errorf("validate draft corpus provenance: %w", err)
	}
	if err := broker.ValidateMemoryTree(ctx, scratch); err != nil {
		return fmt.Errorf("validate typed artifact draft: %w", err)
	}
	diagnostics, err := memory.CandidateDiagnostics(scratch)
	if err != nil {
		return err
	}
	workspace, err := builder.BuildWorkspace(ctx, builder.Config{ApplicationRoot: scratch, DiagnosticsRoot: diagnostics, OperatorRoot: config.OperatorRoot})
	if err != nil {
		return fmt.Errorf("build typed artifact draft: %w", err)
	}
	defer workspace.Close()
	if _, err := workspace.ValidateAndBuild(ctx); err != nil {
		return fmt.Errorf("compile typed artifact draft: %w", err)
	}
	return nil
}

func gitDraft(ctx context.Context, root string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

// applyArtifactSchemaDefaults fills the fields that the agent-facing structured artifact schema
// (sdo/agents/views/artifact_proposal.py) defaults, so a hand-written ProtoJSON draft is checked exactly as the
// structured result the agent returns will be. Identity and provenance fields have no default.
func applyArtifactSchemaDefaults(proposal *contracts.OperationalArtifactProposal) {
	for _, detector := range proposal.GetDetectors() {
		if detector == nil {
			continue
		}
		defaultString(&detector.DetectorClass, "incident")
		defaultString(&detector.Owner, "responder")
		defaultString(&detector.Constructor, "New")
		defaultString(&detector.Interval, "30s")
		defaultString(&detector.Severity, "warn")
		defaultString(&detector.Debounce, "500ms")
		if detector.Firing == 0 {
			detector.Firing = 1
		}
		if detector.Clearing == 0 {
			detector.Clearing = 2
		}
	}
	for _, playbook := range proposal.GetPlaybooks() {
		if playbook != nil {
			defaultString(&playbook.Owner, "responder")
		}
	}
}

func defaultString(field *string, value string) {
	if *field == "" {
		*field = value
	}
}
