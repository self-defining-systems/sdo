package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sdo.dev/operator/memory/layout"
)

// ValidationJobRequest is the contract for an isolated validator. A job
// launcher must mount CandidateRoot read-only, with no repository publishing
// credentials, and must enforce egress isolation. The root is broker-owned;
// the worker never receives it.
type ValidationJobRequest struct {
	// HealthyBaseline is selected by shared Check, outside candidate memory.
	HealthyBaseline         string
	CandidateRoot           string
	CandidateTree           string
	ChangedPaths            []string
	ReadOnly                bool
	NoPublishingCredentials bool
	EgressIsolated          bool
}

// ValidationJobReceipt binds independent compilation and source validation to
// the exact candidate tree. A missing isolation attestation fails closed.
type ValidationJobReceipt struct {
	CandidateTree                   string
	Accepted                        bool
	DetectorCompiled                bool
	SourceValidated                 bool
	ReadOnlyMountObserved           bool
	NoPublishingCredentialsObserved bool
	EgressIsolationCanaryPassed     bool
}

type ValidationJobClient interface {
	Validate(context.Context, ValidationJobRequest) (ValidationJobReceipt, error)
}

// JobValidator adapts an isolated job launcher to the broker validation gate.
// A nil launcher or incomplete receipt never authorizes a commit. Candidates
// that change only inert prose, scripts or outcome records skip the Job: the
// static layers already ran in the broker and nothing there is compiled.
func JobValidator(client ValidationJobClient) Validator {
	return func(ctx context.Context, candidate string, paths []string) error {
		if client == nil {
			return errors.New("isolated validator job is not configured")
		}
		if len(paths) > 0 && !layout.RequiresValidatorJob(paths) {
			slog.Info("validator_job_skipped", "reason", "no executable or corpus change", "changed_paths", len(paths))
			return nil
		}
		if _, err := runGit(ctx, candidate, "add", "--all", "--", "."); err != nil {
			return err
		}
		if err := addIgnoredOperationalMemory(ctx, candidate, nil); err != nil {
			return err
		}
		tree, err := gitText(ctx, candidate, "write-tree")
		if err != nil {
			return err
		}
		request := ValidationJobRequest{
			HealthyBaseline: compileBaseline(ctx), CandidateRoot: candidate, CandidateTree: tree, ChangedPaths: append([]string(nil), paths...),
			ReadOnly: true, NoPublishingCredentials: true, EgressIsolated: true,
		}
		receipt, err := client.Validate(ctx, request)
		if err != nil {
			return fmt.Errorf("isolated validator job: %w", err)
		}
		if receipt.CandidateTree != tree || !receipt.Accepted || !receipt.SourceValidated || !receipt.DetectorCompiled ||
			!receipt.ReadOnlyMountObserved || !receipt.NoPublishingCredentialsObserved || !receipt.EgressIsolationCanaryPassed {
			return errors.New("isolated validator receipt is incomplete or bound to a different candidate tree")
		}
		return nil
	}
}

func compileBaseline(ctx context.Context) string {
	baseline, _ := ctx.Value(compileBaselineKey{}).(string)
	return baseline
}
