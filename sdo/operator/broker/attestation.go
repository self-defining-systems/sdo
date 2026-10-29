package broker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"encoding/hex"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

// validationAttestation never leaves the broker. Worker-visible reports are
// advisory: only a successful Check on a broker-private frozen tree can mint
// this record. Restart discards it and safely repeats validation.
type validationAttestation struct {
	identity    string
	gaps        []validate.Diagnostic
	replayNotes []*contracts.MitigationReplayNote
}

// CheckReflection is the trusted self-check endpoint. It freezes the proposal,
// runs the very same Check and isolated compile transport used by publication,
// and retains a content-addressed attestation outside operational memory.
func (b *Broker) CheckReflection(ctx context.Context, incidentID string, decision *contracts.ReflectionDecision) (*contracts.MemoryCheckReport, error) {
	report := &contracts.MemoryCheckReport{}
	var replayNotes []*contracts.MitigationReplayNote
	ctx = context.WithValue(ctx, replayNotesKey{}, &replayNotes)
	err := b.locked(func() error {
		p, err := b.proposal(incidentID, ReflectionPhase)
		if err != nil {
			return err
		}
		report.Root, report.Base, report.Actor, report.Phase = p.Path, p.BaseCommit, string(Responder), string(ReflectionPhase)
		paths, err := validate.ChangedPaths(ctx, p.Path, p.BaseCommit)
		if err != nil {
			return err
		}
		if _, err = validate.NormalizeAuthoredWhitespace(p.Path, paths); err != nil {
			return err
		}
		if _, err = validate.NormalizeAuthoredPlaybooks(p.Path, paths); err != nil {
			return err
		}
		if _, err = b.normalizeCorpus(p.Path, paths); err != nil {
			return err
		}
		paths, err = validate.ChangedPaths(ctx, p.Path, p.BaseCommit)
		if err != nil {
			return err
		}
		publication := &validate.PublicationContext{BaseCommit: p.BaseCommit, IncidentID: incidentID}
		// Reject filesystem/ownership errors before copying any proposed file.
		ds, err := b.checkCandidate(ctx, p.Path, p.BaseCommit, Responder, ReflectionPhase, paths, func(context.Context, string, []string) error { return nil }, publication, decision, nil)
		if err != nil {
			return err
		}
		if len(ds) != 0 {
			report.Diagnostics = validate.ProtoDiagnostics(ds)
			return nil
		}
		stage, cleanup, err := b.privateClone(ctx)
		if err != nil {
			return err
		}
		defer cleanup()
		if _, err := runGit(ctx, stage, "checkout", "--quiet", "--detach", p.BaseCommit); err != nil {
			return err
		}
		for _, path := range paths {
			if err := copyChange(p.Path, stage, path); err != nil {
				return err
			}
		}
		var gaps []validate.Diagnostic
		ds, err = b.checkAttestedCandidate(ctx, stage, p.BaseCommit, Responder, ReflectionPhase, paths, publication, decision, &gaps)
		if err != nil {
			return err
		}
		report.Diagnostics, report.CoverageGaps = validate.ProtoDiagnostics(ds), validate.ProtoDiagnostics(gaps)
		return nil
	})
	report.MitigationReplayNotes = replayNotes
	return report, err
}

func (b *Broker) checkAttestedCandidate(ctx context.Context, root, base string, actor Actor, phase Phase, paths []string, publication *validate.PublicationContext, decision *contracts.ReflectionDecision, gaps *[]validate.Diagnostic) ([]validate.Diagnostic, error) {
	// Captures are frozen too: an ABA write to a live capture directory
	// cannot make validation consume bytes different from its attestation.
	if b.ValidationToolchain != "" && phase == ReflectionPhase && b.codePath != "shell" {
		frozen, cleanup, err := b.freezeCaptureSource(ctx)
		if err != nil {
			slog.Info("reflection_attestation_skipped", "reason", "capture_snapshot_unavailable", "error", err)
			return b.checkCandidate(ctx, root, base, actor, phase, paths, b.validator, publication, decision, gaps)
		}
		defer cleanup()
		ctx = frozen
	}
	// An empty toolchain identity disables reuse. The launcher supplies only
	// immutable image identities; mutable tags/development mounts fail closed.
	identity, err := b.validationIdentity(ctx, root, base, actor, phase, publication, decision)
	if err != nil {
		slog.Info("reflection_attestation_skipped", "reason", "input_identity_unavailable", "error", err)
		return b.checkCandidate(ctx, root, base, actor, phase, paths, b.validator, publication, decision, gaps)
	}
	id := publication.IncidentID
	if attestation, ok := b.attestations[id]; identity != "" && ok && attestation.identity == identity {
		if gaps != nil {
			*gaps = append(*gaps, attestation.gaps...)
		}
		if collected, ok := ctx.Value(replayNotesKey{}).(*[]*contracts.MitigationReplayNote); ok {
			*collected = append(*collected, attestation.replayNotes...)
		}
		return nil, nil
	}
	var notes []validate.Diagnostic
	var replayNotes []*contracts.MitigationReplayNote
	checkCtx := context.WithValue(ctx, replayNotesKey{}, &replayNotes)
	ds, err := b.checkCandidate(checkCtx, root, base, actor, phase, paths, b.validator, publication, decision, &notes)
	if gaps != nil {
		*gaps = append(*gaps, notes...)
	}
	if collected, ok := ctx.Value(replayNotesKey{}).(*[]*contracts.MitigationReplayNote); ok {
		*collected = append(*collected, replayNotes...)
	}
	if err == nil && len(ds) == 0 && identity != "" {
		after, identityErr := b.validationIdentity(ctx, root, base, actor, phase, publication, decision)
		if identityErr != nil {
			slog.Info("reflection_attestation_skipped", "reason", "checked_identity_unavailable", "error", identityErr)
			return ds, nil
		}
		// Check can prepare generic policy files. Never attest inputs different
		// from those selected at the start, including a concurrently changed capture.
		if after == identity {
			if b.attestations == nil {
				b.attestations = map[string]validationAttestation{}
			}
			b.attestations[id] = validationAttestation{identity: identity, gaps: append([]validate.Diagnostic(nil), notes...), replayNotes: replayNotes}
		}
	}
	return ds, err
}

func (b *Broker) validationIdentity(ctx context.Context, root, base string, actor Actor, phase Phase, publication *validate.PublicationContext, decision *contracts.ReflectionDecision) (string, error) {
	if b.ValidationToolchain == "" || phase != ReflectionPhase || b.codePath == "shell" {
		return "", nil
	}
	tree, err := ProposalTreeDigest(ctx, root)
	if err != nil {
		return "", err
	}
	capture, incident := validate.CaptureSource(ctx)
	captures, err := captureDigest(capture)
	if err != nil {
		return "", err
	}
	// Usage/narration do not affect validation. Match exactly the decision the
	// shared Check consumes, including controller-owned failure fields.
	var decisionBytes []byte
	if decision != nil {
		cloned := proto.Clone(decision).(*contracts.ReflectionDecision)
		cloned.Usage = nil
		decisionBytes, err = (proto.MarshalOptions{Deterministic: true}).Marshal(cloned)
		if err != nil {
			return "", err
		}
	}
	request := b.validationRequest("", base, actor, phase, publication, nil, nil)
	bound := struct {
		Tree, Toolchain, Captures, CaptureIncident string
		Request                                    validate.Request
		Decision                                   string
	}{tree, b.ValidationToolchain, captures, incident, request, hex.EncodeToString(decisionBytes)}
	data, err := json.Marshal(bound)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

// Captures are independently provisioned validator inputs, not candidate files.
// Bind names, modes and bytes; reject symlinks rather than following them.
func captureDigest(root string) (string, error) {
	h := sha256.New()
	if root == "" {
		return "", nil
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if os.IsNotExist(err) && path == root {
			fmt.Fprint(h, "missing")
			return nil
		}
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "%d:%s:%v:", len(relative), relative, info.Mode())
		if entry.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("validation capture is not a regular file: %s", relative)
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		fmt.Fprintf(h, "%d:", info.Size())
		_, err = io.Copy(h, file)
		return err
	})
	return fmt.Sprintf("%x", h.Sum(nil)), err
}

// freezeCaptureSource gives Check an immutable broker-private copy. It uses
// canonical directory permissions; original file bytes and modes are retained.
func (b *Broker) freezeCaptureSource(ctx context.Context) (context.Context, func(), error) {
	source, incident := validate.CaptureSource(ctx)
	noop := func() {}
	if source == "" {
		return ctx, noop, nil
	}
	info, err := os.Lstat(source)
	missing := os.IsNotExist(err)
	if err != nil && !missing {
		return ctx, noop, err
	}
	if !missing && !info.IsDir() {
		return ctx, noop, fmt.Errorf("capture source must be a directory")
	}
	parent, err := os.MkdirTemp(b.workspaceRoot, ".validation-captures-")
	if err != nil {
		return ctx, noop, err
	}
	cleanup := func() { _ = os.RemoveAll(parent) }
	destination := filepath.Join(parent, "capture")
	if missing {
		return validate.WithCaptureSource(ctx, destination, incident), cleanup, nil
	}
	err = filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("validation capture is not a regular file: %s", relative)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
		if err != nil {
			return err
		}
		if err := output.Chmod(info.Mode().Perm()); err != nil {
			output.Close()
			return err
		}
		_, copyErr := io.Copy(output, input)
		closeErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if err != nil {
		cleanup()
		return ctx, noop, err
	}
	return validate.WithCaptureSource(ctx, destination, incident), cleanup, nil
}
