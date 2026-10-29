package broker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
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
		report.Root, report.Base, report.Actor, report.Phase = p.Path, p.BaseCommit, string(Responder), string(ReflectionPhase)
		report.Diagnostics, report.CoverageGaps = validate.ProtoDiagnostics(ds), validate.ProtoDiagnostics(gaps)
		return nil
	})
	report.MitigationReplayNotes = replayNotes
	return report, err
}

func (b *Broker) checkAttestedCandidate(ctx context.Context, root, base string, actor Actor, phase Phase, paths []string, publication *validate.PublicationContext, decision *contracts.ReflectionDecision, gaps *[]validate.Diagnostic) ([]validate.Diagnostic, error) {
	// An empty toolchain identity disables reuse. The launcher supplies only
	// immutable image identities; mutable tags/development mounts fail closed.
	identity, err := b.validationIdentity(ctx, root, base, actor, phase, publication, decision)
	if err != nil {
		return nil, err
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
			return nil, identityErr
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
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "%d:", len(data))
		_, err = h.Write(data)
		return err
	})
	return fmt.Sprintf("%x", h.Sum(nil)), err
}
