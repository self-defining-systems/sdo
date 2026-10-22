package memory

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/sdktest"
)

// ProgramLearningEvidence links a newly introduced program to the captured
// incident whose diagnosis corpus the broker independently validated. The trust
// projection additionally requires successful mitigation replay for origin credit;
// an external effect with skipped replay can earn only live repair credits.
// It is derived from immutable accepted Git objects, never worker claims or a
// mutable checkout. It does not claim the program performed the original repair.
type ProgramLearningEvidence struct {
	ProgramID      string
	IncidentID     string
	AcceptedCommit string
}

var acceptedCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

func ReadProgramLearningEvidence(ctx context.Context, repository, commit string) ([]ProgramLearningEvidence, error) {
	if !acceptedCommitPattern.MatchString(commit) {
		return nil, nil
	}
	git := func(args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, "git", append([]string{"-C", repository}, args...)...).Output()
	}
	listing, err := git("ls-tree", "-r", "--name-only", commit, "--", layout.CorpusDir+"/")
	if err != nil {
		return nil, fmt.Errorf("read accepted program learning evidence: %w", err)
	}
	var evidence []ProgramLearningEvidence
	seen := map[string]bool{}
	for _, name := range strings.Split(strings.TrimSpace(string(listing)), "\n") {
		if !strings.HasSuffix(name, "/labels.json") {
			continue
		}
		data, err := git("show", commit+":"+name)
		if err != nil {
			return nil, err
		}
		var labels struct {
			RootCauseProgramIDs    []string                 `json:"root_cause_program_ids"`
			Provenance             sdktest.CorpusProvenance `json:"provenance"`
			SyntheticNoMatchReason string                   `json:"synthetic_no_match_reason"`
		}
		if json.Unmarshal(data, &labels) != nil || labels.Provenance.IncidentID == "" || labels.SyntheticNoMatchReason != "" {
			continue
		}
		originRaw, err := git("log", "-1", "--format=%H", commit, "--", name)
		if err != nil {
			return nil, err
		}
		origin := strings.TrimSpace(string(originRaw))
		message, err := git("show", "-s", "--format=%B", origin)
		if err != nil {
			return nil, err
		}
		body := "\n" + string(message) + "\n"
		valid := true
		for _, trailer := range []string{"SDO-Incident: " + labels.Provenance.IncidentID, "SDO-Actor: responder", "SDO-Phase: reflection", "SDO-Validation: passed"} {
			valid = valid && strings.Contains(body, "\n"+trailer+"\n")
		}
		if !valid {
			continue
		}
		directory := strings.TrimSuffix(name, "labels.json")
		for file, digest := range map[string]string{"open.json": labels.Provenance.OpenSHA256, "fixed.json": labels.Provenance.FixedSHA256} {
			snapshot, err := git("show", origin+":"+directory+file)
			if err != nil || fmt.Sprintf("%x", sha256.Sum256(snapshot)) != digest {
				valid = false
				break
			}
		}
		if !valid || labels.Provenance.OpenSHA256 == labels.Provenance.FixedSHA256 {
			continue
		}
		for _, id := range labels.RootCauseProgramIDs {
			if !layout.ValidPlaybookID(id) || seen[id+"\x00"+labels.Provenance.IncidentID] {
				continue
			}
			programDir := layout.PlaybookDir(id)
			if _, err := git("cat-file", "-e", origin+":"+programDir+"/mitigate.go"); err != nil {
				continue
			}
			if _, err := git("cat-file", "-e", origin+":"+programDir+"/diagnose.go"); err != nil {
				continue
			}
			// Only the introduction earns this credit. Revalidating old captures after
			// edits or adding more case labels cannot manufacture new successes.
			if _, err := git("cat-file", "-e", origin+"^:"+programDir+"/diagnose.go"); err == nil {
				continue
			}
			seen[id+"\x00"+labels.Provenance.IncidentID] = true
			evidence = append(evidence, ProgramLearningEvidence{ProgramID: id, IncidentID: labels.Provenance.IncidentID, AcceptedCommit: origin})
		}
	}
	return evidence, nil
}
