package memory

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

var captureDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ResolveCorpusReference copies verified controller captures into a corpus case.
func ResolveCorpusReference(captureDir, incidentID string, corpus *contracts.ProposedCorpusCase) (map[string][]byte, error) {
	ref := corpus.GetReference()
	if ref == nil {
		return nil, fmt.Errorf("corpus %s lacks a capture reference", corpus.GetId())
	}
	if ref.GetIncidentId() == "" || ref.GetIncidentId() != incidentID {
		return nil, fmt.Errorf("unknown or mismatched captured incident %q", ref.GetIncidentId())
	}
	label := ref.GetCaptureLabel()
	if label != "open" && label != "open@detect" && label != "open@settle" && label != "open@pre-apply" {
		return nil, fmt.Errorf("invalid capture label %q", label)
	}
	if !captureDigest.MatchString(ref.GetOpenSha256()) || !captureDigest.MatchString(ref.GetFixedSha256()) || !captureDigest.MatchString(ref.GetRecordingsSha256()) {
		return nil, fmt.Errorf("corpus %s has invalid capture digest", corpus.GetId())
	}
	recordingName := "recordings.json"
	if label != "open" {
		recordingName = label + "-recordings.json"
	}
	files := map[string][]byte{}
	for _, item := range []struct{ target, source, digest string }{
		{"open.json", label + ".json", ref.GetOpenSha256()},
		{"fixed.json", "fixed.json", ref.GetFixedSha256()},
		{"recordings.json", recordingName, ref.GetRecordingsSha256()},
	} {
		data, err := os.ReadFile(filepath.Join(captureDir, item.source))
		if err != nil {
			return nil, fmt.Errorf("read incident %s capture %s: %w", incidentID, item.source, err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(data)) != item.digest {
			return nil, fmt.Errorf("incident %s capture %s digest mismatch", incidentID, item.source)
		}
		files[item.target] = data
	}
	files["healthy.json"] = files["fixed.json"]
	bindings := func(input []*contracts.CorpusBinding) program.Bindings {
		result := make(program.Bindings, len(input))
		for _, binding := range input {
			object := binding.GetObject()
			result[binding.GetName()] = sdk.ObjectRef{APIVersion: object.GetApiVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
		}
		return result
	}
	openAt := strings.TrimPrefix(label, "open@")
	if label == "open" {
		openAt = ""
	}
	labels, err := json.Marshal(struct {
		RootCauseProgramIDs []string         `json:"root_cause_program_ids,omitempty"`
		NoMatchProgramIDs   []string         `json:"no_match_program_ids,omitempty"`
		Bindings            program.Bindings `json:"bindings"`
		PeerBindings        program.Bindings `json:"peer_bindings"`
		Provenance          struct {
			IncidentID  string `json:"incident_id"`
			OpenAt      string `json:"open_at,omitempty"`
			OpenSHA256  string `json:"open_sha256"`
			FixedSHA256 string `json:"fixed_sha256"`
		} `json:"provenance"`
	}{RootCauseProgramIDs: ref.GetRootCauseProgramIds(), NoMatchProgramIDs: ref.GetNoMatchProgramIds(), Bindings: bindings(ref.GetBindings()), PeerBindings: bindings(ref.GetPeerBindings()), Provenance: struct {
		IncidentID  string `json:"incident_id"`
		OpenAt      string `json:"open_at,omitempty"`
		OpenSHA256  string `json:"open_sha256"`
		FixedSHA256 string `json:"fixed_sha256"`
	}{incidentID, openAt, ref.GetOpenSha256(), ref.GetFixedSha256()}})
	if err != nil {
		return nil, err
	}
	files["labels.json"] = labels
	return files, nil
}
