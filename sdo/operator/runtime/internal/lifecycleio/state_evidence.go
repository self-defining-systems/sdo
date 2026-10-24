package lifecycleio

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

const StateEvidenceAnnotation = "sdo.dev/state-evidence-sha256"
const inlineStateEvidenceBytes = 32 << 10

// stageEvidence moves verbose incident records to the repository volume. The
// digest is committed with the state CAS, so a lost reply or stale writer cannot
// replace evidence referenced by a different durable revision. Archives are
// retained for offline analysis and must not be removed with proposal checkouts.
func (s *ConfigMapStateStore) stageEvidence(state *contracts.ControllerState) (*contracts.ControllerState, string, error) {
	if s.evidenceRoot == "" {
		return state, "", nil
	}
	evidence := &contracts.ControllerState{Version: state.Version, IncidentRequest: state.IncidentRequest, IncidentResult: state.IncidentResult, PendingClosure: state.PendingClosure}
	payload, err := wire.Canonical(evidence)
	if err != nil {
		return nil, "", err
	}
	if len(payload) <= inlineStateEvidenceBytes {
		return state, "", nil
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(payload))
	if err := s.writeEvidence(digest, payload); err != nil {
		return nil, "", fmt.Errorf("archive controller evidence: %w", err)
	}
	document := proto.Clone(state).(*contracts.ControllerState)
	compactRequest := func(request *contracts.IncidentRequest) {
		if request != nil {
			request.InitialProgramRuns = nil
			request.PreparedAgentTask = ""
		}
	}
	compactResult := func(result *contracts.IncidentResult) {
		if result != nil {
			result.ProgramRuns = nil
			result.ProgramToolCalls = nil
		}
	}
	compactRequest(document.IncidentRequest)
	compactResult(document.IncidentResult)
	if closure := document.PendingClosure; closure != nil {
		compactRequest(closure.Request)
		compactResult(closure.Result)
	}
	return document, digest, nil
}

func (s *ConfigMapStateStore) writeEvidence(digest string, payload []byte) error {
	if err := os.MkdirAll(s.evidenceRoot, 0700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(s.evidenceRoot, ".evidence-")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	if _, err := temp.Write(payload); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	path := filepath.Join(s.evidenceRoot, digest+".json")
	if err := os.Link(temp.Name(), path); err != nil {
		if !os.IsExist(err) {
			return err
		}
		existing, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(existing) != string(payload) {
			return fmt.Errorf("controller evidence digest collision or corruption")
		}
	}
	dir, err := os.Open(s.evidenceRoot)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *ConfigMapStateStore) restoreEvidence(state *contracts.ControllerState, digest string) error {
	if digest == "" {
		return nil
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || s.evidenceRoot == "" {
		return fmt.Errorf("controller evidence requires its archive root and a SHA-256 reference")
	}
	payload, err := os.ReadFile(filepath.Join(s.evidenceRoot, digest+".json"))
	if err != nil {
		return fmt.Errorf("read controller evidence %s: %w", digest, err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(payload)) != digest {
		return fmt.Errorf("controller evidence digest mismatch")
	}
	evidence := &contracts.ControllerState{}
	if err := wire.UnmarshalDurable(payload, evidence); err != nil {
		return fmt.Errorf("decode controller evidence: %w", err)
	}
	// All three are frozen together; restore the full values before validation or
	// closure derivation, never a truncated prefix that could alter trust credit.
	state.IncidentRequest = evidence.IncidentRequest
	state.IncidentResult = evidence.IncidentResult
	state.PendingClosure = evidence.PendingClosure
	return wire.Validate(state)
}

func stateDigest(payload []byte, evidence string) [sha256.Size]byte {
	return sha256.Sum256(append(append([]byte(nil), payload...), []byte("\x00"+evidence)...))
}
