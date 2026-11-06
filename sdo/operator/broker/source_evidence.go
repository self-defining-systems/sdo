package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/rendered"
)

// ErrSourceEvidencePredatesDeploymentSource marks a ledger record written
// before receipts carried a DeploymentSource (manifest paths and a strategy
// ID). It has no reader: the lane must be reinstalled.
var ErrSourceEvidencePredatesDeploymentSource = errors.New("deployment evidence predates deployment_source (written by an older operator); persistent lanes must reinstall the operator (`sdo-operator install --fresh` / lane reset)")

// ErrSourceEvidencePredatesEffectiveTree marks a ledger record written before
// receipts bound the effective tree and the deployment patch series. It has no
// reader: the deployed workloads carry a different source stamp, so the lane
// must be reinstalled.
var ErrSourceEvidencePredatesEffectiveTree = errors.New("deployment evidence predates effective_tree and patches_digest (written by an older operator); persistent lanes must reinstall the operator (`sdo-operator install --fresh` / lane reset)")

func validateSourceEvidence(value SourceEvidence) error {
	if value.AcceptedCommit != "" {
		return errors.New("deployment evidence cannot claim an accepted commit")
	}
	if !validGitID(value.BaseTree) || !validGitID(value.EffectiveTree) || !validSHA256(value.PatchesDigest) || value.ObservedRevision != value.EffectiveTree || !value.Ready || len(value.Images) == 0 || len(value.ResourceIDs) == 0 {
		return errors.New("trusted source evidence is incomplete or does not match the deployed tree")
	}
	if value.DeploymentSource == nil || value.DeploymentSource.GetRendererId() == "" || value.DeploymentSource.GetConfig() == nil {
		return errors.New("trusted source evidence names no deployment source")
	}
	if !validSHA256(value.RenderDigest) || !validSHA256(value.EnvDigest) {
		return errors.New("trusted source evidence lacks its render and environment digests")
	}
	if value.Plan != nil && !rendered.SameDeploymentSource(value.Plan.GetSource(), value.DeploymentSource) {
		return errors.New("accepted deployment plan names another source than the deployment evidence")
	}
	seen := map[string]bool{}
	for _, resource := range value.ResourceIDs {
		if resource == "" || seen[resource] {
			return errors.New("source evidence resource IDs must be unique and nonempty")
		}
		seen[resource] = true
	}
	return nil
}

// requireEffectiveEvidence recomputes the effective tree of root's candidate
// tree and requires it, and its patch series digest, to be what the deployer
// verified. The broker never trusts the evidence's claim: a patch applied
// differently (or a different series) than at deployment is not published.
func requireEffectiveEvidence(ctx context.Context, root string, evidence SourceEvidence) error {
	tree, err := ProposalTreeDigest(ctx, root)
	if err != nil {
		return err
	}
	effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: root}, rendered.BaseTree(tree))
	if err != nil {
		return fmt.Errorf("compute effective tree of the staged source: %w", err)
	}
	defer effective.Close()
	if effective.ID != evidence.EffectiveTree || effective.PatchesDigest != evidence.PatchesDigest {
		return fmt.Errorf("staged effective tree %s (patches %s) differs from the verified deployment's %s (patches %s)", effective.ID, effective.PatchesDigest, evidence.EffectiveTree, evidence.PatchesDigest)
	}
	return nil
}

func validSHA256(value string) bool {
	return len(value) == 64 && validGitID(value)
}

// sourceEvidenceRecord is the durable JSON form. Protobuf fields are stored as
// ProtoJSON so their Any configs stay readable and survive Go struct changes.
type sourceEvidenceRecord struct {
	ContainerImages   []rendered.DeployedImage `json:"container_images,omitempty"`
	DeploymentSource  json.RawMessage          `json:"deployment_source"`
	Plan              json.RawMessage          `json:"plan,omitempty"`
	BaseTree          string                   `json:"base_tree"`
	EffectiveTree     string                   `json:"effective_tree"`
	PatchesDigest     string                   `json:"patches_digest"`
	ObservedRevision  string                   `json:"observed_revision"`
	RenderDigest      string                   `json:"render_digest"`
	EnvDigest         string                   `json:"env_digest"`
	RenderEnvironment json.RawMessage          `json:"render_environment,omitempty"`
	Images            []string                 `json:"images"`
	ResourceIDs       []string                 `json:"resource_ids"`
	Ready             bool                     `json:"ready"`
	AcceptedCommit    string                   `json:"accepted_commit,omitempty"`
}

// MarshalJSON encodes the ledger record.
func (e SourceEvidence) MarshalJSON() ([]byte, error) {
	record := sourceEvidenceRecord{
		ContainerImages: e.ContainerImages, BaseTree: e.BaseTree, EffectiveTree: e.EffectiveTree, PatchesDigest: e.PatchesDigest, ObservedRevision: e.ObservedRevision,
		RenderDigest: e.RenderDigest, EnvDigest: e.EnvDigest, Images: e.Images, ResourceIDs: e.ResourceIDs,
		Ready: e.Ready, AcceptedCommit: e.AcceptedCommit,
	}
	var err error
	if e.DeploymentSource != nil {
		if record.DeploymentSource, err = canonicalProtoJSON(e.DeploymentSource); err != nil {
			return nil, fmt.Errorf("encode deployment source: %w", err)
		}
	}
	if e.RenderEnvironment != nil {
		if record.RenderEnvironment, err = canonicalProtoJSON(e.RenderEnvironment); err != nil {
			return nil, fmt.Errorf("encode render environment: %w", err)
		}
	}
	if e.Plan != nil {
		if record.Plan, err = canonicalProtoJSON(e.Plan); err != nil {
			return nil, fmt.Errorf("encode deployment plan: %w", err)
		}
	}
	return json.Marshal(record)
}

// UnmarshalJSON decodes a ledger record. A record from before deployment
// sources (it has no deployment_source) fails with
// ErrSourceEvidencePredatesDeploymentSource, one from before effective trees
// (no effective_tree) with ErrSourceEvidencePredatesEffectiveTree.
func (e *SourceEvidence) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if source, found := fields["deployment_source"]; !found || string(source) == "null" {
		return ErrSourceEvidencePredatesDeploymentSource
	}
	if tree, found := fields["effective_tree"]; !found || string(tree) == "null" || string(tree) == `""` {
		return ErrSourceEvidencePredatesEffectiveTree
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record sourceEvidenceRecord
	if err := decoder.Decode(&record); err != nil {
		return err
	}
	value := SourceEvidence{
		ContainerImages: record.ContainerImages, DeploymentSource: &contracts.DeploymentSource{}, BaseTree: record.BaseTree,
		EffectiveTree: record.EffectiveTree, PatchesDigest: record.PatchesDigest,
		ObservedRevision: record.ObservedRevision, RenderDigest: record.RenderDigest, EnvDigest: record.EnvDigest,
		Images: record.Images, ResourceIDs: record.ResourceIDs, Ready: record.Ready, AcceptedCommit: record.AcceptedCommit,
	}
	if err := protojson.Unmarshal(record.DeploymentSource, value.DeploymentSource); err != nil {
		return fmt.Errorf("decode deployment source: %w", err)
	}
	if len(record.RenderEnvironment) > 0 {
		value.RenderEnvironment = &contracts.RenderEnvironment{}
		if err := protojson.Unmarshal(record.RenderEnvironment, value.RenderEnvironment); err != nil {
			return fmt.Errorf("decode render environment: %w", err)
		}
	}
	if len(record.Plan) > 0 {
		value.Plan = &contracts.DeploymentPlan{}
		if err := protojson.Unmarshal(record.Plan, value.Plan); err != nil {
			return fmt.Errorf("decode deployment plan: %w", err)
		}
	}
	*e = value
	return nil
}

func canonicalProtoJSON(message proto.Message) (json.RawMessage, error) {
	encoded, err := protojson.Marshal(message)
	if err != nil {
		return nil, err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, encoded); err != nil {
		return nil, err
	}
	return compact.Bytes(), nil
}

// sameSourceEvidence compares two records by content: protobuf fields by
// canonical encoding (Any bytes of one message may differ), the rest exactly.
func sameSourceEvidence(left, right SourceEvidence) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func validGitID(value string) bool {
	if (len(value) != 40 && len(value) != 64) || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// SourceEvidence returns only evidence bound to an accepted source commit. A
// pending record without a commit is never presented as deployment success.
func (b *Broker) SourceEvidence(ctx context.Context, incidentID string) (SourceEvidence, bool, error) {
	var value SourceEvidence
	var found bool
	err := b.locked(func() error {
		receipt, accepted, err := b.findAccepted(ctx, incidentID, InitialPhase)
		if err != nil || !accepted {
			return err
		}
		value, found, err = b.sourceEvidenceLocked(ctx, incidentID, receipt)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("accepted source commit has no trusted deployment evidence")
		}
		return nil
	})
	return value, found, err
}

func (b *Broker) sourceEvidenceLocked(ctx context.Context, incidentID string, receipt Receipt) (SourceEvidence, bool, error) {
	data, err := os.ReadFile(b.sourceEvidencePath(incidentID))
	if errors.Is(err, os.ErrNotExist) {
		return SourceEvidence{}, false, nil
	}
	if err != nil {
		return SourceEvidence{}, false, err
	}
	var value SourceEvidence
	if err := json.Unmarshal(data, &value); err != nil {
		return SourceEvidence{}, false, fmt.Errorf("decode source evidence %s: %w", b.sourceEvidencePath(incidentID), err)
	}
	persistedCommit := value.AcceptedCommit
	if value.AcceptedCommit != "" && value.AcceptedCommit != receipt.Commit {
		return SourceEvidence{}, false, errors.New("source evidence points to another accepted commit")
	}
	value.AcceptedCommit = ""
	if err := validateSourceEvidence(value); err != nil {
		return SourceEvidence{}, false, err
	}
	acceptedTree, err := gitText(ctx, b.target, "rev-parse", receipt.Commit+"^{tree}")
	if err != nil {
		return SourceEvidence{}, false, err
	}
	if value.BaseTree != acceptedTree {
		return SourceEvidence{}, false, errors.New("accepted source tree differs from trusted deployment evidence")
	}
	value.AcceptedCommit = receipt.Commit
	if persistedCommit == "" {
		if err := b.writeSourceEvidence(incidentID, value); err != nil {
			return SourceEvidence{}, false, fmt.Errorf("reconcile accepted source evidence: %w", err)
		}
	}
	return value, true, nil
}

func (b *Broker) sourceEvidencePath(incidentID string) string {
	digest := sha256.Sum256([]byte(incidentID))
	return filepath.Join(b.workspaceRoot, "source-evidence", fmt.Sprintf("%x.json", digest))
}

// writeSourceEvidence atomically replaces one broker-owned ledger record. Call
// while holding the broker lock; fsync makes the pre-commit record durable.
func (b *Broker) writeSourceEvidence(incidentID string, value SourceEvidence) error {
	path := b.sourceEvidencePath(incidentID)
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".source-evidence-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
