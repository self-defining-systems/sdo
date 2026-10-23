package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/rendered"
)

// RecordRenderedLock accepts only a controller-produced deployment snapshot,
// independently checked against a fresh render under the publication lock.
// The expected head prevents a rollout observation from racing source acceptance.
func (b *Broker) RecordRenderedLock(ctx context.Context, expected string, snapshot rendered.Snapshot) (string, error) {
	var commit string
	err := b.locked(func() error {
		if err := b.requireClean(ctx); err != nil {
			return err
		}
		head, err := gitText(ctx, b.target, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		if head != expected {
			return errors.New("rendered lock accepted head changed")
		}
		evidence, err := b.renderedDeploymentEvidenceLocked(ctx, head)
		if err != nil {
			return err
		}
		if err := snapshot.ValidateWithEvidence(ctx, b.target, head, evidence); err != nil {
			return err
		}
		stage, cleanup, err := b.privateClone(ctx)
		if err != nil {
			return err
		}
		defer cleanup()
		if err := snapshot.Write(stage); err != nil {
			return err
		}
		paths, err := validate.ChangedPaths(ctx, stage, head)
		if err != nil {
			return err
		}
		if len(paths) == 0 {
			commit = head
			return nil
		}
		receipt, err := b.accept(ctx, stage, "rendered-"+snapshot.Lock.SourceCommit, "controller", Phase("rendered"), paths)
		commit = receipt.Commit
		return err
	})
	return commit, err
}

// Head returns the current broker-accepted revision, including a publication
// whose reply was lost before the controller persisted its next revision.
func (b *Broker) Head(ctx context.Context) (string, error) {
	var head string
	err := b.locked(func() error { var err error; head, err = gitText(ctx, b.target, "rev-parse", "HEAD"); return err })
	return head, err
}

// RenderedDeploymentEvidence reads only finalized, accepted source records from
// the private broker ledger. Losing this ledger yields gaps, never live evidence.
func (b *Broker) RenderedDeploymentEvidence(ctx context.Context, revision string) ([]rendered.DeploymentEvidence, error) {
	var evidence []rendered.DeploymentEvidence
	err := b.locked(func() error {
		var err error
		evidence, err = b.renderedDeploymentEvidenceLocked(ctx, revision)
		return err
	})
	return evidence, err
}

func (b *Broker) renderedDeploymentEvidenceLocked(ctx context.Context, revision string) ([]rendered.DeploymentEvidence, error) {
	paths, err := filepath.Glob(filepath.Join(b.workspaceRoot, "source-evidence", "*.json"))
	if err != nil {
		return nil, err
	}
	result := []rendered.DeploymentEvidence{}
	source := rendered.Source{Repository: b.target}
	records := []SourceEvidence{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var value SourceEvidence
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, fmt.Errorf("read deployment image evidence: %w", err)
		}
		commit := value.AcceptedCommit
		if commit == "" {
			continue
		} // Pending publication is not accepted evidence.
		value.AcceptedCommit = ""
		if err := validateSourceEvidence(value); err != nil {
			return nil, err
		}
		tree, err := gitText(ctx, b.target, "rev-parse", commit+"^{tree}")
		if err != nil || tree != value.ProposalTreeDigest {
			return nil, errors.New("deployment image evidence has no matching accepted tree")
		}
		if _, err := runGit(ctx, b.target, "merge-base", "--is-ancestor", commit, revision); err != nil {
			continue
		}
		value.AcceptedCommit = commit
		records = append(records, value)
	}
	// Newer independently verified deployments supersede earlier observations
	// of the same named container, including mutable external image tags.
	history, err := gitText(ctx, b.target, "rev-list", "--topo-order", revision)
	if err != nil {
		return nil, err
	}
	rank := map[string]int{}
	for index, commit := range strings.Fields(history) {
		rank[commit] = index
	}
	sort.Slice(records, func(i, j int) bool { return rank[records[i].AcceptedCommit] < rank[records[j].AcceptedCommit] })
	type imageKey struct {
		ref  sdk.ObjectRef
		name string
		init bool
	}
	seen := map[imageKey]bool{}
	for _, value := range records {
		commit, tree := value.AcceptedCommit, value.ProposalTreeDigest
		// A source edit permanently retires affected evidence. Comparing each
		// accepted source transition also rejects edit-then-revert histories.
		revisions, err := gitText(ctx, b.target, "log", "--format=%H", commit+".."+revision, "--", ".", ":(exclude).sdo")
		if err != nil {
			return nil, err
		}
		source.ManifestPaths = value.ManifestPaths
		type comparisonKey struct {
			revision string
			inputs   rendered.ImageInputs
		}
		manifests := map[string]map[imageKey]string{}
		comparisons := map[comparisonKey]bool{}
		proof := rendered.DeploymentEvidence{SourceTree: tree}
		for _, image := range value.ContainerImages {
			identity := imageKey{image.Ref, image.Name, image.Init}
			if seen[identity] {
				continue
			}
			seen[identity] = true
			valid := true
			for _, candidate := range append(strings.Fields(revisions), revision) {
				key := comparisonKey{candidate, image.Inputs}
				same, compared := comparisons[key]
				if !compared {
					same, err = source.SameImageInputs(ctx, tree, candidate, image.Inputs)
					comparisons[key] = same && err == nil
					same = comparisons[key]
				}
				if image.Inputs.SourceImage != "" {
					names, read := manifests[candidate]
					if !read {
						names = map[imageKey]string{}
						objects, readErr := source.RawManifests(ctx, candidate, image.Ref.Namespace)
						if readErr != nil {
							valid = false
							break
						}
						for _, object := range objects {
							ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
							for _, field := range []string{"initContainers", "containers"} {
								containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", field)
								for _, raw := range containers {
									container, _ := raw.(map[string]any)
									name, _ := container["name"].(string)
									sourceImage, _ := container["image"].(string)
									names[imageKey{ref, name, field == "initContainers"}] = sourceImage
								}
							}
						}
						manifests[candidate] = names
					}
					if names[imageKey{image.Ref, image.Name, image.Init}] != image.Inputs.SourceImage {
						valid = false
						break
					}
				}
				if !same {
					valid = false
					break
				}
			}
			if valid {
				proof.Images = append(proof.Images, image)
			}
		}
		if len(proof.Images) > 0 {
			result = append(result, proof)
		}
	}
	return result, nil
}
