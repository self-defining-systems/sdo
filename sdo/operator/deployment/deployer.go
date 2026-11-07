package deployment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operability/offline"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
	"sdo.dev/operator/sdk"
)

// Applier owns the cluster effects: applying documents and reading the live
// rollout state. CommandBackend is the production implementation; Observe must
// read actual state independently of anything the plan says.
type Applier interface {
	Apply(ctx context.Context, documents []byte) error
	Observe(ctx context.Context, resourceID string) (WorkloadObservation, error)
}

// Deployer deploys a candidate checkout from a DeploymentPlan: one desired
// state (rendered.Desired, shared with every read), image resolution through
// the imagesrc catalog, source-tree stamping, phased apply with readiness
// gates, and independent observation.
type Deployer struct {
	Namespace string
	Renderers *render.Catalog
	Images    *imagesrc.Catalog
	Applier   Applier
	// TreeDigest returns the proposal-tree ID of a checkout
	// (broker.ProposalTreeDigest).
	TreeDigest func(ctx context.Context, checkout string) (string, error)
	// Environment is the install-time render environment (renderenv.Discover)
	// every ApplyAndVerify renders with and records in its receipt; nil is
	// unknown (renderer defaults). VerifyAccepted renders with the
	// environment the prior receipt recorded, never this one.
	Environment *contracts.RenderEnvironment
	// AcceptedRoot is the accepted repository VerifyAccepted reads.
	AcceptedRoot string
	// Materializer writes the effective tree to the directory image builds
	// read (builds never read the agent's checkout). Nil means no build
	// source gets a directory: only sources that need none can resolve.
	Materializer *rendered.Materializer
	// Git replaces the git process edge (simulation); nil runs git.
	Git rendered.GitFunc
	// Policy is the install-time deployment policy (registry allowlist,
	// namespace and cluster-scope grants; deploymentpolicy). ApplyAndVerify
	// refuses a desired set that violates it before any image build or apply,
	// the offline operability report evaluates the same checks, and the
	// receipt records it. Nil is today's behavior: no allowlist, and
	// foreign-namespace and cluster-scoped objects are notes.
	Policy *contracts.DeploymentPolicy
	// JobWaitLimit bounds the wait for a running Job whose template changed
	// before it is recreated (DefaultJobWaitLimit when zero); past it the
	// deploy stops with an ImmutableChangeError and the Job keeps running.
	JobWaitLimit time.Duration
	// Sleep waits between reads of a running Job; nil is a real timer.
	Sleep func(context.Context, time.Duration) error
}

func (d Deployer) validate() error {
	if d.Namespace == "" || d.Renderers == nil || d.Images == nil || d.Applier == nil || d.TreeDigest == nil {
		return errors.New("deployer needs a namespace, renderer catalog, image catalog, applier and tree digest")
	}
	return nil
}

// ApplyAndVerify deploys the checkout's tree as plan describes and returns the
// receipt of what is actually running, bound to the source and its digests.
func (d Deployer) ApplyAndVerify(ctx context.Context, checkout string, plan *contracts.DeploymentPlan) (lifecycle.DeploymentReceipt, error) {
	if err := d.validate(); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if plan == nil || plan.GetSource() == nil {
		return lifecycle.DeploymentReceipt{}, errors.New("deployment plan needs a source")
	}
	if len(plan.GetExpectedWorkloadIds()) == 0 {
		return lifecycle.DeploymentReceipt{}, errors.New("deployment plan needs expected workloads")
	}
	base, err := d.TreeDigest(ctx, checkout)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	store := rendered.ObjectStore{Dir: checkout, Git: d.Git}
	effective, err := rendered.Effective(ctx, store, rendered.BaseTree(base))
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	defer effective.Close()
	fsys, err := rendered.GitTreeFS(ctx, effective)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	set, err := rendered.Desired(ctx, fsys, renderenv.For(d.Environment, d.Namespace, render.SecretsInclude), d.Renderers, plan.GetSource())
	if err != nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("render deployment source: %w", err)
	}
	if err := d.enforcePolicy(set); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if err := rendered.CheckServedAPIs(set, d.Environment.GetApiVersions()); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	declared := make([]imagesrc.Declared, 0, len(plan.GetImages()))
	for _, image := range plan.GetImages() {
		declared = append(declared, imagesrc.Declared{Image: image.GetImage(), SourceID: image.GetSourceId(), Config: image.GetConfig()})
	}
	request := imagesrc.Request{Tree: effective.ID, FS: fsys}
	if d.Materializer != nil {
		dir, release, err := d.Materializer.Materialize(ctx, effective)
		if err != nil {
			return lifecycle.DeploymentReceipt{}, err
		}
		defer release()
		request.Checkout = dir
	}
	resolved, err := d.Images.ResolveAll(ctx, rendered.ImageSites(set), declared, request)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("resolve images: %w", err)
	}
	if err := rendered.CheckExpectedWorkloads(set, plan.GetExpectedWorkloadIds()); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	operation, err := d.operation(ctx, set)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	run := &applyRun{deployer: d, base: base, effective: effective, resolved: resolved, workloads: map[string]*workloadRecord{}}
	if err := run.apply(ctx, set, operation); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	receipt, err := run.receipt(ctx)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	// Every workload of this revision is ready: the Jobs it renamed are dead.
	if _, err := run.pruneSuperseded(ctx, set); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	receipt.Source = proto.Clone(plan.GetSource()).(*contracts.DeploymentSource)
	receipt.RenderDigest, receipt.EnvDigest = set.RenderDigest, set.EnvDigest
	if d.Environment != nil {
		receipt.Environment = proto.Clone(d.Environment).(*contracts.RenderEnvironment)
	}
	if d.Policy != nil {
		receipt.Policy = proto.Clone(d.Policy).(*contracts.DeploymentPolicy)
	}
	return receipt, nil
}

// PolicyError is a desired set that violates the install-time deployment
// policy. Nothing was built or applied.
type PolicyError struct {
	Checks []operability.Check
}

func (e *PolicyError) Error() string {
	details := make([]string, 0, len(e.Checks))
	for _, c := range e.Checks {
		details = append(details, c.ID+": "+c.Detail)
	}
	return "deployment violates the install-time policy (" + strings.Join(details, "; ") + ")"
}

// PolicyChecks are the policy's failed checks (lifecycle.PolicyViolation).
func (e *PolicyError) PolicyChecks() []operability.Check { return e.Checks }

func (d Deployer) enforcePolicy(set rendered.DesiredSet) error {
	var failed []operability.Check
	for _, c := range offline.CheckPolicy(set, deploymentpolicy.Offline(d.Policy)) {
		if c.Status == operability.StatusFail {
			failed = append(failed, c)
		}
	}
	if len(failed) > 0 {
		return &PolicyError{Checks: failed}
	}
	return nil
}

// workloadRecord is what one applied workload must look like when observed.
type workloadRecord struct {
	id        string
	ephemeral bool
	images    []string
	named     []rendered.DeployedImage
	observed  *WorkloadObservation
}

type applyRun struct {
	deployer  Deployer
	base      string
	effective rendered.EffectiveTree
	resolved  map[string]imagesrc.Resolved
	workloads map[string]*workloadRecord
	order     []string
	recreated []*recreation
}

// operation is what this deploy does, as Helm decides it: an upgrade when the
// application's long-running workloads already run in the cluster, an install otherwise (and
// whenever the applier cannot read live objects). It selects the hook phases
// the deploy applies; the desired objects do not depend on it.
func (d Deployer) operation(ctx context.Context, set rendered.DesiredSet) (render.Operation, error) {
	recreator, ok := d.Applier.(Recreator)
	if !ok {
		return render.OperationInstall, nil
	}
	for _, object := range set.Objects() {
		// A Job is no evidence of a running release: it finishes and may be gone.
		if !object.Workload || object.Foreign || object.Ref.Kind == "Job" {
			continue
		}
		_, found, err := recreator.Get(ctx, object.Ref)
		if err != nil {
			return "", fmt.Errorf("decide install or upgrade: %w", err)
		}
		if found {
			return render.OperationUpgrade, nil
		}
	}
	return render.OperationInstall, nil
}

func (r *applyRun) apply(ctx context.Context, set rendered.DesiredSet, operation render.Operation) error {
	for _, phase := range set.For(operation) {
		if len(phase.Objects) == 0 {
			continue
		}
		var documents bytes.Buffer
		var gated []*workloadRecord
		for _, object := range phase.Objects {
			document := object.Object.DeepCopy().Object
			record := &workloadRecord{id: object.Ref.Kind + "/" + object.Ref.Namespace + "/" + object.Ref.Name, ephemeral: phase.Ephemeral}
			images, err := Stamp(document, r.effective.ID, func(site ImageSite) string {
				pinned := r.resolved[site.Image]
				record.named = append(record.named, rendered.DeployedImage{
					Ref:  sdk.ObjectRef{APIVersion: object.Ref.APIVersion, Kind: site.Kind, Namespace: object.Ref.Namespace, Name: site.Name},
					Name: site.Container, Init: site.Init, Applied: pinned.Ref, Digest: pinned.Ref,
					Inputs: rendered.ImageInputs{SourceImage: pinned.Inputs.SourceImage, ContextPath: pinned.Inputs.ContextPath, DockerfilePath: pinned.Inputs.DockerfilePath},
				})
				return pinned.Ref
			})
			if err != nil {
				return fmt.Errorf("stamp %s/%s: %w", object.Ref.Kind, object.Ref.Name, err)
			}
			encoded, err := yaml.Marshal(document)
			if err != nil {
				return err
			}
			documents.WriteString("---\n")
			documents.Write(encoded)
			if object.Workload {
				record.images = images
				r.workloads[record.id] = record
				r.order = append(r.order, record.id)
				gated = append(gated, record)
			}
		}
		if err := r.resolveConflicts(ctx, phase, documents.Bytes()); err != nil {
			return err
		}
		if err := r.deployer.Applier.Apply(ctx, documents.Bytes()); err != nil {
			return fmt.Errorf("apply phase %q: %w", phase.Name, err)
		}
		if err := r.afterApply(ctx); err != nil {
			return err
		}
		if phase.Gate == render.GateReady {
			for _, record := range gated {
				if err := r.observe(ctx, record); err != nil {
					return fmt.Errorf("phase %q: %w", phase.Name, err)
				}
			}
		}
	}
	return nil
}

func (r *applyRun) observe(ctx context.Context, record *workloadRecord) error {
	if record.observed != nil {
		return nil
	}
	observed, err := r.deployer.Applier.Observe(withJobsInFlight(ctx, r.jobsInFlight(record)), record.id)
	if err != nil {
		return fmt.Errorf("observe workload %s: %w", record.id, err)
	}
	if !observed.Ready || observed.ResourceID != record.id {
		return fmt.Errorf("workload %s is not ready", record.id)
	}
	if !record.ephemeral {
		if observed.Revision != r.effective.ID || !sameImageSet(record.images, observed.Images) {
			return fmt.Errorf("workload %s is not ready at the candidate revision and images", record.id)
		}
		// A backend that supplies only a flat image list cannot authorize
		// per-container substitutions in a durable lock.
		if len(observed.ContainerImages) > 0 && !sameNamedImages(record.named, observed.ContainerImages) {
			return fmt.Errorf("workload %s container names or roles differ from applied images", record.id)
		}
	}
	record.observed = &observed
	return nil
}

// jobsInFlight lists the Jobs applied so far (the current phase and every
// earlier one) other than record that are not yet observed complete.
func (r *applyRun) jobsInFlight(record *workloadRecord) []string {
	var ids []string
	for _, id := range r.order {
		if other := r.workloads[id]; other != record && other.observed == nil && strings.HasPrefix(id, "Job/") {
			ids = append(ids, id)
		}
	}
	return ids
}

func (r *applyRun) receipt(ctx context.Context) (lifecycle.DeploymentReceipt, error) {
	var ids []string
	for _, id := range r.order {
		record := r.workloads[id]
		if record.ephemeral {
			continue
		}
		// Workloads in ungated phases are observed only now.
		if err := r.observe(ctx, record); err != nil {
			return lifecycle.DeploymentReceipt{}, err
		}
		ids = append(ids, id)
	}
	// Every workload, recreated ones included, is ready: orphans no recreated
	// workload adopted are replaced now.
	if err := r.replaceRemainingOrphans(ctx); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	sort.Strings(ids)
	receipt := lifecycle.DeploymentReceipt{
		BaseTree: r.base, EffectiveTree: r.effective.ID, PatchesDigest: r.effective.PatchesDigest,
		ObservedRevision: r.effective.ID, ResourceIDs: ids, Ready: true, Recreated: r.recreations(),
	}
	for _, id := range ids {
		record := r.workloads[id]
		receipt.Images = append(receipt.Images, record.observed.Images...)
		if len(record.observed.ContainerImages) > 0 {
			receipt.ContainerImages = append(receipt.ContainerImages, record.named...)
		}
	}
	if len(receipt.Images) == 0 {
		return lifecycle.DeploymentReceipt{}, errors.New("deployment produced no workload images")
	}
	return receipt, nil
}

func sameImageSet(expected, actual []string) bool {
	a := append([]string(nil), expected...)
	b := append([]string(nil), actual...)
	sort.Strings(a)
	sort.Strings(b)
	return strings.Join(a, "\x00") == strings.Join(b, "\x00")
}
