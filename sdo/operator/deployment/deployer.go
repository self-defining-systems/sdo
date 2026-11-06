package deployment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
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
	// Env supplies install configuration (Release, versions); Namespace
	// overrides Env.Namespace and Secrets is chosen per operation.
	Env render.Env
	// AcceptedRoot is the accepted repository VerifyAccepted reads.
	AcceptedRoot string
	// Git replaces the git process edge (simulation); nil runs git.
	Git rendered.GitFunc
}

func (d Deployer) env(mode render.SecretMode) render.Env {
	env := d.Env
	env.Namespace = d.Namespace
	env.Secrets = mode
	return env
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
	fsys, err := rendered.GitTreeFS(ctx, store, effective)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	set, err := rendered.Desired(ctx, fsys, d.env(render.SecretsInclude), d.Renderers, plan.GetSource())
	if err != nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("render deployment source: %w", err)
	}
	declared := make([]imagesrc.Declared, 0, len(plan.GetImages()))
	for _, image := range plan.GetImages() {
		declared = append(declared, imagesrc.Declared{Image: image.GetImage(), SourceID: image.GetSourceId(), Config: image.GetConfig()})
	}
	resolved, err := d.Images.ResolveAll(ctx, imageSites(set), declared, imagesrc.Request{Tree: string(effective), FS: fsys, Checkout: checkout})
	if err != nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("resolve images: %w", err)
	}
	if again, err := d.TreeDigest(ctx, checkout); err != nil || again != base {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("candidate tree changed during image resolution (was %s, now %q, err %v)", base, again, err)
	}
	if err := checkExpectedWorkloads(set, plan.GetExpectedWorkloadIds()); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	run := &applyRun{deployer: d, base: base, resolved: resolved, workloads: map[string]*workloadRecord{}}
	if err := run.apply(ctx, set); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	receipt, err := run.receipt(ctx)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	receipt.Source = proto.Clone(plan.GetSource()).(*contracts.DeploymentSource)
	receipt.RenderDigest, receipt.EnvDigest = set.RenderDigest, set.EnvDigest
	return receipt, nil
}

// imageSites lists every container image of every workload in apply order,
// including ephemeral phases (their pods run those images too).
func imageSites(set rendered.DesiredSet) []string {
	var images []string
	for _, phase := range set.Phases {
		for _, object := range phase.Objects {
			if !object.Workload {
				continue
			}
			_, _ = Stamp(object.Object.DeepCopy().Object, "", func(site ImageSite) string {
				images = append(images, site.Image)
				return site.Image
			})
		}
	}
	return images
}

func checkExpectedWorkloads(set rendered.DesiredSet, expected []string) error {
	wanted := map[string]bool{}
	for _, id := range expected {
		if id == "" || wanted[id] {
			return errors.New("expected workload IDs must be unique and nonempty")
		}
		wanted[id] = true
	}
	rendered := map[string]bool{}
	for _, object := range set.Objects() {
		if object.Workload {
			rendered[object.Ref.Kind+"/"+object.Ref.Namespace+"/"+object.Ref.Name] = true
		}
	}
	for id := range rendered {
		if !wanted[id] {
			return fmt.Errorf("workload %s is missing from deployment plan", id)
		}
	}
	if len(wanted) != len(rendered) {
		return errors.New("expected workload IDs differ from rendered manifests")
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
	resolved  map[string]imagesrc.Resolved
	workloads map[string]*workloadRecord
	order     []string
}

func (r *applyRun) apply(ctx context.Context, set rendered.DesiredSet) error {
	for _, phase := range set.Phases {
		if len(phase.Objects) == 0 {
			continue
		}
		var documents bytes.Buffer
		var gated []*workloadRecord
		for _, object := range phase.Objects {
			document := object.Object.DeepCopy().Object
			record := &workloadRecord{id: object.Ref.Kind + "/" + object.Ref.Namespace + "/" + object.Ref.Name, ephemeral: phase.Ephemeral}
			images, err := Stamp(document, rendered.BaseTree(r.base), func(site ImageSite) string {
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
		if err := r.deployer.Applier.Apply(ctx, documents.Bytes()); err != nil {
			return fmt.Errorf("apply phase %q: %w", phase.Name, err)
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
	observed, err := r.deployer.Applier.Observe(ctx, record.id)
	if err != nil {
		return fmt.Errorf("observe workload %s: %w", record.id, err)
	}
	if !observed.Ready || observed.ResourceID != record.id {
		return fmt.Errorf("workload %s is not ready", record.id)
	}
	if !record.ephemeral {
		if observed.Revision != r.base || !sameImageSet(record.images, observed.Images) {
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
	sort.Strings(ids)
	receipt := lifecycle.DeploymentReceipt{BaseTree: r.base, ObservedRevision: r.base, ResourceIDs: ids, Ready: true}
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
