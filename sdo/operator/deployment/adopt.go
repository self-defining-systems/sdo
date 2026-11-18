package deployment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// ContainerSite names one container of a live workload's pod template.
type ContainerSite struct {
	Name string
	Init bool
}

// LiveContainer is what a live workload says about one container.
type LiveContainer struct {
	// Image is the container's image field in the live pod template.
	Image string
	// Digests are the image digests ("sha256:<hex>") of the container in each
	// running pod, one entry per pod (empty when a pod reports none).
	Digests []string
}

// LiveImages is the live image state of one workload.
type LiveImages struct {
	// Found is false when the workload does not exist or its kind has no
	// reportable pods.
	Found      bool
	Containers map[ContainerSite]LiveContainer
}

// ImageAdopter is the part of an Applier that lets the deployer leave a
// running workload's image fields alone. Adopting a running application must
// not roll it: rewriting a tag to the digest of the same bytes changes the pod
// template, and every workload would restart and lose its in-memory state. An
// Applier that does not implement it always applies the pins.
type ImageAdopter interface {
	// LiveImages reads the workload's template images and its running pods'
	// image digests.
	LiveImages(ctx context.Context, ref sdk.ObjectRef) (LiveImages, error)
	// TemplateUnchanged server-side dry-run applies document and reports
	// whether the workload's pod template would stay exactly as it is live.
	TemplateUnchanged(ctx context.Context, ref sdk.ObjectRef, document []byte) (bool, error)
}

var digestHex = regexp.MustCompile(`(sha256:[a-f0-9]{64})$`)

// ImageDigest returns the "sha256:<hex>" suffix of an image reference or a
// pod's imageID, or "".
func ImageDigest(reference string) string {
	if m := digestHex.FindStringSubmatch(reference); m != nil {
		return m[1]
	}
	return ""
}

// keptImages decides, for one workload document, which container images stay as
// the live template has them. A container's image is kept only when the live
// template still declares the image the source declares, the pin differs from
// it, and every running pod runs the pinned digest; and only when the whole
// document, with those images kept, leaves the live pod template unchanged
// (a deploy that changes the template for another reason pins as usual). Any
// doubt (not found, unreadable, a mismatch, an applier without live reads)
// returns nil: the pins are applied as before.
func (r *applyRun) keptImages(ctx context.Context, object rendered.DesiredObject, document map[string]any) map[ContainerSite]string {
	adopter, ok := r.deployer.Applier.(ImageAdopter)
	if !ok {
		return nil
	}
	live, err := adopter.LiveImages(ctx, object.Ref)
	if err != nil {
		slog.Warn("deploy_adoption_live_images_failed", "workload", object.Ref.Kind+"/"+object.Ref.Name, "error", err)
		return nil
	}
	if !live.Found {
		return nil
	}
	trial, err := copyDocument(document)
	if err != nil {
		return nil
	}
	kept := map[ContainerSite]string{}
	// pins counts the sites that would still change: one pod template, so
	// any pin applied rolls the workload and nothing is kept.
	pins := 0
	if _, err := Stamp(trial, "", func(site ImageSite) string {
		key := ContainerSite{Name: site.Container, Init: site.Init}
		pinned := r.resolved[site.Image].Ref
		current, found := live.Containers[key]
		want := ImageDigest(pinned)
		if found && current.Image == pinned {
			return pinned
		}
		if !found || want == "" || current.Image != site.Image || len(current.Digests) == 0 {
			pins++
			return pinned
		}
		for _, digest := range current.Digests {
			if digest != want {
				pins++
				return pinned
			}
		}
		kept[key] = current.Image
		return current.Image
	}); err != nil || len(kept) == 0 || pins > 0 {
		return nil
	}
	if _, err := Stamp(trial, r.effective.ID, nil); err != nil {
		return nil
	}
	encoded, err := yaml.Marshal(trial)
	if err != nil {
		return nil
	}
	var buffer bytes.Buffer
	buffer.WriteString("---\n")
	buffer.Write(encoded)
	unchanged, err := adopter.TemplateUnchanged(ctx, object.Ref, buffer.Bytes())
	if err != nil {
		slog.Warn("deploy_adoption_dry_run_failed", "workload", object.Ref.Kind+"/"+object.Ref.Name, "error", err)
		return nil
	}
	if !unchanged {
		slog.Info("deploy_adoption_template_changes", "workload", object.Ref.Kind+"/"+object.Ref.Name)
		return nil
	}
	return kept
}

// copyDocument deep-copies through the encoding the apply itself uses, so the
// dry run sees the same scalars (integers stay integers) the apply sends.
func copyDocument(document map[string]any) (map[string]any, error) {
	data, err := yaml.Marshal(document)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	return out, yaml.Unmarshal(data, &out)
}

// liveForm is what an observation of the workload reports for a recorded image:
// the image left in the template when the pin was kept out of it.
func liveForm(image rendered.DeployedImage) rendered.DeployedImage {
	if image.Live != "" {
		image.Applied, image.Digest, image.Live = image.Live, image.Live, ""
	}
	return image
}

// liveImages maps images recorded as pins to the images the workloads hold:
// each recorded image with a Live form replaces one occurrence of its pin.
func liveImages(images []string, named []rendered.DeployedImage) []string {
	return substituteImages(images, named, func(i rendered.DeployedImage) (string, string) { return i.Applied, i.Live })
}

// pinnedImages is the inverse of liveImages.
func pinnedImages(images []string, named []rendered.DeployedImage) []string {
	return substituteImages(images, named, func(i rendered.DeployedImage) (string, string) { return i.Live, i.Applied })
}

func substituteImages(images []string, named []rendered.DeployedImage, pair func(rendered.DeployedImage) (from, to string)) []string {
	out := append([]string(nil), images...)
	for _, image := range named {
		if image.Live == "" {
			continue
		}
		from, to := pair(image)
		for i := range out {
			if out[i] == from {
				out[i] = to
				break
			}
		}
	}
	return out
}

// LiveImages implements ImageAdopter for workloads whose selector is a plain
// label match (Deployment, StatefulSet, DaemonSet); other kinds report
// nothing and are pinned as before.
func (b CommandBackend) LiveImages(ctx context.Context, ref sdk.ObjectRef) (LiveImages, error) {
	if !rendered.WorkloadKind(ref.Kind) {
		return LiveImages{}, nil
	}
	object, found, err := b.getJSON(ctx, ref)
	if err != nil || !found {
		return LiveImages{}, err
	}
	selectorSpec := objectMap(objectMap(object, "spec"), "selector")
	matchLabels := objectMap(selectorSpec, "matchLabels")
	if len(matchLabels) == 0 || len(objectSlice(selectorSpec, "matchExpressions")) > 0 {
		return LiveImages{}, nil
	}
	var selector []string
	for key, value := range matchLabels {
		text, _ := value.(string)
		selector = append(selector, key+"="+text)
	}
	sort.Strings(selector)
	podSpec := objectMap(objectMap(objectMap(object, "spec"), "template"), "spec")
	out := LiveImages{Found: true, Containers: map[ContainerSite]LiveContainer{}}
	for _, field := range rendered.ContainerFields {
		for _, raw := range objectSlice(podSpec, field) {
			container, _ := raw.(map[string]any)
			out.Containers[ContainerSite{Name: objectString(container, "name"), Init: field == rendered.InitContainers}] = LiveContainer{Image: objectString(container, "image")}
		}
	}
	args := b.kubectlArgs("get", "pods", "--namespace", ref.Namespace, "--selector", strings.Join(selector, ","), "--output=json")
	output, err := b.run(ctx, b.kubectl(), args, nil)
	if err != nil {
		return LiveImages{}, fmt.Errorf("list pods of %s/%s: %w: %s", ref.Kind, ref.Name, err, strings.TrimSpace(string(output)))
	}
	var list map[string]any
	if err := json.Unmarshal(output, &list); err != nil {
		return LiveImages{}, fmt.Errorf("decode pods of %s/%s: %w", ref.Kind, ref.Name, err)
	}
	for _, rawPod := range objectSlice(list, "items") {
		pod, _ := rawPod.(map[string]any)
		status := objectMap(pod, "status")
		if phase := objectString(status, "phase"); phase == "Succeeded" || phase == "Failed" {
			continue
		}
		for _, field := range []struct {
			status string
			init   bool
		}{{"containerStatuses", false}, {"initContainerStatuses", true}} {
			reported := map[string]string{}
			for _, rawStatus := range objectSlice(status, field.status) {
				entry, _ := rawStatus.(map[string]any)
				reported[objectString(entry, "name")] = ImageDigest(objectString(entry, "imageID"))
			}
			for site, container := range out.Containers {
				if site.Init == field.init {
					container.Digests = append(container.Digests, reported[site.Name])
					out.Containers[site] = container
				}
			}
		}
	}
	return out, nil
}

// TemplateUnchanged implements ImageAdopter.
func (b CommandBackend) TemplateUnchanged(ctx context.Context, ref sdk.ObjectRef, document []byte) (bool, error) {
	live, found, err := b.getJSON(ctx, ref)
	if err != nil || !found {
		return false, err
	}
	args := b.kubectlArgs("apply", "--server-side", "--force-conflicts", "--field-manager="+sdk.DeployerFieldManager, "--dry-run=server", "--output=json", "--filename=-")
	output, err := b.run(ctx, b.kubectl(), args, document)
	if err != nil {
		return false, fmt.Errorf("dry-run apply %s/%s: %w: %s", ref.Kind, ref.Name, err, strings.TrimSpace(string(output)))
	}
	var preview map[string]any
	if err := json.Unmarshal(output, &preview); err != nil {
		return false, fmt.Errorf("decode dry-run of %s/%s: %w", ref.Kind, ref.Name, err)
	}
	return reflect.DeepEqual(objectMap(objectMap(live, "spec"), "template"), objectMap(objectMap(preview, "spec"), "template")), nil
}

func (b CommandBackend) getJSON(ctx context.Context, ref sdk.ObjectRef) (map[string]any, bool, error) {
	args := b.kubectlArgs("get", ref.Kind, ref.Name, "--namespace", ref.Namespace, "--ignore-not-found", "--output=json")
	output, err := b.run(ctx, b.kubectl(), args, nil)
	if err != nil {
		return nil, false, fmt.Errorf("get %s/%s: %w: %s", ref.Kind, ref.Name, err, strings.TrimSpace(string(output)))
	}
	if strings.TrimSpace(string(output)) == "" {
		return nil, false, nil
	}
	var object map[string]any
	if err := json.Unmarshal(output, &object); err != nil {
		return nil, false, fmt.Errorf("decode %s/%s: %w", ref.Kind, ref.Name, err)
	}
	return object, true, nil
}

var _ ImageAdopter = CommandBackend{}
