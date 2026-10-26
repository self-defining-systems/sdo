package install

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/distribution/reference"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// AdoptionStrategyID marks deployment evidence captured when the operator
// adopts an application that SDO did not deploy itself.
const AdoptionStrategyID = "adopted-existing"

var pulledDigest = regexp.MustCompile(`^[^@\s]+@sha256:[a-f0-9]{64}$`)

// AdoptionCapture records the image identities of an already-running
// application at install-existing, the operator's explicit adoption act.
// Without it, every workload with a tag image has no trusted deployment
// evidence, so the rendered lock and DiffAgainstSource fail for it.
type AdoptionCapture struct {
	Client        kubernetes.Interface
	ManifestPaths []string
}

// Evidence captures only containers whose live template still applies the
// accepted source image. A tag image also needs one pulled digest agreed by
// every current Pod; anything else stays an explicit lock gap.
func (c AdoptionCapture) Evidence(ctx context.Context, repository, commit, namespace string) (broker.SourceEvidence, bool, error) {
	if c.Client == nil || namespace == "" {
		return broker.SourceEvidence{}, false, fmt.Errorf("adoption capture requires a Kubernetes client and namespace")
	}
	if len(c.ManifestPaths) == 0 {
		// Without a selected deployment there is no accepted lock to support.
		return broker.SourceEvidence{}, false, nil
	}
	tree, err := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", commit+"^{tree}").Output()
	if err != nil {
		return broker.SourceEvidence{}, false, fmt.Errorf("resolve accepted tree: %w", err)
	}
	source := rendered.Source{Repository: repository, ManifestPaths: c.ManifestPaths}
	objects, err := source.RawManifests(ctx, commit, namespace)
	if err != nil {
		return broker.SourceEvidence{}, false, err
	}
	evidence := broker.SourceEvidence{ProposalTreeDigest: strings.TrimSpace(string(tree)), StrategyID: AdoptionStrategyID, Ready: true, ManifestPaths: append([]string(nil), c.ManifestPaths...), AcceptedCommit: commit}
	evidence.ObservedRevision = evidence.ProposalTreeDigest
	digests := map[string]bool{}
	for _, object := range objects {
		images, err := c.workloadImages(ctx, namespace, object)
		if err != nil {
			return broker.SourceEvidence{}, false, err
		}
		if len(images) == 0 {
			continue
		}
		evidence.ResourceIDs = append(evidence.ResourceIDs, object.GetKind()+"/"+namespace+"/"+object.GetName())
		for _, image := range images {
			evidence.ContainerImages = append(evidence.ContainerImages, image)
			digests[image.Digest] = true
		}
	}
	for digest := range digests {
		evidence.Images = append(evidence.Images, digest)
	}
	sort.Strings(evidence.Images)
	return evidence, len(evidence.ContainerImages) > 0, nil
}

func (c AdoptionCapture) workloadImages(ctx context.Context, namespace string, object *unstructured.Unstructured) ([]rendered.DeployedImage, error) {
	var template corev1.PodTemplateSpec
	var selector *metav1.LabelSelector
	name := object.GetName()
	switch object.GetKind() {
	case "Deployment":
		live, err := c.Client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, ignoreAbsent(err)
		}
		template, selector = live.Spec.Template, live.Spec.Selector
	case "StatefulSet":
		live, err := c.Client.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, ignoreAbsent(err)
		}
		template, selector = live.Spec.Template, live.Spec.Selector
	case "DaemonSet":
		live, err := c.Client.AppsV1().DaemonSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, ignoreAbsent(err)
		}
		template, selector = live.Spec.Template, live.Spec.Selector
	case "Job":
		live, err := c.Client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, ignoreAbsent(err)
		}
		template, selector = live.Spec.Template, live.Spec.Selector
	default:
		return nil, nil
	}
	var pods []corev1.Pod
	if selector != nil {
		labels, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			return nil, nil
		}
		list, err := c.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.String()})
		if err != nil {
			return nil, fmt.Errorf("list Pods for %s/%s: %w", object.GetKind(), name, err)
		}
		pods = list.Items
	}
	ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: namespace, Name: name}
	var images []rendered.DeployedImage
	for _, field := range []string{"initContainers", "containers"} {
		init := field == "initContainers"
		containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", field)
		for _, raw := range containers {
			container, _ := raw.(map[string]any)
			containerName, _ := container["name"].(string)
			sourceImage, _ := container["image"].(string)
			if containerName == "" || sourceImage == "" || liveImage(template.Spec, containerName, init) != sourceImage {
				continue
			}
			digest := sourceImage
			if !rendered.IsImmutableImage(sourceImage) {
				var ok bool
				if digest, ok = agreedDigest(pods, containerName, init, sourceImage); !ok {
					continue
				}
			}
			images = append(images, rendered.DeployedImage{Ref: ref, Name: containerName, Init: init, Applied: sourceImage, Digest: digest, Inputs: rendered.ImageInputs{SourceImage: sourceImage}})
		}
	}
	return images, nil
}

// A source workload absent from the cluster has no evidence to capture.
func ignoreAbsent(err error) error {
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func liveImage(spec corev1.PodSpec, name string, init bool) string {
	containers := spec.Containers
	if init {
		containers = spec.InitContainers
	}
	for _, container := range containers {
		if container.Name == name {
			return container.Image
		}
	}
	return ""
}

// agreedDigest returns the pulled digest only when every current Pod running
// the applied image has reported the same one from the applied image's
// repository. A mixed rollout, a Pod that has not pulled yet, or a digest
// recorded under another repository (a mirror) is not evidence.
func agreedDigest(pods []corev1.Pod, name string, init bool, applied string) (string, bool) {
	source, err := reference.ParseNormalizedNamed(applied)
	if err != nil {
		return "", false
	}
	digest := ""
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || liveImage(pod.Spec, name, init) != applied {
			continue
		}
		statuses := pod.Status.ContainerStatuses
		if init {
			statuses = pod.Status.InitContainerStatuses
		}
		reported := false
		for _, status := range statuses {
			if status.Name != name {
				continue
			}
			id := status.ImageID
			if _, rest, found := strings.Cut(id, "://"); found {
				id = rest
			}
			pulled, err := reference.ParseNormalizedNamed(id)
			if err != nil || !pulledDigest.MatchString(id) || pulled.Name() != source.Name() || (digest != "" && digest != id) {
				return "", false
			}
			digest, reported = id, true
		}
		if !reported {
			return "", false
		}
	}
	return digest, digest != ""
}

// adoptionArchive validates captured evidence through the same broker check as
// an operator-supplied seed ledger before anything reaches the PVC.
func adoptionArchive(ctx context.Context, capture AdoptionCapture, repository, commit, namespace string) ([]byte, error) {
	evidence, ok, err := capture.Evidence(ctx, repository, commit, namespace)
	if err != nil || !ok {
		return nil, err
	}
	data, err := json.Marshal(evidence)
	if err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp("", "sdo-adoption-evidence-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(root)
	if err := os.Mkdir(filepath.Join(root, "source-evidence"), 0700); err != nil {
		return nil, err
	}
	name := fmt.Sprintf("%x.json", sha256.Sum256([]byte(AdoptionStrategyID+"/"+commit)))
	if err := os.WriteFile(filepath.Join(root, "source-evidence", name), data, 0600); err != nil {
		return nil, err
	}
	return seedBrokerArchive(ctx, repository, commit, root)
}
