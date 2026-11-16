package deployment

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/imagesrc/imagesrctest"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// adoptFake is a cluster whose workload already runs: Observe reports the
// per-container identities the way the real backend does (from the document
// applied), and the live reads say what the running pods hold.
type adoptFake struct {
	*clusterFake
	live      LiveImages
	liveErr   error
	unchanged bool
	probed    []string
}

func (a *adoptFake) LiveImages(context.Context, sdk.ObjectRef) (LiveImages, error) {
	return a.live, a.liveErr
}

func (a *adoptFake) TemplateUnchanged(_ context.Context, _ sdk.ObjectRef, document []byte) (bool, error) {
	a.probed = append(a.probed, string(document))
	return a.unchanged, nil
}

func (a *adoptFake) Observe(ctx context.Context, id string) (WorkloadObservation, error) {
	observed, err := a.clusterFake.Observe(ctx, id)
	if err != nil {
		return observed, err
	}
	document := a.applied[len(a.applied)-1]
	parts := strings.Split(id, "/")
	_, _ = Stamp(document, "", func(site ImageSite) string {
		observed.ContainerImages = append(observed.ContainerImages, rendered.DeployedImage{
			Ref:  sdk.ObjectRef{APIVersion: "apps/v1", Kind: parts[0], Namespace: parts[1], Name: parts[2]},
			Name: site.Container, Init: site.Init, Applied: site.Image, Digest: site.Image,
		})
		return site.Image
	})
	return observed, nil
}

// adoptLive is a workload running the tags the source declares, each pod on the
// digest the fake image source pins them to.
func adoptLive(webDigest string) LiveImages {
	return LiveImages{Found: true, Containers: map[ContainerSite]LiveContainer{
		{Name: "init", Init: true}: {Image: "busybox:1", Digests: []string{imagesrctest.Digest("busybox:1")}},
		{Name: "web"}:              {Image: "web:1", Digests: []string{webDigest, webDigest}},
		{Name: "side"}:             {Image: "web:1", Digests: []string{webDigest, webDigest}},
	}}
}

func appliedImages(t *testing.T, cluster *clusterFake) []string {
	t.Helper()
	var images []string
	for _, document := range cluster.applied {
		if document["kind"] != "Deployment" {
			continue
		}
		images, _ = Stamp(document, "", nil)
	}
	return images
}

func adoptDeploy(t *testing.T, fake *adoptFake) (receiptImages []string, containers []rendered.DeployedImage, applied []string, err error) {
	t.Helper()
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, fake.clusterFake)
	deployer.Applier = fake
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	return receipt.Images, receipt.ContainerImages, appliedImages(t, fake.clusterFake), err
}

func TestAdoptionOfAWorkloadRunningThePinnedDigestLeavesTheTemplateImages(t *testing.T) {
	fake := &adoptFake{clusterFake: &clusterFake{}, live: adoptLive(imagesrctest.Digest("web:1")), unchanged: true}
	fake.live.Containers[ContainerSite{Name: "web"}] = LiveContainer{Image: "web:1", Digests: []string{imagesrctest.Digest("web:1")}}
	receiptImages, containers, applied, err := adoptDeploy(t, fake)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(applied, ","); got != "busybox:1,web:1,web:1" {
		t.Fatalf("applied template images = %s, want the live tags", got)
	}
	// The pin is recorded, not applied: the receipt names the digests.
	pinned := "web@" + imagesrctest.Digest("web:1")
	busybox := "busybox@" + imagesrctest.Digest("busybox:1")
	if got := strings.Join(receiptImages, ","); got != busybox+","+pinned+","+pinned {
		t.Fatalf("receipt images = %v", receiptImages)
	}
	if len(containers) != 3 {
		t.Fatalf("container images = %+v", containers)
	}
	for _, image := range containers {
		wantLive := map[string]string{"init": "busybox:1", "web": "web:1", "side": "web:1"}[image.Name]
		if image.Live != wantLive || !strings.Contains(image.Applied, "@sha256:") || image.Digest != image.Applied {
			t.Fatalf("container image %+v: want pin recorded with live %q", image, wantLive)
		}
	}
	if len(fake.probed) != 1 || !strings.Contains(fake.probed[0], "web:1") || strings.Contains(fake.probed[0], "@sha256:") {
		t.Fatalf("the dry run must see the kept images, got %v", fake.probed)
	}
}

func TestAdoptionPinsWhenARunningPodRunsAnotherDigest(t *testing.T) {
	fake := &adoptFake{clusterFake: &clusterFake{}, live: adoptLive(imagesrctest.Digest("web:other")), unchanged: true}
	receiptImages, containers, applied, err := adoptDeploy(t, fake)
	if err != nil {
		t.Fatal(err)
	}
	for _, image := range applied {
		if !strings.Contains(image, "@sha256:") {
			t.Fatalf("applied images = %v, want every image pinned", applied)
		}
	}
	for _, image := range containers {
		if image.Live != "" {
			t.Fatalf("pinned container records a live image: %+v", image)
		}
	}
	if len(receiptImages) != 3 {
		t.Fatalf("receipt images = %v", receiptImages)
	}
}

func TestAdoptionPinsWhenTheTemplateChangesForAnotherReason(t *testing.T) {
	fake := &adoptFake{clusterFake: &clusterFake{}, live: adoptLive(imagesrctest.Digest("web:1")), unchanged: false}
	_, containers, applied, err := adoptDeploy(t, fake)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(applied, ",") == "busybox:1,web:1,web:1" || strings.Contains(strings.Join(applied, ","), ":1,") {
		t.Fatalf("applied = %v, want the pins when the template rolls anyway", applied)
	}
	for _, image := range containers {
		if image.Live != "" {
			t.Fatalf("pinned container records a live image: %+v", image)
		}
	}
}

func TestAdoptionBehavesAsBeforeWhenLiveImagesCannotBeRead(t *testing.T) {
	fake := &adoptFake{clusterFake: &clusterFake{}, liveErr: errors.New("boom"), unchanged: true}
	_, _, applied, err := adoptDeploy(t, fake)
	if err != nil {
		t.Fatal(err)
	}
	for _, image := range applied {
		if !strings.Contains(image, "@sha256:") {
			t.Fatalf("applied images = %v, want every image pinned", applied)
		}
	}
}

func TestVerifyAcceptedReadsKeptImagesAsTheirLiveForm(t *testing.T) {
	fake := &adoptFake{clusterFake: &clusterFake{}, live: adoptLive(imagesrctest.Digest("web:1")), unchanged: true}
	fake.live.Containers[ContainerSite{Name: "web"}] = LiveContainer{Image: "web:1", Digests: []string{imagesrctest.Digest("web:1")}}
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, fake.clusterFake)
	deployer.Applier = fake
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deployer.VerifyAccepted(context.Background(), depHead(t, root), depNamespace, receipt); err != nil {
		t.Fatalf("a kept tag with the recorded pin is the accepted state: %v", err)
	}
}

func TestCommandBackendReportsRunningPodDigestsAndTemplateEquality(t *testing.T) {
	digest := imagesrctest.Digest("web:1")
	deployment := `{"kind":"Deployment","spec":{"selector":{"matchLabels":{"app":"web"}},"template":{"spec":{"containers":[{"name":"web","image":"web:1"}]}}}}`
	pods := `{"items":[{"status":{"phase":"Running","containerStatuses":[{"name":"web","imageID":"docker.io/library/web@` + digest + `"}]}},` +
		`{"status":{"phase":"Running","containerStatuses":[{"name":"web","imageID":"` + digest + `"}]}},{"status":{"phase":"Succeeded"}}]}`
	template := `{"spec":{"template":{"spec":{"containers":[{"name":"web","image":"web@` + digest + `"}]}}}}`
	var dryRun string
	backend := CommandBackend{Run: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.Contains(joined, "get pods"):
			if !strings.Contains(joined, "--selector app=web") {
				t.Errorf("pods are selected by the workload's selector: %s", joined)
			}
			return []byte(pods), nil
		case strings.Contains(joined, "get Deployment"):
			return []byte(deployment), nil
		case strings.Contains(joined, "--dry-run=server"):
			return []byte(dryRun), nil
		}
		return nil, errors.New("unexpected " + joined)
	}}
	ref := sdk.ObjectRef{Kind: "Deployment", Namespace: "demo", Name: "web"}
	live, err := backend.LiveImages(context.Background(), ref)
	if err != nil || !live.Found {
		t.Fatalf("LiveImages = %+v, %v", live, err)
	}
	got := live.Containers[ContainerSite{Name: "web"}]
	if got.Image != "web:1" || len(got.Digests) != 2 || got.Digests[0] != digest || got.Digests[1] != digest {
		t.Fatalf("web = %+v, want the digest of each running pod and none for the finished one", got)
	}
	dryRun = template
	if same, err := backend.TemplateUnchanged(context.Background(), ref, []byte("---\nkind: Deployment\n")); err != nil || same {
		t.Fatalf("TemplateUnchanged with a different live template = %v, %v", same, err)
	}
	dryRun = deployment
	if same, err := backend.TemplateUnchanged(context.Background(), ref, []byte("---\nkind: Deployment\n")); err != nil || !same {
		t.Fatalf("TemplateUnchanged with the same template = %v, %v", same, err)
	}
}
