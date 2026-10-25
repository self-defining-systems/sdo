package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func cleanInstallerInventory() ([]*unstructured.Unstructured, []*unstructured.Unstructured) {
	declared, live := hotelInventory()
	for i, name := range []string{"mongo-geo-script", "mongo-rate-script"} {
		_ = unstructured.SetNestedSlice(declared[i*3].Object, []any{map[string]any{"name": "script", "configMap": map[string]any{"name": name}}}, "spec", "template", "spec", "volumes")
		live[i*8] = declared[i*3].DeepCopy()
		live = append(live, inventoryObject("v1", "ConfigMap", "app", name))
	}
	for _, name := range []string{"failure-admin-geo", "failure-admin-rate", "wrk2-payload-script"} {
		live = append(live, inventoryObject("v1", "ConfigMap", "app", name))
	}
	for _, name := range []string{"jaeger-agent", "jaeger-collector", "jaeger-query"} {
		live = append(live, inventoryObject("v1", "Service", "app", name))
	}
	live = append(live, inventoryObject("batch/v1", "Job", "app", "wrk2-job"), inventoryChild("v1", "Pod", "wrk2-pod", "Job", "wrk2-job"))
	rbac := latentInventoryObjects()[3:]
	rbac[0].SetName("configmap-cleanup-controller")
	rbac[1].SetName("configmap-cleanup-controller")
	rbac[1].Object["roleRef"].(map[string]any)["name"] = "configmap-cleanup-controller"
	return declared, append(live, rbac...)
}
func TestSourceInventoryOnboardingCleanThenLaterControllerBlocksAndRestartPreserves(t *testing.T) {
	declared, live := cleanInstallerInventory()
	index := NewSourceInventory("app", strings.Repeat("a", 40), declared)
	before := index.Findings(live, time.Unix(99, 0))
	if len(before) != 10 {
		t.Fatalf("fixture must expose ten pre-existing extras after declared dependencies: %d", len(before))
	}
	record := &contracts.NamespaceOnboarding{Namespace: "app", AcceptedCommit: strings.Repeat("a", 40), EstablishedAt: timestamppb.New(time.Unix(100, 0))}
	for _, finding := range before {
		record.Objects = append(record.Objects, objectRefContract(finding.PrimaryResource))
	}
	c, err := NewController(testControllerConfig(), []sdk.Detector{inventoryHealth()}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	c.config.Namespace = "app"
	c.ConfigureDrift(DriftControllerConfig{Inventory: true, Source: &inventorySource{objects: declared}, Events: &inventoryStore{objects: live}})
	if err := c.SetNamespaceOnboarding(record); err != nil {
		t.Fatal(err)
	}
	if err := c.prepareSourceInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.observeSourceInventory(time.Unix(101, 0))
	if len(c.sourceInventoryFindings) != 0 {
		t.Fatalf("clean namespace created response work: %+v", c.sourceInventoryFindings)
	}
	// The acknowledged Job may produce new children after onboarding.
	live = append(live, inventoryChild("v1", "Pod", "wrk2-later-pod", "Job", "wrk2-job"))
	additions := latentInventoryObjects()
	live = append(live, additions[0], additions[3], additions[4])
	c.drift.Events.(*inventoryStore).objects = live
	c.observeSourceInventory(time.Unix(102, 0))
	if ok, remaining := c.verifySourceInventoryLocked(nil); ok || len(remaining) != 3 {
		t.Fatalf("later controller/RBAC must block closure: %v %+v", ok, remaining)
	}
	state := exportState(t, c)
	restarted, _ := NewController(c.config, []sdk.Detector{inventoryHealth()}, c.provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(103, 0))
	restarted.ConfigureDrift(*c.drift)
	if err := restarted.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if err := restarted.prepareSourceInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted.observeSourceInventory(time.Unix(104, 0))
	if len(restarted.sourceInventoryFindings) != 3 {
		t.Fatalf("restart lost acknowledgement or accepted later fault: %+v", restarted.sourceInventoryFindings)
	}
	request := restarted.incidentRequest(time.Unix(104, 0), restarted.sourceInventoryFindings)
	wire, err := incidentRequestContract(request)
	if err != nil {
		t.Fatal(err)
	}
	roundtrip := incidentRequestState(wire)
	if len(wire.GetNamespaceOnboarding().GetObjects()) != 10 || len(roundtrip.NamespaceOnboarding.GetObjects()) != 10 {
		t.Fatal("incident and reflection closure request lost separate acknowledgement")
	}
	changed := record.ProtoReflect().New().Interface().(*contracts.NamespaceOnboarding)
	changed.Namespace = "app"
	changed.AcceptedCommit = strings.Repeat("b", 40)
	changed.EstablishedAt = timestamppb.New(time.Unix(200, 0))
	if err := restarted.SetNamespaceOnboarding(changed); err == nil {
		t.Fatal("later event replaced onboarding")
	}
}
