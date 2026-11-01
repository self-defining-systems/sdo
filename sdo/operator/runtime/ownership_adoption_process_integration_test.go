//go:build integration

package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"testing"
	"time"
)

func TestControllerAdoptsAvailableObjectsWithExplicitRenderGap(t *testing.T) {
	controller, api, _, closeServer := adoptionController(t, 8080)
	defer closeServer()
	source, _, commit, _, _ := acceptedSourceDeployment(t, true)
	source.Images = nil // A cold process has no healthy workload image evidence yet.
	controller.config.SourceCommit = commit
	controller.drift.Source = source
	api.service.Spec.Selector = map[string]string{"app": "api"}
	controller.drift.View = func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(snapshot), Source: source, SourceCommit: commit, Client: controller.drift.Client}
	}
	start := time.Unix(10, 0).UTC()
	if err := controller.StepEvents(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	status := exportState(t, controller).OwnershipAdoption
	if status.AdoptedObjects != 1 || status.LastError == "" || !controller.adoptionNeeded || status.RetryAt == nil {
		t.Fatalf("partial adoption hid coverage gap or lost service: %+v", status)
	}
	before := api.service.DeepCopy()
	api.service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	controller.drift.Events = &fixedObjectChanges{changes: []ObjectChange{objectChange(t, before, &api.service, "v1", "Service")}}
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	events := exportState(t, controller).DriftEvents
	if len(events) != 1 || events[0].Classification != string(sdk.DriftUnexplained) {
		t.Fatalf("partial render gap silenced service drift: %+v", events)
	}
	if err := controller.StepEvents(context.Background(), start.Add(22*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if !controller.IncidentOpen() || exportState(t, controller).IncidentRequest.Findings[0].DetectorId != "drift.unexplained" {
		t.Fatalf("service drift never opened an incident: %+v", exportState(t, controller))
	}
}
