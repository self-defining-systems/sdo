package runtime

import (
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// healthyWorld is a live app whose Services all depart from accepted source;
// content is each one's live selector, and gone marks a deleted one.
type healthyWorld struct {
	*sdktest.ReplayReadView
	content map[string]string
	gone    map[string]bool
}

func (w healthyWorld) DiffAgainstSource(sdk.ObjectRef) (sdk.SourceDiff, error) {
	return sdk.SourceDiff{Changed: true}, nil
}

func (w healthyWorld) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if w.gone[ref.Name] {
		return nil, fmt.Errorf("%w: %s", sdk.ErrObjectAbsent, ref.Name)
	}
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind,
		"metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace},
		"spec":     map[string]any{"selector": w.content[ref.Name]}}}, nil
}

func healthyController(t *testing.T, start int64, world healthyWorld, probe sdk.Detector) *Controller {
	t.Helper()
	c := startupController(t, 0, start, probe)
	c.ConfigureDrift(DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView { return world }})
	return c
}

// driftFindings are the drift findings that link the history into a request.
func driftFindings(c *Controller) []sdk.Finding {
	var findings []sdk.Finding
	for i := range c.driftEvents {
		c.driftEvents[i].Classification = sdk.DriftUnexplained
		event := c.driftEvents[i]
		findings = append(findings, sdk.Finding{DetectorID: "drift", Status: sdk.FindingActive, Metadata: map[string]any{"drift_event": event}})
	}
	return findings
}

// exempt is, per departed Service, whether the automatic restore would leave it
// as seen healthy, from the request an incident opening now would carry.
func exempt(c *Controller, world healthyWorld, names ...string) map[string]bool {
	request := c.incidentRequest(at(1000), driftFindings(c))
	var refs []sdk.ObjectRef
	for _, name := range names {
		refs = append(refs, dref("Service", name))
	}
	marks := map[string]bool{}
	for _, departure := range SourceDepartures(departureInput(request, world, at(1000)), refs) {
		marks[departure.Ref.Name] = departure.SeenHealthy
	}
	return marks
}

func stepRange(t *testing.T, c *Controller, from, to int64) {
	t.Helper()
	for second := from; second <= to; second++ {
		step(t, c, second)
	}
}

func newHealthyWorld(content map[string]string) healthyWorld {
	return healthyWorld{ReplayReadView: &sdktest.ReplayReadView{}, content: content, gone: map[string]bool{}}
}

// A departure counts as seen healthy once the application stayed healthy for a
// whole settle window that began after the change was observed: not before,
// and never while it fails. The mark reaches the incident request and survives
// a restart.
func TestDriftPresentThroughAHealthySettleIsSeenHealthy(t *testing.T) {
	probe := probeSamples(400, func(int) bool { return false })
	world := newHealthyWorld(map[string]string{"old": "a", "recent": "a"})
	c := healthyController(t, 0, world, probe)
	c.driftEvents = []sdk.DriftEvent{
		departureEvent(dref("Service", "old"), "kubectl", at(0), "spec"),
		departureEvent(dref("Service", "recent"), "kubectl", at(5), "spec"),
	}
	stepRange(t, c, 0, 119)
	if got := exempt(c, world, "old", "recent"); got["old"] || got["recent"] {
		t.Fatalf("marked before a settle window passed: %v", got)
	}
	step(t, c, 120)
	if got := exempt(c, world, "old", "recent"); !got["old"] || got["recent"] {
		t.Fatalf("after the first settled window %v, want only old", got)
	}
	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	restarted := healthyController(t, 130, world, probe)
	if err := restarted.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if got := exempt(restarted, world, "old", "recent"); !got["old"] || got["recent"] {
		t.Fatalf("after a restart %v, want only old", got)
	}
	stepRange(t, c, 121, 240)
	if got := exempt(c, world, "old", "recent"); !got["recent"] {
		t.Fatalf("after the next settled window %v, want recent too", got)
	}
}

// Only the content the healthy stretch saw is exempt. A later write that makes
// no drift event of its own (another manager owns the field) still qualifies
// for the restore, and the seen content is exempt again when it returns.
func TestSeenHealthyExemptsOnlyTheContentSeenHealthy(t *testing.T) {
	world := newHealthyWorld(map[string]string{"changed": "a", "deleted": "a", "gone": ""})
	world.gone["gone"] = true
	c := healthyController(t, 0, world, probeSamples(400, func(int) bool { return false }))
	for _, name := range []string{"changed", "deleted", "gone"} {
		c.driftEvents = append(c.driftEvents, departureEvent(dref("Service", name), "kubectl", at(0), "spec"))
	}
	stepRange(t, c, 0, 120)
	if got := exempt(c, world, "changed", "deleted", "gone"); !got["changed"] || !got["deleted"] || !got["gone"] {
		t.Fatalf("after a settled window %v, want all exempt", got)
	}
	world.content["changed"] = "b"
	world.gone["deleted"] = true
	if got := exempt(c, world, "changed", "deleted", "gone"); got["changed"] || got["deleted"] || !got["gone"] {
		t.Fatalf("after later changes %v, want only the unchanged absence exempt", got)
	}
	// A later event for the same object keeps what was seen healthy.
	c.appendDriftEvent(departureEvent(dref("Service", "changed"), "kubectl", at(121), "spec"))
	if got := exempt(c, world, "changed"); got["changed"] {
		t.Fatalf("different content exempt after a new event: %v", got)
	}
	world.content["changed"] = "a"
	if got := exempt(c, world, "changed"); !got["changed"] {
		t.Fatalf("the content seen healthy returned and is not exempt: %v", got)
	}
}

// Content that appears during a settle window was not seen healthy for a whole
// window: the stretch that saw only part of it does not exempt it.
func TestContentChangedDuringTheWindowIsNotSeenHealthyYet(t *testing.T) {
	world := newHealthyWorld(map[string]string{"api": "a"})
	c := healthyController(t, 0, world, probeSamples(400, func(int) bool { return false }))
	c.driftEvents = []sdk.DriftEvent{departureEvent(dref("Service", "api"), "kubectl", at(0), "spec")}
	stepRange(t, c, 0, 60)
	world.content["api"] = "b"
	stepRange(t, c, 61, 120)
	if got := exempt(c, world, "api"); got["api"] {
		t.Fatalf("content present for half a window exempt: %v", got)
	}
	stepRange(t, c, 121, 240)
	if got := exempt(c, world, "api"); !got["api"] {
		t.Fatalf("content present for a whole window not exempt: %v", got)
	}
}

func TestDriftIsNotSeenHealthyWhileTheApplicationFails(t *testing.T) {
	world := newHealthyWorld(map[string]string{"api": "a"})
	c := healthyController(t, 0, world, probeSamples(300, func(int) bool { return true }))
	c.driftEvents = []sdk.DriftEvent{departureEvent(dref("Service", "api"), "kubectl", at(0), "spec")}
	stepRange(t, c, 0, 250)
	if got := exempt(c, world, "api"); len(got) != 1 || got["api"] {
		t.Fatalf("a failing application marked it seen healthy: %v", got)
	}
}
