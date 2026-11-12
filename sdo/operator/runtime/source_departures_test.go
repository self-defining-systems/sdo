package runtime

import (
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
)

// departureView is a synthetic snapshot with canned source comparisons and a
// set of live objects.
type departureView struct {
	detectFake
	diffs map[string]sdk.SourceDiff
	gone  map[string]bool
	// written gives a live object its managedFields (manager to write time).
	written map[string][]metav1.ManagedFieldsEntry
}

func (v departureView) DiffAgainstSource(ref sdk.ObjectRef) (sdk.SourceDiff, error) {
	if diff, ok := v.diffs[ref.Kind+"/"+ref.Name]; ok {
		return diff, nil
	}
	return sdk.SourceDiff{}, errors.New("not compared")
}

func (v departureView) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if v.gone[ref.Kind+"/"+ref.Name] {
		return nil, nil
	}
	object := &unstructured.Unstructured{}
	if entries, ok := v.written[ref.Kind+"/"+ref.Name]; ok {
		object.SetManagedFields(entries)
	}
	return object, nil
}

func wrote(manager string, at time.Time, subresource string) metav1.ManagedFieldsEntry {
	stamp := metav1.NewTime(at)
	return metav1.ManagedFieldsEntry{Manager: manager, Operation: metav1.ManagedFieldsOperationApply, Time: &stamp, Subresource: subresource}
}

func dref(kind, name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: kind, Namespace: "app", Name: name}
}

func departureEvent(ref sdk.ObjectRef, manager string, at time.Time, path string) sdk.DriftEvent {
	return sdk.DriftEvent{Ref: ref, Manager: manager, ManagerAt: &at, ObservedAt: at, Fields: []sdk.DriftField{{Path: path}}}
}

func TestSourceDeparturesAreNonSDOChangesThatStillDiffer(t *testing.T) {
	before := detectOnset.Add(-4 * time.Second)
	view := departureView{detectFake: detectApp(), diffs: map[string]sdk.SourceDiff{
		"Service/api":       {Changed: true},
		"Deployment/front":  {Changed: true},
		"ConfigMap/clean":   {Changed: false},
		"NetworkPolicy/new": {Changed: true, SourceAbsent: true},
		"ConfigMap/lost":    {Changed: true},
		"Service/late":      {Changed: true},
		"Deployment/db":     {Changed: true},
	}, gone: map[string]bool{"ConfigMap/lost": true}}
	events := []sdk.DriftEvent{
		departureEvent(dref("Service", "api"), "kubectl", before, "spec.ports"),
		departureEvent(dref("Deployment", "front"), sdk.ResponderFieldManager, before, "spec.replicas"),
		departureEvent(dref("NetworkPolicy", "new"), "kubectl", before, "spec"),
		departureEvent(dref("Service", "late"), "kubectl", detectOnset.Add(time.Minute), "spec"),
		departureEvent(dref("ConfigMap", "clean"), "kubectl", before, "data"),
	}
	objects := []sdk.ObjectRef{dref("Service", "api"), dref("Deployment", "front"), dref("ConfigMap", "clean"), dref("NetworkPolicy", "new"), dref("ConfigMap", "lost"), dref("Service", "late"), dref("Deployment", "db")}
	got := map[string]DepartureKind{}
	for _, departure := range SourceDepartures(DepartureInput{View: view, Events: events, Onset: detectOnset}, objects) {
		got[departure.Ref.Kind+"/"+departure.Ref.Name] = departure.Kind
	}
	want := map[string]DepartureKind{"Service/api": DepartureChanged, "NetworkPolicy/new": DepartureExtra, "ConfigMap/lost": DepartureMissing}
	if len(got) != len(want) {
		t.Fatalf("departures = %v, want %v", got, want)
	}
	for key, kind := range want {
		if got[key] != kind {
			t.Fatalf("departures = %v, want %v", got, want)
		}
	}
	// SDO's own writes, an object that matches source, a change after onset, and a
	// difference no event attributes are all excluded.
}

func TestSourceDeparturesNeverMatchWhileDeploying(t *testing.T) {
	before := detectOnset.Add(-4 * time.Second)
	view := departureView{detectFake: detectApp(), diffs: map[string]sdk.SourceDiff{"Service/api": {Changed: true}}}
	events := []sdk.DriftEvent{departureEvent(dref("Service", "api"), "kubectl", before, "spec")}
	input := DepartureInput{View: view, Events: events, Onset: detectOnset, DeploymentInFlight: true}
	if got := SourceDepartures(input, []sdk.ObjectRef{dref("Service", "api")}); len(got) != 0 {
		t.Fatalf("a departure during a deployment is SDO's own work: %v", got)
	}
}

func TestSourceDeparturesCompareSecretsByMetadataOnly(t *testing.T) {
	changed := detectOnset.Add(-time.Minute)
	app := detectApp()
	app.SecretList = []sdk.SecretMetadata{{Name: "creds", Namespace: "app", DataChangedAt: changed}, {Name: "untouched", Namespace: "app"}}
	// DiffAgainstSource would error on a Secret; the classifier must not call it.
	view := departureView{detectFake: app}
	got := SourceDepartures(DepartureInput{View: view, Onset: detectOnset}, []sdk.ObjectRef{dref("Secret", "creds"), dref("Secret", "untouched")})
	if len(got) != 1 || got[0].Kind != DepartureSecretData || got[0].Ref.Name != "creds" {
		t.Fatalf("departures = %v", got)
	}
	if got[0].Command() != "" || !strings.Contains(got[0].Text(), "values not compared") {
		t.Fatalf("a Secret has no restore command and says its values were not compared: %q", got[0].Text())
	}
}

func TestDepartureCommandsUseTheExistingBuiltInActions(t *testing.T) {
	for kind, want := range map[DepartureKind]string{
		DepartureChanged: "sdo mitigate ReconcileFromSource --bind ref=Service/app/api",
		DepartureMissing: "sdo mitigate CreateFromSource --bind ref=Service/app/api",
		DepartureExtra:   "sdo mitigate DeleteObject --bind ref=Service/app/api",
	} {
		if got := (SourceDeparture{Ref: dref("Service", "api"), Kind: kind}).Command(); got != want {
			t.Fatalf("%s: %q, want %q", kind, got, want)
		}
	}
}

// The detect line is the classifier's output, not a second implementation.
func TestDetectChangeLineShowsTheSourceDepartureAndHowToRestoreIt(t *testing.T) {
	view := departureView{detectFake: detectApp("create"), diffs: map[string]sdk.SourceDiff{"NetworkPolicy/restrict-db": {Changed: true, SourceAbsent: true}}}
	request := detectRequest()
	request.Findings = []sdk.Finding{probeFinding("create")}
	request.OpeningKeys = []string{findingKey(request.Findings[0])}
	text := buildDetectSummary(detectInput{Request: request, Findings: request.Findings, View: view, Now: detectNow()}).Text
	for _, want := range []string{"NetworkPolicy/restrict-db present at 07:28:05Z by kubectl (not sdo); absent from accepted source", "more: sdo mitigate DeleteObject --bind ref=NetworkPolicy/app/restrict-db"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q:\n%s", want, text)
		}
	}
	if len(text) > detectSummaryCap {
		t.Fatalf("summary is %d bytes", len(text))
	}
}

func closedIncidentController(t *testing.T) *Controller {
	t.Helper()
	config := testControllerConfig()
	config.SourceCommit, config.DeployedCommit = "closure-commit", "start-up-commit"
	c, err := NewController(config, nil, staticProvider{}, &recordingDispatcher{}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// A closure moves the source commit; the deployed commit stays. That alone is
// not a rollout, so the classifier must keep working after the first incident.
func TestSourceDeparturesStillClassifyAfterAClosureMovedTheSourceCommit(t *testing.T) {
	c := closedIncidentController(t)
	request := c.incidentRequest(detectNow(), nil)
	if request.SourceCommit == request.DeployedCommit || request.DeploymentInFlight {
		t.Fatalf("a moved source commit is not a rollout: %+v", request)
	}
	before := detectOnset.Add(-4 * time.Second)
	request.DriftEvents = []sdk.DriftEvent{departureEvent(dref("Service", "api"), "kubectl", before, "spec.ports")}
	view := departureView{detectFake: detectApp(), diffs: map[string]sdk.SourceDiff{"Service/api": {Changed: true}}}
	got := SourceDepartures(departureInput(request, view, detectOnset), []sdk.ObjectRef{dref("Service", "api")})
	if len(got) != 1 || got[0].Manager != "kubectl" {
		t.Fatalf("departures after a closure = %v", got)
	}
}

func TestDeploymentInFlightFollowsTheControllersRolloutState(t *testing.T) {
	c := closedIncidentController(t)
	if c.deploymentInFlight() {
		t.Fatal("no rollout is pending")
	}
	c.reflections.rollout = "inc-1"
	if !c.deploymentInFlight() || !c.incidentRequest(detectNow(), nil).DeploymentInFlight {
		t.Fatal("a queued reflection's rollout that has not converged is a deployment in flight")
	}
	c.reflections.rollout, c.reflections.unknown = "", true
	if !c.deploymentInFlight() {
		t.Fatal("an unreadable ledger must not be trusted as converged")
	}
}

func TestSourceDeparturesIgnoreAForeignEventSDOHasSinceOverwritten(t *testing.T) {
	old := detectOnset.Add(-time.Hour)
	view := departureView{detectFake: detectApp(), diffs: map[string]sdk.SourceDiff{
		"Service/api": {Changed: true}, "Service/front": {Changed: true},
	}, written: map[string][]metav1.ManagedFieldsEntry{
		"Service/front": {wrote("kubectl", old, ""), wrote(sdk.ResponderFieldManager, detectOnset.Add(-time.Minute), ""), wrote("kube-controller-manager", detectOnset, "status")},
	}}
	events := []sdk.DriftEvent{
		departureEvent(dref("Service", "api"), "kubectl", old, "spec"),
		departureEvent(dref("Service", "api"), sdk.DeployerFieldManager, detectOnset.Add(-time.Minute), "spec"),
		departureEvent(dref("Service", "front"), "kubectl", old, "spec"),
	}
	got := SourceDepartures(DepartureInput{View: view, Events: events, Onset: detectOnset}, []sdk.ObjectRef{dref("Service", "api"), dref("Service", "front")})
	if len(got) != 0 {
		t.Fatalf("an old foreign write must not be blamed for what SDO changed later: %v", got)
	}
}

func TestMissingObjectsNeedTheLatestWriterToBeForeign(t *testing.T) {
	before := detectOnset.Add(-4 * time.Second)
	view := departureView{detectFake: detectApp(), diffs: map[string]sdk.SourceDiff{
		"Service/deleted-by-sdo": {Changed: true}, "Service/deleted-by-user": {Changed: true}, "Service/not-yet": {Changed: true},
	}, gone: map[string]bool{"Service/deleted-by-sdo": true, "Service/deleted-by-user": true, "Service/not-yet": true}}
	events := []sdk.DriftEvent{
		departureEvent(dref("Service", "deleted-by-sdo"), "kubectl", before.Add(-time.Hour), "metadata"),
		departureEvent(dref("Service", "deleted-by-sdo"), sdk.ResponderFieldManager, before, "metadata"),
		departureEvent(dref("Service", "deleted-by-user"), "kubectl", before, "metadata"),
	}
	objects := []sdk.ObjectRef{dref("Service", "deleted-by-sdo"), dref("Service", "deleted-by-user"), dref("Service", "not-yet")}
	got := map[string]SourceDeparture{}
	for _, departure := range SourceDepartures(DepartureInput{View: view, Events: events, Onset: detectOnset}, objects) {
		got[departure.Ref.Name] = departure
	}
	if _, ok := got["deleted-by-sdo"]; ok || len(got) != 2 {
		t.Fatalf("departures = %v", got)
	}
	if user := got["deleted-by-user"]; user.Unattributed || user.Command() != "sdo mitigate CreateFromSource --bind ref=Service/app/deleted-by-user" || !strings.Contains(user.Text(), "by kubectl (not sdo)") {
		t.Fatalf("a foreign deletion is attributed and restorable: %+v", user)
	}
	if pending := got["not-yet"]; !pending.Unattributed || pending.Command() != "" || strings.Contains(pending.Text(), "not sdo") {
		t.Fatalf("a missing object no event explains is information only: %+v %q", pending, pending.Text())
	}
	view.diffs["Service/not-yet"] = sdk.SourceDiff{Changed: true}
	if got := SourceDepartures(DepartureInput{View: view, Events: events, Onset: detectOnset, DeploymentInFlight: true}, objects); len(got) != 0 {
		t.Fatalf("nothing is missing yet while an advance is in flight: %v", got)
	}
}

func TestSecretDepartureNeedsAReadingPodAndNoLaterSDOWrite(t *testing.T) {
	changed := detectOnset.Add(-time.Minute)
	app := detectApp()
	app.SecretList = []sdk.SecretMetadata{
		{Name: "read", Namespace: "app", DataChangedAt: changed}, {Name: "unread", Namespace: "app", DataChangedAt: changed},
		{Name: "rewritten", Namespace: "app", DataChangedAt: changed},
	}
	started := metav1.NewTime(changed.Add(-time.Hour))
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "app", CreationTimestamp: started}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main",
		EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "read"}}}},
		Env:     []corev1.EnvVar{{Name: "X", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "rewritten"}}}}}}}}}
	app.PodList = []corev1.Pod{pod}
	events := []sdk.DriftEvent{departureEvent(dref("Secret", "rewritten"), sdk.DeployerFieldManager, changed.Add(time.Second), "data")}
	got := SourceDepartures(DepartureInput{View: departureView{detectFake: app}, Events: events, Onset: detectOnset}, []sdk.ObjectRef{dref("Secret", "read"), dref("Secret", "unread"), dref("Secret", "rewritten")})
	if len(got) != 1 || got[0].Ref.Name != "read" {
		t.Fatalf("departures = %v", got)
	}
}
