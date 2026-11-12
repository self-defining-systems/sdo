package runtime

import (
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
)

// departureView is a synthetic snapshot with canned source comparisons and a
// set of live objects.
type departureView struct {
	detectFake
	diffs map[string]sdk.SourceDiff
	gone  map[string]bool
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
	return &unstructured.Unstructured{}, nil
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

