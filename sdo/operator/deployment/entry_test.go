package deployment

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

func entrySet(objects ...map[string]any) rendered.DesiredSet {
	var desired []rendered.DesiredObject
	for _, o := range objects {
		u := &unstructured.Unstructured{Object: o}
		desired = append(desired, rendered.DesiredObject{Ref: sdk.ObjectRef{APIVersion: u.GetAPIVersion(), Kind: u.GetKind(), Name: u.GetName()}, Object: u})
	}
	return rendered.DesiredSet{Phases: []rendered.DesiredPhase{{Name: "main", Objects: desired}}}
}

func service(name, kind string, clusterIP string, ports ...map[string]any) map[string]any {
	list := make([]any, 0, len(ports))
	for _, p := range ports {
		list = append(list, p)
	}
	spec := map[string]any{"ports": list}
	if kind != "" {
		spec["type"] = kind
	}
	if clusterIP != "" {
		spec["clusterIP"] = clusterIP
	}
	return map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": name}, "spec": spec}
}

func port(number int64, name string) map[string]any {
	return map[string]any{"port": number, "name": name}
}

func TestEntryPointsAreTheExposedServicesElseEveryServiceWithAnIP(t *testing.T) {
	ingress := map[string]any{"apiVersion": "networking.k8s.io/v1", "kind": "Ingress", "metadata": map[string]any{"name": "web"},
		"spec": map[string]any{"rules": []any{map[string]any{"http": map[string]any{"paths": []any{
			map[string]any{"backend": map[string]any{"service": map[string]any{"name": "frontend"}}}}}}}}}
	exposed := entrySet(service("frontend", "", "", port(5000, "web")), service("db", "", "", port(5432, "pg")),
		service("api", "NodePort", "", port(8080, "")), ingress)
	if got, want := EntryPoints(exposed), []EntryPoint{{"api", 8080, true}, {"frontend", 5000, false}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("exposed entries = %v, want %v", got, want)
	}
	internal := entrySet(service("web", "", "", port(80, "")), service("cache", "", "None", port(6379, "")), service("metrics", "", "", port(9000, "http-metrics")))
	if got, want := EntryPoints(internal), []EntryPoint{{"metrics", 9000, true}, {"web", 80, true}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexposed entries = %v, want %v (headless Services are not entries)", got, want)
	}
}

type flakyDialer struct{ failures map[string]int }

func (f *flakyDialer) Answer(_ context.Context, _ string, e EntryPoint) error {
	if f.failures[e.Service] > 0 {
		f.failures[e.Service]--
		return errors.New("connection refused")
	}
	return nil
}

func TestEntryProbeRetriesUntilEveryEntryAnswersOrTheBoundPasses(t *testing.T) {
	slept := time.Duration(0)
	d := Deployer{Namespace: "app", EntryWait: 20 * time.Second, Sleep: func(_ context.Context, step time.Duration) error { slept += step; return nil }}
	entries := []EntryPoint{{"web", 80, true}, {"api", 8080, true}}
	d.EntryDialer = &flakyDialer{failures: map[string]int{"web": 2}}
	for _, r := range d.probeEntries(context.Background(), entries) {
		if r.Err != nil {
			t.Fatalf("entry %s still failing after retries: %v", r.Entry, r.Err)
		}
	}
	slept = 0
	d.EntryDialer = &flakyDialer{failures: map[string]int{"web": 100}}
	results := d.probeEntries(context.Background(), entries)
	if results[0].Err == nil || results[1].Err != nil || slept > d.EntryWait {
		t.Fatalf("results = %+v after %s, want web failing within the %s bound", results, slept, d.EntryWait)
	}
}

func TestEntryCheckStatuses(t *testing.T) {
	ready := lifecycle.DeploymentReceipt{Ready: true, ResourceIDs: []string{"Deployment/app/web"}}
	for name, tc := range map[string]struct {
		in     LiveInput
		status operability.Status
		cause  operability.Cause
	}{
		"no dialer":      {LiveInput{Receipt: ready}, operability.StatusSkipped, ""},
		"no entries":     {LiveInput{Receipt: ready, EntryChecked: true}, operability.StatusNote, ""},
		"all answer":     {LiveInput{Receipt: ready, EntryChecked: true, Entries: []EntryResult{{Entry: EntryPoint{"web", 80, true}}}}, operability.StatusPass, ""},
		"one is refused": {LiveInput{Receipt: ready, EntryChecked: true, Entries: []EntryResult{{Entry: EntryPoint{"web", 80, true}, Err: errors.New("GET / answered 503")}}}, operability.StatusFail, operability.CauseRollout},
		"deploy failed":  {LiveInput{Err: errors.New("apply failed")}, operability.StatusSkipped, ""},
	} {
		t.Run(name, func(t *testing.T) {
			var got operability.Check
			for _, c := range LiveChecks(tc.in) {
				if c.ID == "O1.entry" {
					got = c
				}
			}
			if got.Status != tc.status || got.Cause != tc.cause {
				t.Fatalf("O1.entry = %+v, want %s %s", got, tc.status, tc.cause)
			}
			if tc.status == operability.StatusFail && !strings.Contains(got.Detail, "web:80") {
				t.Fatalf("detail %q does not name the failing entry", got.Detail)
			}
		})
	}
}
