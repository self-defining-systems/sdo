package sdk

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func deploymentFields(t *testing.T, mutate func(spec map[string]any)) []byte {
	t.Helper()
	spec := map[string]any{
		"containers":     []any{map[string]any{"name": "profile", "image": "hotel/profile:1", "command": []any{"profile"}, "ports": []any{map[string]any{"containerPort": float64(8081)}}}},
		"initContainers": []any{map[string]any{"name": "migrate", "image": "hotel/migrate:1"}},
	}
	if mutate != nil {
		mutate(spec)
	}
	data, err := json.Marshal(map[string]any{"spec": map[string]any{"replicas": float64(1), "template": map[string]any{"spec": spec}}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func sourceDiff(accepted, observed []byte) SourceDiff {
	return SourceDiff{Changed: string(accepted) != string(observed), Text: SourceComparisonText(accepted, observed)}
}

// The p024 shape: an added init container carries an image, the app image is
// unchanged. A whole-document substring match on "image" calls this image drift.
func TestAddedInitContainerIsNotAppImageDrift(t *testing.T) {
	accepted := deploymentFields(t, nil)
	observed := deploymentFields(t, func(spec map[string]any) {
		spec["initContainers"] = append(spec["initContainers"].([]any), map[string]any{"name": "block", "image": "busybox", "command": []any{"sleep", "infinity"}})
	})
	diff := sourceDiff(accepted, observed)
	if !strings.Contains(diff.Text, "image") {
		t.Fatal("setup: the text carries an image")
	}
	changes, err := diff.ContainerChanges()
	if err != nil {
		t.Fatal(err)
	}
	want := []ContainerChange{{Kind: InitContainer, Name: "block", Added: true}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("changes = %+v, want %+v", changes, want)
	}
	for _, change := range changes {
		if change.Kind == AppContainer && change.Changed("image") {
			t.Fatal("an added init container reads as app image drift")
		}
	}
}

// Every container list and declared field is reported as exactly that kind,
// name and field; nothing else changes.
func TestContainerChangesNameExactlyTheChangedField(t *testing.T) {
	for _, list := range []struct {
		kind ContainerKind
		key  string
	}{{AppContainer, "containers"}, {InitContainer, "initContainers"}} {
		accepted := deploymentFields(t, nil)
		var spec map[string]any
		_ = json.Unmarshal(accepted, &spec)
		container := spec["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)[list.key].([]any)[0].(map[string]any)
		for field := range container {
			if field == "name" {
				continue
			}
			observed := deploymentFields(t, func(spec map[string]any) {
				target := spec[list.key].([]any)[0].(map[string]any)
				target[field] = "changed"
			})
			changes, err := sourceDiff(accepted, observed).ContainerChanges()
			if err != nil {
				t.Fatal(err)
			}
			want := []ContainerChange{{Kind: list.kind, Name: container["name"].(string), Fields: []string{field}}}
			if !reflect.DeepEqual(changes, want) {
				t.Fatalf("%s %s: changes = %+v, want %+v", list.key, field, changes, want)
			}
		}
	}
}

func TestContainerChangesIgnoreServerDefaultsAndUnchangedPeers(t *testing.T) {
	accepted := deploymentFields(t, nil)
	defaulted := deploymentFields(t, func(spec map[string]any) {
		for _, key := range []string{"containers", "initContainers"} {
			spec[key].([]any)[0].(map[string]any)["imagePullPolicy"] = "IfNotPresent"
		}
	})
	changes, err := sourceDiff(accepted, defaulted).ContainerChanges()
	if err != nil || len(changes) != 0 {
		t.Fatalf("server defaults reported as changes: %+v %v", changes, err)
	}
	peer := sourceDiff(accepted, accepted)
	if changes, err := peer.ContainerChanges(); err != nil || len(changes) != 0 {
		t.Fatalf("unchanged peer reported changes: %+v %v", changes, err)
	}
}

func TestContainerChangesMatchByNameNotPosition(t *testing.T) {
	accepted := deploymentFields(t, nil)
	observed := deploymentFields(t, func(spec map[string]any) {
		spec["containers"] = append([]any{map[string]any{"name": "sidecar", "image": "proxy"}}, spec["containers"].([]any)...)
	})
	changes, err := sourceDiff(accepted, observed).ContainerChanges()
	want := []ContainerChange{{Kind: AppContainer, Name: "sidecar", Added: true}}
	if err != nil || !reflect.DeepEqual(changes, want) {
		t.Fatalf("changes = %+v %v, want %+v", changes, err, want)
	}
	removed, err := sourceDiff(observed, accepted).ContainerChanges()
	if err != nil || len(removed) != 1 || !removed[0].Removed || removed[0].Name != "sidecar" {
		t.Fatalf("removal = %+v %v", removed, err)
	}
}

func TestFieldDiffersOnlyForDeclaredFields(t *testing.T) {
	accepted := deploymentFields(t, nil)
	observed := deploymentFields(t, func(spec map[string]any) { spec["nodeSelector"] = map[string]any{"disk": "ssd"} })
	diff := sourceDiff(accepted, observed)
	diff.Text += "\nchanged since acceptance: spec.template.spec.nodeSelector"
	added, err := diff.Field("spec.template.spec.nodeSelector")
	if err != nil || added.Declared || !added.Live || added.Differs() {
		t.Fatalf("undeclared live field: %+v %v", added, err)
	}
	replicas, err := diff.Field("spec.replicas")
	if err != nil || !replicas.Declared || replicas.Differs() {
		t.Fatalf("unchanged declared field: %+v %v", replicas, err)
	}
	gone := sourceDiff(accepted, []byte("null"))
	if field, err := gone.Field("spec.replicas"); err != nil || !field.Differs() || field.Live {
		t.Fatalf("absent live object: %+v %v", field, err)
	}
}

func TestDiffWithoutComparisonIsAnError(t *testing.T) {
	for _, diff := range []SourceDiff{{}, {Changed: true, Text: `{"kind":"Deployment"}`}} {
		if _, err := diff.ContainerChanges(); !errors.Is(err, ErrNoFieldComparison) {
			t.Fatalf("%q: err = %v", diff.Text, err)
		}
	}
}
