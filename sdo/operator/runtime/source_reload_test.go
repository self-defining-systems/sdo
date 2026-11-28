package runtime

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// reloadView is a replay view whose ConfigMap cfg carries live content and
// the field managers that wrote it.
type reloadView struct {
	restoreView
	writtenAt map[string]time.Time
	// content is cfg's live data (default: the accepted value).
	content string
	// extra are later writes by other managers (FieldsV1 JSON by manager).
	extra []metav1.ManagedFieldsEntry
}

const (
	reloadSourceValue  = `{"pool":8}`
	reloadForeignValue = `{"pool":0}`
)

func (v reloadView) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object, _ := v.restoreView.Object(ref)
	if ref.Kind != "ConfigMap" {
		return object, nil
	}
	_ = unstructured.SetNestedStringMap(object.Object, map[string]string{"config.json": cmp.Or(v.content, reloadSourceValue)}, "data")
	entries := slices.Clone(v.extra)
	if at, ok := v.writtenAt[ref.Kind+"/"+ref.Name]; ok {
		entries = append(entries, metav1.ManagedFieldsEntry{Manager: sdk.ResponderFieldManager, Operation: metav1.ManagedFieldsOperationApply, Time: &metav1.Time{Time: at},
			FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:data":{"f:config.json":{}}}`)}})
	}
	object.SetManagedFields(entries)
	return object, nil
}

var (
	reloadChanged  = detectOnset.Add(-2 * time.Minute)
	reloadRestored = detectOnset.Add(time.Minute)
)

// reloadWorld: ConfigMap cfg is read by Deployment api through how, and by
// the data store StatefulSet db through a subPath mount. Each runs one Pod
// started at the given time.
func reloadWorld(how string, apiStarted, dbStarted time.Time, diff sdk.SourceDiff, restoredAt time.Time) reloadView {
	volume := corev1.Volume{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cfg"}}}}
	container := corev1.Container{Name: "main"}
	switch how {
	case "subPath":
		container.VolumeMounts = []corev1.VolumeMount{{Name: "config", MountPath: "/etc/app/config.json", SubPath: "config.json"}}
	case "mount":
		container.VolumeMounts = []corev1.VolumeMount{{Name: "config", MountPath: "/etc/app"}}
	case "env":
		container.Env = []corev1.EnvVar{{Name: "POOL", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "cfg"}, Key: "pool"}}}}
	case "envFrom":
		container.EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cfg"}}}}
	}
	template := func(name string, c corev1.Container) corev1.PodTemplateSpec {
		return corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{volume}, Containers: []corev1.Container{c}}}
	}
	dbContainer := corev1.Container{Name: "main", VolumeMounts: []corev1.VolumeMount{{Name: "config", MountPath: "/etc/db/db.conf", SubPath: "db.conf"}}}
	pod := func(name string, started time.Time, spec corev1.PodSpec) corev1.Pod {
		return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-0", Namespace: "app", Labels: map[string]string{"app": name}, CreationTimestamp: metav1.NewTime(started)},
			Spec: spec, Status: corev1.PodStatus{StartTime: &metav1.Time{Time: started}}}
	}
	api, db := template("api", container), template("db", dbContainer)
	snapshot := sdktest.Snapshot{NamespaceName: "app",
		DeploymentList:  []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: api}}},
		StatefulSetList: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "app"}, Spec: appsv1.StatefulSetSpec{Template: db}}},
		PodList:         []corev1.Pod{pod("api", apiStarted, api.Spec), pod("db", dbStarted, db.Spec)},
		ApplicationMap: &program.AppMap{Components: []program.AppComponent{
			{Name: "api", Workload: "Deployment/api"},
			{Name: "db", Workload: "StatefulSet/db", HoldsData: true, Data: &program.DataRole{Engine: "mongodb", Role: "authoritative"}},
		}},
	}
	written := map[string]time.Time{}
	if !restoredAt.IsZero() {
		written["ConfigMap/cfg"] = restoredAt
	}
	return reloadView{restoreView: restoreView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot}, diffs: map[string]sdk.SourceDiff{"ConfigMap/cfg": diff}}, writtenAt: written}
}

func reloadEvents() []sdk.DriftEvent {
	foreign := departureEvent(dref("ConfigMap", "cfg"), "kubectl", reloadChanged, "data.config.json")
	foreign.Fields[0].Old, foreign.Fields[0].Source, foreign.Fields[0].New = digestJSON(reloadSourceValue), digestJSON(reloadSourceValue), digestJSON(reloadForeignValue)
	return []sdk.DriftEvent{
		foreign,
		departureEvent(dref("ConfigMap", "cfg"), sdk.ResponderFieldManager, reloadRestored, "data.config.json"),
	}
}

func cfgDepartures(view reloadView) []SourceDeparture {
	return SourceDepartures(DepartureInput{View: view, Events: reloadEvents(), Onset: detectOnset}, []sdk.ObjectRef{dref("ConfigMap", "cfg")})
}

// A ConfigMap written back to accepted source still departs while a workload
// that reads it only at start (subPath, env, envFrom) runs a Pod started with
// the changed content; it clears once that workload's Pods restarted after the
// write, and never for a mount Kubernetes refreshes or a Pod started before
// the change.
func TestRestoredConfigMapDepartsUntilItsStartOnlyReadersRestart(t *testing.T) {
	stale := reloadChanged.Add(10 * time.Second)
	for _, how := range []string{"subPath", "env", "envFrom"} {
		view := reloadWorld(how, stale, reloadChanged.Add(-time.Hour), sdk.SourceDiff{}, reloadRestored)
		got := cfgDepartures(view)
		if len(got) != 1 || got[0].Kind != DepartureNotReloaded || len(got[0].Consumers) != 1 || got[0].Consumers[0].Name != "api" {
			t.Fatalf("%s: departures %+v, want cfg not reloaded by api", how, got)
		}
		if command := got[0].Command(); command != "sdo mitigate RestartRollout --bind ref=Deployment/app/api" {
			t.Fatalf("%s: command %q", how, command)
		}
		if !strings.Contains(got[0].Text(), "Deployment/api") {
			t.Fatalf("%s: text %q names no reader", how, got[0].Text())
		}
		if fresh := cfgDepartures(reloadWorld(how, reloadRestored.Add(5*time.Second), reloadChanged.Add(-time.Hour), sdk.SourceDiff{}, reloadRestored)); len(fresh) != 0 {
			t.Fatalf("%s: Pods restarted after the write still depart: %+v", how, fresh)
		}
	}
	if got := cfgDepartures(reloadWorld("mount", stale, reloadChanged.Add(-time.Hour), sdk.SourceDiff{}, reloadRestored)); len(got) != 0 {
		t.Fatalf("a refreshed mount departs: %+v", got)
	}
	if got := cfgDepartures(reloadWorld("subPath", reloadChanged.Add(-time.Hour), reloadChanged.Add(-time.Hour), sdk.SourceDiff{}, reloadRestored)); len(got) != 0 {
		t.Fatalf("a Pod started before the change departs: %+v", got)
	}
}

// The built-in restore restarts the non-data readers that loaded the changed
// content after it restores the object, never a data store's, and reports
// the departure cleared only once the new Pods run the restored content.
func TestSourceRestoreRestartsStartOnlyReadersAndClearsOnlyOnNewPods(t *testing.T) {
	stale := reloadChanged.Add(10 * time.Second)
	request := detectRequest()
	request.DriftEvents = reloadEvents()[:1]
	restore := sourceRestoreProgram{evidence: newRestoreEvidence(request)}
	bindings := program.Bindings{Values: map[string]any{SourceRestoreComponentParam: "api"}}

	changed := reloadWorld("subPath", stale, stale, sdk.SourceDiff{Changed: true}, time.Time{})
	plan, err := restore.Mitigate(context.Background(), changed, bindings)
	if err != nil || len(plan.Actions) != 2 {
		t.Fatalf("plan %+v, err %v", plan, err)
	}
	if action, ok := plan.Actions[0].(program.ReconcileFromSource); !ok || action.Ref.Name != "cfg" {
		t.Fatalf("first action %#v, want the ConfigMap restore", plan.Actions[0])
	}
	if action, ok := plan.Actions[1].(program.RestartRollout); !ok || action.Ref.Kind != "Deployment" || action.Ref.Name != "api" {
		t.Fatalf("second action %#v, want a restart of api only (db holds data)", plan.Actions[1])
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("plan invalid: %v", err)
	}

	request.DriftEvents = reloadEvents()
	restore = sourceRestoreProgram{evidence: newRestoreEvidence(request)}
	restored := reloadWorld("subPath", stale, reloadChanged.Add(-time.Hour), sdk.SourceDiff{}, reloadRestored)
	if verdict, _ := restore.Diagnose(context.Background(), restored, bindings); verdict.Result != program.Match {
		t.Fatalf("restored object with old Pods: %+v, want still matching", verdict)
	}
	rolled := reloadWorld("subPath", reloadRestored.Add(3*time.Second), reloadChanged.Add(-time.Hour), sdk.SourceDiff{}, reloadRestored)
	if verdict, _ := restore.Diagnose(context.Background(), rolled, bindings); verdict.Result != program.NoMatch {
		t.Fatalf("new Pods run the restored content: %+v, want cleared", verdict)
	}

}

// A data store that still runs the departed content is reported to the
// responder, with no restart command: it never makes the built-in restore
// match, so the restore neither plans a step that errors nor stays matched.
func TestDataStoreReadersNeverMakeTheRestoreMatch(t *testing.T) {
	stale := reloadChanged.Add(10 * time.Second)
	dbOnly := reloadWorld("mount", stale, stale, sdk.SourceDiff{}, reloadRestored)
	got := cfgDepartures(dbOnly)
	if len(got) != 1 || got[0].Kind != DepartureNotReloaded || len(got[0].Consumers) != 0 || len(got[0].DataConsumers) != 1 {
		t.Fatalf("departures %+v, want cfg not reloaded by the data store only", got)
	}
	if command := got[0].Command(); command != "" {
		t.Fatalf("command %q, want none for a data store", command)
	}
	if text := got[0].Text(); !strings.Contains(text, "StatefulSet/db") || !strings.Contains(text, "responder's call") || strings.Contains(text, "sdo mitigate") {
		t.Fatalf("text %q", text)
	}
	request := detectRequest()
	request.DriftEvents = reloadEvents()
	restore := sourceRestoreProgram{evidence: newRestoreEvidence(request)}
	for _, component := range []string{"api", "db"} {
		bindings := program.Bindings{Values: map[string]any{SourceRestoreComponentParam: component}}
		if verdict, _ := restore.Diagnose(context.Background(), dbOnly, bindings); verdict.Result != program.NoMatch {
			t.Fatalf("%s: %+v, want no match when only a data store still runs the changed content", component, verdict)
		}
	}
	// A changed ConfigMap read by a data store too: the restore plans the
	// object write and the non-data restart only.
	changed := reloadWorld("subPath", stale, stale, sdk.SourceDiff{Changed: true}, time.Time{})
	request.DriftEvents = reloadEvents()[:1]
	restore = sourceRestoreProgram{evidence: newRestoreEvidence(request)}
	plan, err := restore.Mitigate(context.Background(), changed, program.Bindings{Values: map[string]any{SourceRestoreComponentParam: "db"}})
	if err != nil || len(plan.Actions) != 2 {
		t.Fatalf("plan %+v, err %v", plan, err)
	}
	for _, action := range plan.Actions {
		if restart, ok := action.(program.RestartRollout); ok && restart.Ref.Name == "db" {
			t.Fatalf("plan restarts the data store: %+v", plan)
		}
	}
}

// What a Pod loaded is judged by content: a later write that did not change
// the data keys (a label by another manager) is not a restore, and content
// that still equals what the foreign change wrote (an engineer adopted it in
// source) is what the Pods already run.
func TestReloadIsJudgedByContentNotAnyLaterWrite(t *testing.T) {
	afterRestore := reloadRestored.Add(30 * time.Second)
	labeled := reloadWorld("subPath", afterRestore.Add(-10*time.Second), reloadChanged.Add(-time.Hour), sdk.SourceDiff{}, reloadRestored)
	labeled.extra = []metav1.ManagedFieldsEntry{{Manager: "labeler", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: afterRestore},
		FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:labels":{"f:team":{}}}}`)}}}
	if got := cfgDepartures(labeled); len(got) != 0 {
		t.Fatalf("Pod started after the restore is flagged by a later label write: %+v", got)
	}
	adopted := reloadWorld("subPath", reloadChanged.Add(10*time.Second), reloadChanged.Add(-time.Hour), sdk.SourceDiff{}, afterRestore)
	adopted.content = reloadForeignValue
	if got := cfgDepartures(adopted); len(got) != 0 {
		t.Fatalf("Pod running the adopted content is flagged: %+v", got)
	}
	labelOnly := reloadWorld("subPath", reloadChanged.Add(10*time.Second), reloadChanged.Add(-time.Hour), sdk.SourceDiff{Changed: true}, time.Time{})
	events := []sdk.DriftEvent{departureEvent(dref("ConfigMap", "cfg"), "kubectl", reloadChanged, "metadata.labels.team")}
	got := SourceDepartures(DepartureInput{View: labelOnly, Events: events, Onset: detectOnset}, []sdk.ObjectRef{dref("ConfigMap", "cfg")})
	if len(got) != 1 || len(got[0].Consumers) != 0 {
		t.Fatalf("a label-only change names start-only readers: %+v", got)
	}
}

// Pods still running changed content are left by the automatic restore only if
// the application was healthy with them: the content was written back before
// the healthy stretch and is unchanged since. A healthy stretch before that
// write says nothing about what they run.
func TestNotReloadedIsSeenHealthyOnlyWhenWrittenBackBeforeTheHealthyStretch(t *testing.T) {
	view := reloadWorld("subPath", reloadChanged.Add(10*time.Second), reloadChanged.Add(-time.Hour), sdk.SourceDiff{}, reloadRestored)
	for name, test := range map[string]struct {
		since time.Time
		want  bool
	}{
		"written back before the stretch": {reloadRestored.Add(time.Minute), true},
		"written back after the stretch":  {reloadRestored.Add(-time.Second), false},
	} {
		events := reloadEvents()
		events[0].HealthyContent, events[0].HealthyContentSince = objectContent(view, events[0].Ref), &test.since
		got := SourceDepartures(DepartureInput{View: view, Events: events, Onset: detectOnset}, []sdk.ObjectRef{dref("ConfigMap", "cfg")})
		if len(got) != 1 || got[0].Kind != DepartureNotReloaded || got[0].SeenHealthy != test.want {
			t.Fatalf("%s: %+v, want seen healthy %t", name, got, test.want)
		}
	}
}
