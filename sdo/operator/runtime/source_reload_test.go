package runtime

import (
	"context"
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

// reloadView is a replay view whose objects carry a last-write time.
type reloadView struct {
	restoreView
	writtenAt map[string]time.Time
}

func (v reloadView) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object, _ := v.restoreView.Object(ref)
	if at, ok := v.writtenAt[ref.Kind+"/"+ref.Name]; ok {
		object.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: sdk.ResponderFieldManager, Operation: metav1.ManagedFieldsOperationApply, Time: &metav1.Time{Time: at}}})
	}
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
	return []sdk.DriftEvent{
		departureEvent(dref("ConfigMap", "cfg"), "kubectl", reloadChanged, "data.config.json"),
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

	// Only the data store still runs the changed content: the built-in
	// restore writes nothing and says why, so the responder owns it.
	dbOnly := reloadWorld("mount", stale, stale, sdk.SourceDiff{}, reloadRestored)
	dbBindings := program.Bindings{Values: map[string]any{SourceRestoreComponentParam: "db"}}
	if verdict, _ := restore.Diagnose(context.Background(), dbOnly, dbBindings); verdict.Result != program.Match {
		t.Fatalf("data store still on the changed content: %+v, want matching", verdict)
	}
	if _, err := restore.Mitigate(context.Background(), dbOnly, dbBindings); err == nil || !strings.Contains(err.Error(), "data store") {
		t.Fatalf("err %v, want a refusal to restart a data store", err)
	}
}
