package operatorcli

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/install"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/upstream"
)

const testControl = "sdo-system"

func TestHoldsConfigMapRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := configMapHolds{Client: fake.NewSimpleClientset(), Namespace: testControl}
	if holds, err := store.Load(ctx); err != nil || holds != nil {
		t.Fatalf("absent ConfigMap = %v, %v", holds, err)
	}
	want := []upstream.Hold{{Path: ".", Fingerprint: "0123456789abcdef", Revision: revB, Reason: "rollout: synthetic", At: time.Unix(1_700_000_000, 0).UTC()}}
	for range 2 { // create, then update
		if err := store.Save(ctx, want); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.Load(ctx)
	if err != nil || len(got) != 1 || got[0] != want[0] {
		t.Fatalf("holds = %+v, %v; want %+v", got, err, want)
	}
	if err := store.Save(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(ctx); err != nil || len(got) != 0 {
		t.Fatalf("cleared holds = %+v, %v", got, err)
	}
}

// A record that cannot be decoded is reported apart, never an error that stops
// loading, and Save keeps it for a person while rewriting the valid ones.
func TestObligationsConfigMapQuarantinesAMalformedRecordAndKeepsIt(t *testing.T) {
	ctx := context.Background()
	store := configMapObligations{Client: fake.NewSimpleClientset(), Namespace: testControl}
	valid := upstream.Obligation{Path: ".", Kind: upstream.ObligationDesignerRefresh, Accepted: "c1", Cause: "coverage", Owner: "designer", Reason: "synthetic", Attempts: 1, Since: time.Unix(1_700_000_000, 0).UTC()}
	if err := store.Save(ctx, []upstream.Obligation{valid}); err != nil {
		t.Fatal(err)
	}
	configMap, err := store.Client.CoreV1().ConfigMaps(testControl).Get(ctx, upstream.ObligationsConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	configMap.Data["obligation-0123456789abcdef"] = "{not json"
	if _, err := store.Client.CoreV1().ConfigMaps(testControl).Update(ctx, configMap, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	got, bad, err := store.Load(ctx)
	if err != nil || len(got) != 1 || len(bad) != 1 || bad[0].Key != "obligation-0123456789abcdef" {
		t.Fatalf("Load = %+v, %+v, %v; want the valid obligation and the one quarantined record", got, bad, err)
	}
	// The valid obligation settles; the quarantined record is not dropped.
	if err := store.Save(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got, bad, err = store.Load(ctx); err != nil || len(got) != 0 || len(bad) != 1 {
		t.Fatalf("after Save = %+v, %+v, %v; want the quarantined record kept", got, bad, err)
	}
}

func TestObligationsConfigMapRoundTripAndReset(t *testing.T) {
	ctx := context.Background()
	store := configMapObligations{Client: fake.NewSimpleClientset(), Namespace: testControl}
	if owed, bad, err := store.Load(ctx); err != nil || owed != nil || bad != nil {
		t.Fatalf("absent ConfigMap = %v, %v, %v", owed, bad, err)
	}
	want := []upstream.Obligation{{Path: ".", Kind: upstream.ObligationDesignerRefresh, Accepted: "c1", Cause: "coverage", Owner: "designer", Reason: "synthetic", Attempts: 1, Since: time.Unix(1_700_000_000, 0).UTC()}}
	for range 2 {
		if err := store.Save(ctx, want); err != nil {
			t.Fatal(err)
		}
	}
	if got, bad, err := store.Load(ctx); err != nil || len(bad) != 0 || len(got) != 1 || got[0] != want[0] {
		t.Fatalf("obligations = %+v, %+v, %v", got, bad, err)
	}
	deleted, err := onboarding.ResetControlState(ctx, store.Client, testControl)
	if err != nil || len(deleted) != 1 || deleted[0] != upstream.ObligationsConfigMapName {
		t.Fatalf("reset deleted %v, %v", deleted, err)
	}
}

// A from-scratch reinstall must not inherit holds of the previous run.
func TestResetControlStateClearsHolds(t *testing.T) {
	ctx := context.Background()
	store := configMapHolds{Client: fake.NewSimpleClientset(), Namespace: testControl}
	if err := store.Save(ctx, []upstream.Hold{{Path: ".", Fingerprint: "0123456789abcdef", Revision: revB, Reason: "rollout: synthetic", At: time.Unix(1, 0).UTC()}}); err != nil {
		t.Fatal(err)
	}
	deleted, err := onboarding.ResetControlState(ctx, store.Client, testControl)
	if err != nil || len(deleted) != 1 || deleted[0] != upstream.HoldsConfigMapName {
		t.Fatalf("reset deleted %v, %v", deleted, err)
	}
	if holds, err := store.Load(ctx); err != nil || len(holds) != 0 {
		t.Fatalf("holds after reset = %+v, %v", holds, err)
	}
}

func maintenanceState(t *testing.T, m advanceMaintenance) map[string]string {
	t.Helper()
	cm, err := m.Client.CoreV1().ConfigMaps(testControl).Get(context.Background(), install.MaintenanceConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return cm.Data
}

func TestAdvanceMaintenancePausesAndResumes(t *testing.T) {
	ctx := context.Background()
	m := advanceMaintenance{Client: fake.NewSimpleClientset(), Namespace: testControl, Now: func() time.Time { return time.Unix(1, 0) }}
	if err := m.Pause(ctx, "upstream advance"); err != nil {
		t.Fatal(err)
	}
	if data := maintenanceState(t, m); data["state"] != "paused" || !strings.HasPrefix(data["generation"], advanceMaintenancePrefix) || data["preserve_baseline"] != "" {
		t.Fatalf("paused = %v (the redeploy must be relisted on resume, not preserved)", data)
	}
	if err := m.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if data := maintenanceState(t, m); data["state"] != "active" {
		t.Fatalf("resumed = %v", data)
	}
}

func TestAdvanceNeverLiftsAnotherOwnersPause(t *testing.T) {
	ctx := context.Background()
	m := advanceMaintenance{Client: fake.NewSimpleClientset(), Namespace: testControl, Now: func() time.Time { return time.Unix(1, 0) }}
	if err := install.SetMaintenance(ctx, m.Client, testControl, true, "operator-redeploy"); err != nil {
		t.Fatal(err)
	}
	if err := m.Pause(ctx, "upstream advance"); err == nil {
		t.Fatal("advance paused over a human pause")
	}
	if recovered, err := m.RecoverStalePause(ctx); err != nil || recovered {
		t.Fatalf("recovered a foreign pause: %v, %v", recovered, err)
	}
	if data := maintenanceState(t, m); data["state"] != "paused" || data["generation"] != "operator-redeploy" {
		t.Fatalf("foreign pause changed: %v", data)
	}
}

func TestStaleAdvancePauseIsRecovered(t *testing.T) {
	ctx := context.Background()
	m := advanceMaintenance{Client: fake.NewSimpleClientset(), Namespace: testControl, Now: func() time.Time { return time.Unix(1, 0) }}
	if recovered, err := m.RecoverStalePause(ctx); err != nil || recovered {
		t.Fatalf("nothing to recover: %v, %v", recovered, err)
	}
	if err := m.Pause(ctx, "upstream advance"); err != nil {
		t.Fatal(err)
	}
	if recovered, err := m.RecoverStalePause(ctx); err != nil || !recovered {
		t.Fatalf("stale advance pause: %v, %v", recovered, err)
	}
	if data := maintenanceState(t, m); data["state"] != "active" {
		t.Fatalf("after recovery = %v", data)
	}
}

func stateConfigMap(t *testing.T, state *contracts.ControllerState) *corev1.ConfigMap {
	t.Helper()
	state.Version = "sdo.controller/v1"
	data, err := protojson.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: controllerStateConfigMap, Namespace: testControl}, Data: map[string]string{controllerStateKey: string(data)}}
}

func onboardingConfigMap(t *testing.T, name string) *corev1.ConfigMap {
	t.Helper()
	data, err := protojson.Marshal(&contracts.NamespaceOnboarding{Namespace: "app", AcceptedCommit: revA, EstablishedAt: timestamppb.New(time.Unix(1_700_000_000, 0))})
	if err != nil {
		t.Fatal(err)
	}
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testControl}, Data: map[string]string{onboarding.DataKey: string(data)}}
}

func marker(name string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testControl}}
}

func TestControllerActivity(t *testing.T) {
	open := &contracts.ControllerState{IncidentOpen: true, IncidentRequest: &contracts.IncidentRequest{IncidentId: "inc-1"}}
	closing := &contracts.ControllerState{ClosureState: contracts.ClosureState_CLOSURE_STATE_COMMITTED, ClosureReceipt: &contracts.ClosureReceipt{}}
	failed := &contracts.ControllerState{ClosureState: contracts.ClosureState_CLOSURE_STATE_FAILED, PendingClosure: &contracts.IncidentClosure{Request: &contracts.IncidentRequest{IncidentId: "inc-0"}}, ClosureFailure: &contracts.ClosureFailure{IncidentId: "inc-0", Permanent: true}}
	for name, tc := range map[string]struct {
		objects []*corev1.ConfigMap
		want    string // "" = idle
	}{
		"no state yet":                 {},
		"quiet controller":             {objects: []*corev1.ConfigMap{stateConfigMap(t, &contracts.ControllerState{})}},
		"open incident":                {objects: []*corev1.ConfigMap{stateConfigMap(t, open)}, want: "incident inc-1 is open"},
		"closure in flight":            {objects: []*corev1.ConfigMap{stateConfigMap(t, closing)}, want: "closure"},
		"permanently failed closure":   {objects: []*corev1.ConfigMap{stateConfigMap(t, failed)}},
		"queued reflection":            {objects: []*corev1.ConfigMap{stateConfigMap(t, &contracts.ControllerState{QueuedReflectionIncidentIds: []string{"inc-0"}})}, want: "reflection"},
		"unknown reflection queue":     {objects: []*corev1.ConfigMap{stateConfigMap(t, &contracts.ControllerState{QueuedReflectionsUnknown: true})}, want: "reflection"},
		"pending onboarding":           {objects: []*corev1.ConfigMap{onboardingConfigMap(t, onboarding.PendingConfigMapName)}, want: "onboarding"},
		"completed onboarding":         {objects: []*corev1.ConfigMap{onboardingConfigMap(t, onboarding.PendingConfigMapName), onboardingConfigMap(t, onboarding.ConfigMapName)}},
		"rejected onboarding":          {objects: []*corev1.ConfigMap{onboardingConfigMap(t, onboarding.PendingConfigMapName), marker(onboarding.RejectedConfigMapName)}},
		"onboarding window closed":     {objects: []*corev1.ConfigMap{onboardingConfigMap(t, onboarding.PendingConfigMapName), marker(onboarding.WindowConfigMapName)}},
		"quiet state, pending onboard": {objects: []*corev1.ConfigMap{stateConfigMap(t, &contracts.ControllerState{}), onboardingConfigMap(t, onboarding.PendingConfigMapName)}, want: "onboarding"},
	} {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			for _, object := range tc.objects {
				if _, err := client.CoreV1().ConfigMaps(testControl).Create(context.Background(), object, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			reason, busy, err := controllerActivity{Client: client, Namespace: testControl, Application: "app"}.Busy(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if busy != (tc.want != "") || !strings.Contains(reason, tc.want) {
				t.Fatalf("busy = %v (%q), want %q", busy, reason, tc.want)
			}
		})
	}
}

func TestControllerActivityRefusesUnreadableState(t *testing.T) {
	client := fake.NewSimpleClientset()
	broken := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: controllerStateConfigMap, Namespace: testControl}, Data: map[string]string{controllerStateKey: "{not json"}}
	if _, err := client.CoreV1().ConfigMaps(testControl).Create(context.Background(), broken, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := (controllerActivity{Client: client, Namespace: testControl, Application: "app"}).Busy(context.Background()); err == nil {
		t.Fatal("an unreadable controller state counted as idle")
	}
}

// Open obligations are surfaced in the operability ConfigMap with cause, owner
// and attempts, and cleared (an empty list) when the last one settles.
func TestObligationsAreSurfacedInTheOperabilityConfigMap(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: install.OperabilityConfigMap, Namespace: testControl}, Data: map[string]string{"summary.json": "{}"}})
	store := configMapObligations{Client: client, Namespace: testControl}
	owed := []upstream.Obligation{{Path: ".", Kind: upstream.ObligationDesignerRefresh, Accepted: "c1", Cause: "coverage", Owner: "designer", Reason: "synthetic", Attempts: 3, Since: time.Unix(1_700_000_000, 0).UTC()}}
	if err := store.Save(ctx, owed); err != nil {
		t.Fatal(err)
	}
	configMap, err := client.CoreV1().ConfigMaps(testControl).Get(ctx, install.OperabilityConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"cause":"coverage"`, `"owner":"designer"`, `"attempts":3`, `"kind":"designer_refresh"`} {
		if !strings.Contains(configMap.Data[install.OperabilityObligationsKey], want) {
			t.Fatalf("obligations summary %q lacks %s", configMap.Data[install.OperabilityObligationsKey], want)
		}
	}
	if configMap.Data["summary.json"] != "{}" {
		t.Fatalf("the report summary was changed: %v", configMap.Data)
	}
	if err := store.Save(ctx, nil); err != nil {
		t.Fatal(err)
	}
	configMap, _ = client.CoreV1().ConfigMaps(testControl).Get(ctx, install.OperabilityConfigMap, metav1.GetOptions{})
	if got := configMap.Data[install.OperabilityObligationsKey]; got != "[]" {
		t.Fatalf("settled obligations summary = %q, want []", got)
	}
}
