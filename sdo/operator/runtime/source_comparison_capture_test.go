//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// flagSourceView compares the live ConfigMap value with accepted source. A
// projected capture keeps key names only, so comparing it reads as drift.
type flagSourceView struct {
	program.ReadView
	snapshot sdk.DetectionContext
}

func (v flagSourceView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	for _, cm := range v.snapshot.ConfigMaps() {
		if cm.Name == ref.Name {
			return program.Diff{Changed: cm.Data["flag"] != "off"}, nil
		}
	}
	return program.Diff{Changed: true}, nil
}

func (v flagSourceView) AdmissionWebhooksFor(sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	ready := 0
	if v.flag() == "off" {
		ready = 1
	}
	return []program.AdmissionWebhook{{Webhook: "policy.example", ReadyEndpoints: ready}}, nil
}

func (v flagSourceView) flag() string {
	for _, cm := range v.snapshot.ConfigMaps() {
		return cm.Data["flag"]
	}
	return ""
}

func flagSnapshot(value string) sdktest.Snapshot {
	return sdktest.Snapshot{NamespaceName: "demo", ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "flags", Namespace: "demo"}, Data: map[string]string{"flag": value}}}}
}

// The controller records every object's source comparison and the admission
// webhooks at detect and repeats them on the live fixed-boundary snapshot, so a drift program replays
// Match on the fault and NoMatch once fixed whatever the responder touched.
func TestControllerRecordsSourceComparisonsOnLiveSnapshots(t *testing.T) {
	root := t.TempDir()
	config := testControllerConfig()
	config.Namespace = "demo"
	config.CorpusSessionRoot = root
	provider := &changingProvider{snapshot: flagSnapshot("on")}
	detector := controllerDetector("health", time.Second, stateFinding("health"), stateFinding("health"), sdk.Finding{}, sdk.Finding{})
	detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	detector.spec.OriginatingCommit = "health-objective"
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(config, []sdk.Detector{detector}, provider, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{View: func(s sdk.DetectionContext) program.ReadView {
		return flagSourceView{ReadView: program.ReadFrom(s), snapshot: s}
	}})
	for i := 0; i < 2; i++ {
		if err := controller.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	dir, err := memory.IncidentCorpusDir(root, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo", Name: "flags"}
	recorded := func(phase, method string, argument, result any) bool {
		raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
		if err != nil {
			return false
		}
		var calls []sdktest.PrimitiveCall
		if json.Unmarshal(raw, &calls) != nil {
			return false
		}
		args, _ := json.Marshal([]any{argument})
		for _, call := range calls {
			if call.Method == method && call.Phase == phase && string(call.Arguments) == string(args) {
				return json.Unmarshal(call.Result, result) == nil
			}
		}
		return false
	}
	comparison := func(phase string) *program.Diff {
		var diff program.Diff
		if !recorded(phase, "DiffAgainstSource", ref, &diff) {
			return nil
		}
		return &diff
	}
	webhooks := func(phase string) []program.AdmissionWebhook {
		var webhooks []program.AdmissionWebhook
		recorded(phase, "AdmissionWebhooksFor", sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: "demo"}, &webhooks)
		return webhooks
	}
	deadline := time.Now().Add(5 * time.Second)
	for comparison("") == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if diff := comparison(""); diff == nil || !diff.Changed {
		t.Fatalf("detect recorded no faulted comparison for an untouched object: %+v", diff)
	}
	if w := webhooks(""); len(w) != 1 || w[0].ReadyEndpoints != 0 {
		t.Fatalf("detect recorded no admission webhook backends: %+v", w)
	}
	provider.snapshot = flagSnapshot("off")
	awaitDispatchCompletionQueued(t, controller)
	for i := 2; i < 19; i++ {
		if err := controller.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	if diff := comparison("fixed"); diff == nil || diff.Changed {
		t.Fatalf("fixed comparison was not read from the live repaired snapshot: %+v", diff)
	}
	if w := webhooks("fixed"); len(w) != 1 || w[0].ReadyEndpoints != 1 {
		t.Fatalf("fixed boundary did not repeat the admission webhook read: %+v", w)
	}
}
