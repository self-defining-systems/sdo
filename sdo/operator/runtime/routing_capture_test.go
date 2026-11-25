package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// bindingProber binds session storage while a probe is in flight, as a standby
// claim does.
type bindingProber struct{ probes, refused int }

func (p *bindingProber) Probe(context.Context, sdk.ObjectRef, int32, *program.HTTPProbe) (program.ProbeResult, error) {
	p.probes++
	release, ok := memory.TrySessionStorageBinding()
	if !ok {
		p.refused++
		return program.ProbeResult{}, nil
	}
	release()
	return program.ProbeResult{Success: true}, nil
}

// A slow or failing probe of a broken Service must not cost the incident its
// warm standby: the claim refuses to wait for session storage.
func TestAStandbyCanBindWhileTheOpeningCaptureProbes(t *testing.T) {
	root, service := t.TempDir(), sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}, {Port: 81}}},
	}}}
	if err := memory.CaptureIncidentSnapshot(root, "incident-1", "open@detect", snapshot, "app", service); err != nil {
		t.Fatal(err)
	}
	prober := &bindingProber{}
	view := LiveReadView{Carrier: inputs.New(snapshot), Context: context.Background(), Prober: prober}
	findings := []sdk.Finding{{PrimaryResource: service}}
	for range 2 { // the second sweep is complete: the marker stops it
		if err := captureRoutingEvidence(context.Background(), root, "incident-1", "open@detect", findings, view); err != nil {
			t.Fatal(err)
		}
	}
	if prober.probes != 2 || prober.refused != 0 {
		t.Fatalf("%d probes, %d refused a standby bind; want 2 probes, 0 refused", prober.probes, prober.refused)
	}
	dir, err := memory.IncidentCorpusDir(root, "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(raw, &calls); err != nil {
		t.Fatal(err)
	}
	recorded := 0
	for _, call := range calls {
		if call.Method == "ProbeTCP" {
			recorded++
		}
	}
	if recorded != 2 {
		t.Fatalf("recorded %d probes, want 2", recorded)
	}
}

// A capture that waits for another one's sweep holds no pin. Correct code can
// never refuse here, whatever the schedule; the yields let a wrong one pin.
func TestACaptureWaitingForAnotherHoldsNoSessionStorage(t *testing.T) {
	routingCaptureMu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = captureRoutingEvidence(context.Background(), t.TempDir(), "incident-1", "open@detect", nil, LiveReadView{})
	}()
	refused := false
	for range 10000 {
		runtime.Gosched()
		if release, ok := memory.TrySessionStorageBinding(); ok {
			release()
		} else {
			refused = true
			break
		}
	}
	routingCaptureMu.Unlock()
	<-done
	if refused {
		t.Fatal("a waiting capture refused a standby bind")
	}
}
