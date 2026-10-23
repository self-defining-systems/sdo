package sdktest

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/routingport"
	"testing"
	"time"
)

func routingCorpus(t *testing.T) CorpusCase {
	c := renameFixture(t)
	for _, snapshot := range []*Snapshot{&c.Open, &c.Fixed, &c.Healthy} {
		for i := range snapshot.PodList {
			snapshot.PodList[i].ResourceVersion = "fixture"
			snapshot.PodList[i].Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		}
	}
	for _, phase := range []string{"open", "fixed"} {
		for _, pod := range c.Open.Pods() {
			for _, port := range []int32{8080, 9999} {
				args, _ := json.Marshal([]any{sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, port})
				result, _ := json.Marshal(program.ProbeResult{Success: port == 8080})
				c.Recordings = append(c.Recordings, PrimitiveCall{Phase: phase, Method: "ProbeTCP", Arguments: args, Result: result})
			}
		}
	}
	return c
}
func TestRoutingMitigatorRequiresRecordedListenersAndReplays(t *testing.T) {
	c := routingCorpus(t)
	p := routingport.Program{}
	CheckGeneralization(t, p, c, c.PeerBindings)
	CheckRenameInvariance(t, p, c)
	CheckMitigatorGeneralization(t, p, c)
	if result, err := CheckMitigation(p, p, c); err != nil || result.Status != MitigationReplayPassed {
		t.Fatalf("routing repair replay: %+v %v", result, err)
	}
	c.Recordings = nil
	verdict := program.RunDiagnose(context.Background(), p, &ReplayReadView{Snapshot: c.Open}, c.Bindings, time.Second)
	if verdict.Result != program.Unknown {
		t.Fatalf("declarations alone established listener: %+v", verdict)
	}
}
func TestRoutingMitigatorCannotInferListenerFromDeclaration(t *testing.T) {
	for _, tc := range []string{"declared port closed", "target also listens", "missing target observation", "unready pod"} {
		t.Run(tc, func(t *testing.T) {
			c := routingCorpus(t)
			switch tc {
			case "declared port closed":
				for i := range c.Recordings {
					c.Recordings[i].Result = json.RawMessage(`{"success":false}`)
				}
			case "target also listens":
				for i := range c.Recordings {
					c.Recordings[i].Result = json.RawMessage(`{"success":true}`)
				}
			case "missing target observation":
				var kept []PrimitiveCall
				for _, call := range c.Recordings {
					var args []json.RawMessage
					_ = json.Unmarshal(call.Arguments, &args)
					if string(args[1]) != "9999" {
						kept = append(kept, call)
					}
				}
				c.Recordings = kept
			case "unready pod":
				for i := range c.Open.PodList {
					c.Open.PodList[i].Status.Conditions = nil
				}
			}
			verdict := program.RunDiagnose(context.Background(), routingport.Program{}, &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, c.Bindings, time.Second)
			if verdict.Result != program.Unknown {
				t.Fatalf("insufficient listener proof matched: %+v", verdict)
			}
		})
	}
}

func TestRoutingReplayDoesNotReusePodReadsForOtherEdits(t *testing.T) {
	c := routingCorpus(t)
	ref := c.Bindings.Objects["service"]
	m := fixtureMitigator{actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)}, program.SetField{Ref: ref, Path: "spec.selector", Value: testPlanValue(map[string]any{"app": "front"})}}}
	if _, err := CheckMitigation(routingport.Program{}, m, c); err == nil {
		t.Fatal("Pod TCP observation survived non-targetPort edit")
	}
}

func TestInstalledRoutingExampleCorpusReplays(t *testing.T) {
	c, err := LoadCorpusCase("../../../../examples/playbook-programs/corpus/targetport-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	p := routingport.Program{}
	CheckGeneralization(t, p, c, c.PeerBindings)
	CheckRenameInvariance(t, p, c)
	CheckMitigatorGeneralization(t, p, c)
	if result, err := CheckMitigation(p, p, c); err != nil || result.Status != MitigationReplayPassed {
		t.Fatalf("installed corpus repair: %+v %v", result, err)
	}
}
