package sdktest

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/routing"
	"testing"
	"time"
)

func sharedRoutingCorpus(t *testing.T) CorpusCase {
	c := renameFixture(t)
	for _, snapshot := range []*Snapshot{&c.Open, &c.Fixed, &c.Healthy} {
		for i := range snapshot.PodList {
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
func TestSharedRoutingMitigatorRequiresRecordedListenersAndReplays(t *testing.T) {
	c := sharedRoutingCorpus(t)
	p := routing.Program{}
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
func TestSharedRoutingMitigatorCannotInferListenerFromDeclaration(t *testing.T) {
	for _, tc := range []string{"declared port closed", "target also listens", "missing target observation", "unready pod"} {
		t.Run(tc, func(t *testing.T) {
			c := sharedRoutingCorpus(t)
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
			verdict := program.RunDiagnose(context.Background(), routing.Program{}, &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, c.Bindings, time.Second)
			if verdict.Result != program.Unknown {
				t.Fatalf("insufficient listener proof matched: %+v", verdict)
			}
		})
	}
}
