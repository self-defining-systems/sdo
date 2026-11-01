//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"io"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"path/filepath"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func TestRunCapturesRoutingWithoutTrafficWorkloads(t *testing.T) {
	for _, synthetic := range []bool{true, false} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[synthetic], func(t *testing.T) {
			probe := noTrafficRoutingProber(t)
			address, err := probe.BaseURL(context.Background(), false)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := routingCaptureSnapshot()
			client := fake.NewSimpleClientset(&snapshot.ServiceList[0], &snapshot.PodList[0])
			finding := sdk.Finding{RuleID: "routing", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
				PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Summary: "routing unavailable", Evidence: "connection refused"}
			detector := controllerDetector("health", 20*time.Millisecond, finding)
			detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
			detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
			detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
			detector.spec.OriginatingCommit = "health-objective"
			if len(TrafficWorkloadNames([]sdk.Detector{detector})) != 0 {
				t.Fatal("fixture must have no traffic consumers")
			}
			root := gitRepository(t)
			sessionRoot := t.TempDir()
			repository, err := broker.New(root, t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			ledgerRoot := t.TempDir()
			launch := &contracts.ControllerConfig{Namespace: "app", ControlNamespace: "app-sdo", AppRoot: root,
				Application: "routing", SyntheticTraffic: proto.Bool(synthetic), StateBaseline: proto.Bool(false),
				CleanResponderHelpers: proto.Bool(false), LeaseDuration: durationpb.New(3 * time.Second)}
			dispatcher := routingCaptureDispatcher{requests: make(chan IncidentRequest, 1)}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			exited := make(chan error, 1)
			go func() {
				exited <- RunWithOptions(ctx, []sdk.Detector{detector}, RuntimeOptions{
					Args: controllerConfigToArgs(launch), Traffic: HTTPProberClient{BaseURL: StaticProberURL(address), Client: probe.Client}, Client: client, Dispatcher: dispatcher, Stdout: io.Discard, Stderr: io.Discard,
					NativeBroker: &NativeIncidentBroker{Repository: repository, LedgerRoot: ledgerRoot, SessionRoot: sessionRoot},
				})
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-exited:
				case <-time.After(5 * time.Second):
					t.Error("controller did not stop")
				}
			})
			var request IncidentRequest
			select {
			case request = <-dispatcher.requests:
			case err := <-exited:
				exited <- err
				t.Fatalf("controller exited before capture: %v", err)
			case <-ctx.Done():
				t.Fatal("controller did not dispatch")
			}
			dir, err := memory.IncidentCorpusDir(sessionRoot, request.IncidentID)
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
			replay := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
			result, err := replay.ProbeTCP(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}, 8080)
			if err != nil || !result.Success {
				t.Fatalf("no-traffic generated config lost routing observations: %+v %v; calls=%s", result, err, raw)
			}
			result, err = replay.ProbeTCP(finding.PrimaryResource, 80)
			if err != nil || result.Success {
				t.Fatalf("finding Service observation missing: %+v %v", result, err)
			}
		})
	}
}
