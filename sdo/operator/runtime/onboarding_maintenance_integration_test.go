//go:build integration

package runtime

import (
	"context"
	"io"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunOnboardingMaintenanceGenerationPreservesSamples(t *testing.T) {
	for _, synthetic := range []string{"true", "false"} {
		t.Run(synthetic, func(t *testing.T) {
			var resets atomic.Int32
			handler := noTrafficProberHandler(t)
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == prober.PathReset {
					resets.Add(1)
				}
				handler.ServeHTTP(w, r)
			}))
			defer api.Close()
			snapshot := routingCaptureSnapshot()
			client := fake.NewSimpleClientset(&snapshot.ServiceList[0], &snapshot.PodList[0])
			detector := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Watches: servicehealth.ReachabilityWatches(), Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "lifecycle-bootstrap", Playbooks: []string{"health-objective"}})
			reader, writer := io.Pipe()
			lines := &lockedLines{}
			go collectLines(reader, lines)
			ctx, cancel := context.WithCancel(context.Background())
			exited := make(chan error, 1)
			root := gitRepository(t)
			go func() {
				exited <- RunWithOptions(ctx, []sdk.Detector{detector, trafficDetector("traffic-health", "health")}, RuntimeOptions{Args: []string{"--namespace", "app", "--control-namespace", "app-sdo", "--app-root", root, "--prober-url", api.URL, "--synthetic-traffic=" + synthetic, "--synthetic-traffic-warmup", "1ms", "--state-baseline=false", "--clean-responder-helpers=false", "--maintenance-poll-interval", "5ms", "--lease-duration", "3s"}, Client: client, Dispatcher: routingCaptureDispatcher{}, Stdout: writer, Stderr: io.Discard})
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-exited:
				case <-time.After(5 * time.Second):
					t.Error("controller did not stop")
				}
				_ = writer.Close()
				_ = reader.Close()
			})
			eventually(t, "reachability startup", func() bool {
				select {
				case err := <-exited:
					exited <- err
					t.Fatalf("controller startup: %v; %s", err, lines.String())
				default:
				}
				return strings.Contains(lines.String(), `"service_reachability":"started"`)
			})
			initialResets := resets.Load()
			expectedInitialResets := int32(0)
			if synthetic == "true" {
				expectedInitialResets = 1
			}
			if initialResets != expectedInitialResets {
				t.Fatalf("fixture did not start the selected traffic observer: %d", initialResets)
			}
			if _, err := client.CoreV1().ConfigMaps("app-sdo").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: MaintenanceConfigMapName, Namespace: "app-sdo"}, Data: map[string]string{"state": "active", "generation": "same-active-mode"}}, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			eventually(t, "active generation acknowledgement", func() bool { return strings.Contains(lines.String(), `"maintenance_generation":"same-active-mode"`) })
			if starts := strings.Count(lines.String(), `"service_reachability":"started"`); starts != 1 {
				t.Fatalf("generation-only update restarted sampling: starts=%d", starts)
			}
			if resets.Load() != initialResets {
				t.Fatal("generation-only update reset synthetic samples")
			}
			cm, err := client.CoreV1().ConfigMaps("app-sdo").Get(ctx, MaintenanceConfigMapName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			cm.Data = map[string]string{"state": "paused", "generation": "actual-pause"}
			if _, err := client.CoreV1().ConfigMaps("app-sdo").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			eventually(t, "actual pause", func() bool { return strings.Contains(lines.String(), `"maintenance_generation":"actual-pause"`) })
			cm, err = client.CoreV1().ConfigMaps("app-sdo").Get(ctx, MaintenanceConfigMapName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			cm.Data = map[string]string{"state": "active", "generation": "actual-resume"}
			if _, err := client.CoreV1().ConfigMaps("app-sdo").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			eventually(t, "actual resume", func() bool { return strings.Contains(lines.String(), `"maintenance_generation":"actual-resume"`) })
			if starts := strings.Count(lines.String(), `"service_reachability":"started"`); starts != 2 {
				t.Fatalf("true resume did not restart sampling: starts=%d", starts)
			}
			if synthetic == "true" && resets.Load() != initialResets+1 {
				t.Fatalf("true resume did not reset synthetic samples: initial=%d final=%d logs=%s", initialResets, resets.Load(), lines.String())
			}
		})
	}
}

