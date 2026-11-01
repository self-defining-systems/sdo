package runtime

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

type memoryTrafficDoer struct{ broken *atomic.Bool }

func (d memoryTrafficDoer) Do(r *http.Request) (*http.Response, error) {
	code := 200
	if d.broken.Load() {
		code = 503
	}
	return &http.Response{StatusCode: code, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("ok")), Request: r}, nil
}
func (c testMemoryRPC) Traffic(ctx context.Context, r *pb.TrafficRequest) (*pb.TrafficResponse, error) {
	return c.client.Traffic(ctx, r)
}

func TestSimMemoryTrafficTwoRevisions(t *testing.T) {
	sim.Run(t, 47, func(t *testing.T, w *sim.World) {
		ctx := context.Background()
		manager := memoryhost.NewManager(time.Second)
		defer manager.Close()
		if err := manager.Publish(ctx, sdktest.Snapshot{NamespaceName: "toy"}); err != nil {
			t.Fatal(err)
		}
		remote := MemoryTraffic{RPC: manager}
		broken := &atomic.Bool{}
		build := func(id string) (*simActivationHost, *prober.Host, *prober.Prober) {
			catalog := traffic.Catalog{{ID: id, Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead, Steps: []traffic.Step{{Name: "get", Endpoint: traffic.GET("/", nil).Contains("ok")}}}}
			workloads, err := prober.ParseWorkloads(map[string][]byte{"health": []byte(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"health","purpose":"health-probe","seed":47,"ratePerSecond":10,"scenarios":[{"id":"` + id + `"}]}`)})
			if err != nil {
				t.Fatal(err)
			}
			config := prober.Config{Namespace: "toy", Catalog: catalog, Workloads: workloads, Client: memoryTrafficDoer{broken}}
			engine, err := prober.New(config)
			if err != nil {
				t.Fatal(err)
			}
			direct, err := prober.New(config)
			if err != nil {
				t.Fatal(err)
			}
			scheduler := prober.NewHost(engine)
			t.Cleanup(scheduler.Close)
			server, _ := memoryhost.NewServer(id, "digest", nil)
			server.WithTraffic(scheduler)
			rpc, stop := memoryTestRPC(t, server)
			h := &simActivationHost{testMemoryRPC: rpc, stop: func() { scheduler.Close(); stop() }}
			if err := manager.Prepare(ctx, h); err != nil {
				t.Fatal(err)
			}
			return h, prober.NewHost(direct), direct
		}
		for _, id := range []string{"first", "second"} {
			host, directHost, direct := build(id)
			if err := manager.Activate(ctx, host, id, "digest"); err != nil {
				t.Fatal(err)
			}
			// Activation preserves the desired scheduler state across revisions.
			if err := remote.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := directHost.Traffic(ctx, &pb.TrafficRequest{Operation: pb.TrafficRequest_OPERATION_START}); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			w.Advance(250 * time.Millisecond)
			synctest.Wait()
			got, err := remote.Windows(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if want := direct.Windows(); !reflect.DeepEqual(got, want) {
				t.Fatalf("traffic parity %s:\ngot %#v\nwant %#v", id, got, want)
			}
			samples := got["health"].Scenarios[0]
			if samples.Scenario.ID != id || len(samples.Samples) == 0 {
				t.Fatalf("wrong revision: %+v", samples)
			}
			broken.Store(true)
			w.Advance(200 * time.Millisecond)
			synctest.Wait()
			got, err = remote.Windows(ctx)
			if err != nil || !windowHasRecentFailure(got["health"]) {
				t.Fatalf("failure disappeared: %+v %v", got, err)
			}
			if err := remote.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			directHost.Close()
			before, _ := remote.Windows(ctx)
			w.Advance(time.Second)
			after, _ := remote.Windows(ctx)
			if !reflect.DeepEqual(before["health"].Scenarios, after["health"].Scenarios) {
				t.Fatal("scheduler sampled after stop")
			}
			if err := remote.Reset(ctx); err != nil {
				t.Fatal(err)
			}
			empty, _ := remote.Windows(ctx)
			if len(empty["health"].Scenarios[0].Samples) != 0 {
				t.Fatal("reset retained samples")
			}
			broken.Store(false)
			// Keep desired state running when the next accepted revision activates.
			if err := remote.Start(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := remote.Burst(ctx, prober.BurstRequest{Workload: "missing"}); err == nil {
			t.Fatal("missing burst authorized")
		}
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := remote.Windows(ctx); err == nil {
			t.Fatal("closed host returned windows")
		}
	})
}

func TestMemoryTrafficObserverReconfiguresWorkloads(t *testing.T) {
	observer := NewTrafficObserver(HTTPProberClient{}, []string{"old"}, nil, 0)
	observer.windows = map[string]traffic.Window{"old": {}}
	observer.Reconfigure([]string{"new"})
	if windows := observer.Windows(); len(windows) != 0 {
		t.Fatalf("retired windows survived: %+v", windows)
	}
	observer.windows = map[string]traffic.Window{}
	if observer.Windows()["new"].LoadError == "" {
		t.Fatal("new missing workload did not fail closed")
	}
}
