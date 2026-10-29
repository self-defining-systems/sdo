// Synthetic controller history produced by the real scheduler and state exporter.
package main

import (
	"context"
	"os"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	runtime "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type detector struct {
	id       string
	interval time.Duration
}

func (d detector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: d.id, Interval: d.interval} }
func (d detector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

type provider struct{}

func (provider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return sdktest.Snapshot{NamespaceName: "synthetic"}, nil
}

type dispatcher struct{}

func (dispatcher) Dispatch(context.Context, runtime.IncidentRequest) (runtime.IncidentResult, error) {
	panic("clear detectors cannot dispatch")
}
func main() {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c, err := runtime.NewController(runtime.ControllerConfig{Application: "synthetic", Namespace: "synthetic", ResponseTimeout: time.Minute, FiringThreshold: 1, ClearThreshold: 1}, []sdk.Detector{detector{"health", time.Second}, detector{"event-policy", time.Hour}}, provider{}, dispatcher{}, start)
	if err != nil {
		panic(err)
	}
	for _, now := range []time.Time{start, start.Add(10 * time.Second)} {
		if err = c.Step(context.Background(), now, nil); err != nil {
			panic(err)
		}
	}
	state, err := c.ExportState()
	if err != nil {
		panic(err)
	}
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(state)
	if err != nil {
		panic(err)
	}
	if _, err = os.Stdout.Write(raw); err != nil {
		panic(err)
	}
}
