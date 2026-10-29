// Synthetic restart/capture-failure schedule produced by the real controller.
package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	runtime "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type detector struct{}

func (detector) Spec() sdk.DetectorSpec                                              { return sdk.DetectorSpec{ID: "health", Interval: time.Hour} }
func (detector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) { return nil, nil }

type provider struct{}

func (provider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return sdktest.Snapshot{NamespaceName: "synthetic"}, nil
}

type dispatcher struct{}

func (dispatcher) Dispatch(context.Context, runtime.IncidentRequest) (runtime.IncidentResult, error) {
	panic("clear cannot dispatch")
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	root, err := os.MkdirTemp("", "replay-readiness-")
	must(err)
	defer os.RemoveAll(root)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	makeController := func(at time.Time) *runtime.Controller {
		c, err := runtime.NewController(runtime.ControllerConfig{Application: "synthetic", Namespace: "synthetic", ResponseTimeout: time.Minute, FiringThreshold: 1, ClearThreshold: 1, CorpusSessionRoot: root}, []sdk.Detector{detector{}}, provider{}, dispatcher{}, at)
		must(err)
		return c
	}
	rows := map[string]json.RawMessage{}
	save := func(key string, c *runtime.Controller) *contracts.ControllerState {
		state, err := c.ExportState()
		must(err)
		raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(state)
		must(err)
		rows[key] = raw
		return state
	}
	c := makeController(start)
	save("startup", c)
	obstruction := filepath.Join(root, ".sdo-runtime")
	must(os.WriteFile(obstruction, []byte("synthetic obstruction"), 0600))
	must(c.EvaluateAll(context.Background(), start))
	save("capture_failure", c)
	must(os.Remove(obstruction))
	must(c.EvaluateAll(context.Background(), start.Add(time.Second)))
	state := save("healthy", c)
	c = makeController(start.Add(2 * time.Second))
	must(c.RestoreState(state))
	save("restart", c)
	must(c.Step(context.Background(), start.Add(2*time.Second), nil))
	save("requalified", c)
	must(json.NewEncoder(os.Stdout).Encode(rows))
}
