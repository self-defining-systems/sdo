package runtime

import (
	"context"
	"fmt"
	"time"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storeops"
)

// structuredReadTimeout bounds one store read inside the shared diagnosis
// deadline.
const structuredReadTimeout = 3 * time.Second

// Read serves a structured query: a store read runs the engine's fixed
// read-only command in the grounded store container; a probe query goes
// through the prober, which returns only the named fields of the response.
func (v LiveReadView) Read(q program.Query) (program.Observation, error) {
	if err := q.Validate(); err != nil {
		return program.Observation{}, err
	}
	if q.Probe != nil {
		return program.ReadProbe(v, *q.Probe)
	}
	t := q.Store.Target
	if t.Pod.Namespace != v.Namespace() {
		return program.Observation{}, fmt.Errorf("store read outside snapshot namespace")
	}
	if err := (TypedPlanClient{ProtectedNamespace: v.ProtectedNamespace}).checkPlanNamespace(t.Pod.Namespace); err != nil {
		return program.Observation{}, err
	}
	if err := program.GroundStoreTarget(v, t); err != nil {
		return program.Observation{}, err
	}
	captured, err := v.Object(t.Pod)
	if err != nil || captured == nil || captured.GetUID() == "" {
		return program.Observation{}, fmt.Errorf("store Pod absent from snapshot")
	}
	engine := t.EngineOps()
	port := t.Port
	if port == 0 {
		port = engine.DefaultPort()
	}
	command, err := engine.Read(port, q.Store.Scope, q.Store.Fields)
	if err != nil {
		return program.Observation{}, err
	}
	ctx, cancel := context.WithTimeout(v.context(), structuredReadTimeout)
	defer cancel()
	out := &cappedWriter{limit: maxStoreOutputBytes}
	client := TypedPlanClient{Client: v.Client, ExecConfig: v.DatabaseExecConfig, ProtectedNamespace: v.ProtectedNamespace}
	code, err := client.StoreCommand(ctx, string(captured.GetUID()), program.Command{Pod: t.Pod, Container: t.Container, Args: command.Args, Stdin: command.Stdin}, out)
	if out.over {
		return program.Observation{}, fmt.Errorf("store read output exceeded %d bytes", maxStoreOutputBytes)
	}
	if err != nil {
		return program.Observation{}, fmt.Errorf("store read failed: %w", err)
	}
	if code != 0 {
		return program.Observation{}, fmt.Errorf("store read gave no answer (exit %d)", code)
	}
	count, rows, err := storeops.ParseRead(out.buffer.String(), q.Store.Fields)
	if err != nil {
		return program.Observation{}, err
	}
	observation := program.Observation{Count: count}
	for _, row := range rows {
		observation.Rows = append(observation.Rows, program.Row(row))
	}
	return observation.Normalize()
}

func (v *cachedReadView) Read(q program.Query) (program.Observation, error) {
	var o program.Observation
	err := v.read("Read", []any{q}, &o, func() (any, error) { return v.ReadView.Read(q) })
	return o, err
}
