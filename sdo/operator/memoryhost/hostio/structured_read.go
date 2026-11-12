package hostio

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storeops"
)

const (
	structuredReadTimeout = 3 * time.Second
	maxStructuredOutput   = 64 << 10
)

type boundedBuffer struct {
	bytes.Buffer
	over bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > maxStructuredOutput {
		b.over = true
		return 0, fmt.Errorf("output exceeds %d bytes", maxStructuredOutput)
	}
	return b.Buffer.Write(p)
}

// Read serves a structured query live: a probe query through ProbeHTTP, a store
// query by the engine's fixed read-only command in the grounded store container.
func (v *LiveReadView) Read(q program.Query) (program.Observation, error) {
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
	if err := v.refuseControl(t.Pod.Namespace); err != nil {
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
	ctx, cancel := context.WithTimeout(v.ctx, structuredReadTimeout)
	defer cancel()
	out := &boundedBuffer{}
	code, err := kubereads.ReadOutput(ctx, v.getPod, v.backends.exec(), v.backends.ProtectedNamespace, string(captured.GetUID()), program.Command{Pod: t.Pod, Container: t.Container, Args: command.Args, Stdin: command.Stdin}, out)
	if out.over {
		return program.Observation{}, fmt.Errorf("store read output exceeded %d bytes", maxStructuredOutput)
	}
	if err != nil {
		return program.Observation{}, fmt.Errorf("store read failed: %w", err)
	}
	if code != 0 {
		return program.Observation{}, fmt.Errorf("store read gave no answer (exit %d)", code)
	}
	count, rows, err := storeops.ParseRead(out.String(), q.Store.Fields)
	if err != nil {
		return program.Observation{}, err
	}
	observation := program.Observation{Count: count}
	for _, row := range rows {
		observation.Rows = append(observation.Rows, program.Row(row))
	}
	return observation.Normalize()
}
