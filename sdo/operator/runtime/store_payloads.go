package runtime

import (
	"context"
	"io"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storestate"
)

// storePayloadTimeout bounds one store Pod's restore-text read.
const storePayloadTimeout = 20 * time.Second

// StorePayloads holds the restore text of each store Pod, captured when the
// pod-read baseline promotes that Pod's healthy store-state reading: the full
// validator and index text a RestoreStoreItem puts back, which the value-free
// read keeps only as a digest. Like the baseline it is in-memory controller
// state with the same lifetime, never written to operational memory or
// evidence; it never holds a credential. Each entry carries the fact value it
// describes, so a restore uses it only while it matches the baseline.
type StorePayloads struct {
	// Run executes one command in a Pod identified by uid.
	Run func(ctx context.Context, uid string, command program.Command, stdout io.Writer) (int32, error)

	mu      sync.RWMutex
	entries map[string]map[string]storestate.Payload // component -> pod -> payload
	pending sync.WaitGroup
}

// Payload returns the restore text of a store Pod, or the only one of a
// single-instance component, matching the baseline's own Pod choice.
func (s *StorePayloads) Payload(component, pod string) (storestate.Payload, bool) {
	if s == nil {
		return storestate.Payload{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	pods := s.entries[component]
	if p, ok := pods[pod]; ok {
		return p, true
	}
	if len(pods) == 1 {
		for _, p := range pods {
			return p, true
		}
	}
	return storestate.Payload{}, false
}

// promoted starts capturing the restore text of every promoted store-state
// reading whose engine needs it, without holding up the caller.
func (s *StorePayloads) promoted(results map[string]podread.Result) {
	if s == nil || s.Run == nil {
		return
	}
	type job struct {
		component string
		reader    storestate.PayloadReader
		port      int
		reading   podread.Reading
	}
	var jobs []job
	for id, result := range results {
		engine, component, ok := storestate.ParseQueryID(id)
		if !ok {
			continue
		}
		reader, ok := storestate.PayloadReaderFor(engine)
		if !ok {
			continue
		}
		adapter, _ := storestate.AdapterFor(engine)
		for _, reading := range result.Readings {
			jobs = append(jobs, job{component: component, reader: reader, port: adapter.DefaultPort(), reading: reading})
		}
	}
	if len(jobs) == 0 {
		return
	}
	s.pending.Add(1)
	go func() {
		defer s.pending.Done()
		for _, j := range jobs {
			s.capture(j.component, j.reader, j.port, j.reading)
		}
	}()
}

// capture reads one Pod's restore text and keeps it; a failed read keeps the
// earlier text, whose fact values still guard its use.
func (s *StorePayloads) capture(component string, reader storestate.PayloadReader, port int, reading podread.Reading) {
	ctx, cancel := context.WithTimeout(context.Background(), storePayloadTimeout)
	defer cancel()
	out := &cappedWriter{limit: storestate.MaxPayloadBytes + 64<<10}
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: reading.Namespace, Name: reading.Pod}
	code, err := s.Run(ctx, reading.UID, program.Command{Pod: pod, Container: reading.Container, Args: reader.PayloadCommand(storestate.Endpoint{Port: port})}, out)
	if err != nil || code != 0 || out.over {
		return
	}
	payload, err := reader.ParsePayload(out.buffer.String())
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = map[string]map[string]storestate.Payload{}
	}
	if s.entries[component] == nil {
		s.entries[component] = map[string]storestate.Payload{}
	}
	s.entries[component][reading.Pod] = payload
}

// wait blocks until the started captures finished (tests).
func (s *StorePayloads) wait() { s.pending.Wait() }
