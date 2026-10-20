package sdktest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// PrimitiveCall stores one live ReadView call for deterministic replay.
type PrimitiveCall struct {
	Method    string          `json:"method"`
	Arguments json.RawMessage `json:"arguments"`
	Result    json.RawMessage `json:"result"`
	Error     string          `json:"error,omitempty"`
}

type RecordingReadView struct {
	program.ReadView
	Calls []PrimitiveCall
}

func (v *RecordingReadView) record(method string, arguments any, result any, err error) {
	args, _ := json.Marshal(arguments)
	value, _ := json.Marshal(result)
	call := PrimitiveCall{Method: method, Arguments: args, Result: value}
	if err != nil {
		call.Error = err.Error()
	}
	v.Calls = append(v.Calls, call)
}

func (v *RecordingReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	result, err := v.ReadView.Logs(ref, container, since)
	v.record("Logs", []any{ref, container, since}, result, err)
	return result, err
}
func (v *RecordingReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	result, err := v.ReadView.ProbeTCP(ref, port)
	v.record("ProbeTCP", []any{ref, port}, result, err)
	return result, err
}
func (v *RecordingReadView) ProbeHTTP(ref sdk.ObjectRef, req program.HTTPProbe) (program.ProbeResult, error) {
	result, err := v.ReadView.ProbeHTTP(ref, req)
	v.record("ProbeHTTP", []any{ref, req}, result, err)
	return result, err
}
func (v *RecordingReadView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	result, err := v.ReadView.DiffAgainstSource(ref)
	v.record("DiffAgainstSource", []any{ref}, result, err)
	return result, err
}
func (v *RecordingReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (program.Diff, error) {
	result, err := v.ReadView.DiffAgainstBaseline(ref)
	v.record("DiffAgainstBaseline", []any{ref}, result, err)
	return result, err
}

type ReplayReadView struct {
	Snapshot
	Calls []PrimitiveCall
	used  map[int]bool
}

func (v *ReplayReadView) replay(method string, arguments any, result any) error {
	args, err := json.Marshal(arguments)
	if err != nil {
		return err
	}
	for index, call := range v.Calls {
		if v.used[index] || call.Method != method || !sameJSON(call.Arguments, args) {
			continue
		}
		if v.used == nil {
			v.used = make(map[int]bool)
		}
		v.used[index] = true
		if call.Error != "" {
			return errors.New(call.Error)
		}
		return json.Unmarshal(call.Result, result)
	}
	return fmt.Errorf("unrecorded %s call with arguments %s", method, args)
}

func sameJSON(left, right []byte) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

func (v *ReplayReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	var result string
	err := v.replay("Logs", []any{ref, container, since}, &result)
	return result, err
}
func (v *ReplayReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	var result program.ProbeResult
	err := v.replay("ProbeTCP", []any{ref, port}, &result)
	return result, err
}
func (v *ReplayReadView) ProbeHTTP(ref sdk.ObjectRef, req program.HTTPProbe) (program.ProbeResult, error) {
	var result program.ProbeResult
	err := v.replay("ProbeHTTP", []any{ref, req}, &result)
	return result, err
}
func (v *ReplayReadView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	var result program.Diff
	err := v.replay("DiffAgainstSource", []any{ref}, &result)
	return result, err
}
func (v *ReplayReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (program.Diff, error) {
	var result program.Diff
	err := v.replay("DiffAgainstBaseline", []any{ref}, &result)
	return result, err
}

type CorpusCase struct {
	Open                Snapshot
	Fixed               Snapshot
	Healthy             Snapshot
	Recordings          []PrimitiveCall
	RootCauseProgramIDs []string
	Bindings            program.Bindings
	PeerBindings        program.Bindings
}

// LoadCorpusCase reads open.json, fixed.json, healthy.json, recordings.json,
// and labels.json from one .sdo/corpus/<case> directory.
func LoadCorpusCase(dir string) (CorpusCase, error) {
	var c CorpusCase
	for _, item := range []struct {
		name   string
		target any
	}{
		{"open.json", &c.Open}, {"fixed.json", &c.Fixed}, {"healthy.json", &c.Healthy}, {"recordings.json", &c.Recordings},
		{"labels.json", &struct {
			RootCauseProgramIDs *[]string         `json:"root_cause_program_ids"`
			Bindings            *program.Bindings `json:"bindings"`
			PeerBindings        *program.Bindings `json:"peer_bindings"`
		}{&c.RootCauseProgramIDs, &c.Bindings, &c.PeerBindings}},
	} {
		path := filepath.Join(dir, item.name)
		raw, err := os.ReadFile(path)
		if err != nil {
			return c, fmt.Errorf("corpus %s: %w", path, err)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(item.target); err != nil {
			return c, fmt.Errorf("corpus %s: %w", path, err)
		}
	}
	c.Open.SourceName, c.Fixed.SourceName, c.Healthy.SourceName = "open.json", "fixed.json", "healthy.json"
	return c, nil
}
