package sdktest

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// PrimitiveCall stores one live ReadView call for deterministic replay.
type PrimitiveCall struct {
	// Phase separates observations made before and after repair. Empty means open.
	Phase     string          `json:"phase,omitempty"`
	Method    string          `json:"method"`
	Arguments json.RawMessage `json:"arguments"`
	Result    json.RawMessage `json:"result"`
	Error     string          `json:"error,omitempty"`
}

// RecordingReadView serializes recorded calls and OnRecord callbacks. ReadView
// must support concurrent reads; inspect Calls only after all readers finish.
type RecordingReadView struct {
	program.ReadView
	Calls    []PrimitiveCall
	OnRecord func(PrimitiveCall)
	mu       sync.Mutex
}

func (v *RecordingReadView) record(method string, arguments any, result any, err error) {
	args, _ := json.Marshal(arguments)
	value, _ := json.Marshal(result)
	call := PrimitiveCall{Method: method, Arguments: args, Result: value}
	if err != nil {
		call.Error = err.Error()
	}
	v.mu.Lock()
	v.Calls = append(v.Calls, call)
	if v.OnRecord != nil {
		v.OnRecord(call)
	}
	v.mu.Unlock()
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

// ReplayReadView consumes each recorded call at most once, including when
// diagnoses share the view. Snapshot and Calls must remain immutable during use.
type ReplayReadView struct {
	Snapshot
	Calls []PrimitiveCall
	used  map[int]bool
	mu    sync.Mutex
}

func (v *ReplayReadView) replay(method string, arguments any, result any) error {
	args, err := json.Marshal(arguments)
	if err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
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
	Open                   Snapshot
	Fixed                  Snapshot
	Healthy                Snapshot
	Recordings             []PrimitiveCall
	RootCauseProgramIDs    []string
	NoMatchProgramIDs      []string
	Bindings               program.Bindings
	PeerBindings           program.Bindings
	Provenance             CorpusProvenance
	SyntheticNoMatchReason string
}

type CorpusProvenance struct {
	IncidentID  string `json:"incident_id"`
	OpenAt      string `json:"open_at,omitempty"`
	OpenSHA256  string `json:"open_sha256"`
	FixedSHA256 string `json:"fixed_sha256"`
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
			RootCauseProgramIDs    *[]string         `json:"root_cause_program_ids"`
			NoMatchProgramIDs      *[]string         `json:"no_match_program_ids"`
			Bindings               *program.Bindings `json:"bindings"`
			PeerBindings           *program.Bindings `json:"peer_bindings"`
			Provenance             *CorpusProvenance `json:"provenance"`
			SyntheticNoMatchReason *string           `json:"synthetic_no_match_reason"`
		}{&c.RootCauseProgramIDs, &c.NoMatchProgramIDs, &c.Bindings, &c.PeerBindings, &c.Provenance, &c.SyntheticNoMatchReason}},
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

// ValidateCorpusProvenance checks that persisted case snapshots still match
// their recorded controller capture, or an explicitly labeled NoMatch case.
func ValidateCorpusProvenance(dir string) error {
	c, err := LoadCorpusCase(dir)
	if err != nil {
		return err
	}
	if c.Provenance.IncidentID == "" {
		if c.SyntheticNoMatchReason != "" && len(c.RootCauseProgramIDs) == 0 && len(c.NoMatchProgramIDs) > 0 {
			return nil
		}
		return errors.New("corpus case lacks captured incident provenance or synthetic NoMatch reason")
	}
	if c.SyntheticNoMatchReason != "" {
		return errors.New("captured corpus cannot also be synthetic")
	}
	for name, digest := range map[string]string{"open.json": c.Provenance.OpenSHA256, "fixed.json": c.Provenance.FixedSHA256, "healthy.json": c.Provenance.FixedSHA256} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if digest == "" || fmt.Sprintf("%x", sha256.Sum256(data)) != digest {
			return fmt.Errorf("corpus %s does not match captured snapshot digest", name)
		}
	}
	if c.Open.NamespaceName == "" || c.Open.NamespaceName != c.Fixed.NamespaceName || c.Fixed.NamespaceName != c.Healthy.NamespaceName {
		return errors.New("captured corpus namespace mismatch")
	}
	return nil
}

// PhaseRecordings selects actual observations for one corpus phase. Healthy
// snapshots are the verified fixed snapshot, so they share its observations.
func (c CorpusCase) PhaseRecordings(phase string) []PrimitiveCall {
	var result []PrimitiveCall
	explicitHealthy := false
	for _, call := range c.Recordings {
		if call.Phase == "healthy" {
			explicitHealthy = true
		}
	}
	for _, call := range c.Recordings {
		recorded := call.Phase
		if recorded == "" {
			recorded = "open"
		}
		if recorded == phase || phase == "healthy" && !explicitHealthy && recorded == "fixed" {
			result = append(result, call)
		}
	}
	return result
}

// RecordPrimitiveCalls repeats previously observed reads on a fresh view. It
// records errors as observations; malformed templates fail explicitly.
func RecordPrimitiveCalls(view program.ReadView, calls []PrimitiveCall) ([]PrimitiveCall, error) {
	recorder := &RecordingReadView{ReadView: view}
	for _, call := range calls {
		var args []json.RawMessage
		if err := json.Unmarshal(call.Arguments, &args); err != nil || len(args) == 0 {
			return nil, fmt.Errorf("invalid %s recording arguments", call.Method)
		}
		var ref sdk.ObjectRef
		if err := json.Unmarshal(args[0], &ref); err != nil {
			return nil, err
		}
		switch call.Method {
		case "DiffAgainstSource":
			if len(args) != 1 {
				return nil, errors.New("invalid DiffAgainstSource arguments")
			}
			_, _ = recorder.DiffAgainstSource(ref)
		case "DiffAgainstBaseline":
			if len(args) != 1 {
				return nil, errors.New("invalid DiffAgainstBaseline arguments")
			}
			_, _ = recorder.DiffAgainstBaseline(ref)
		case "ProbeTCP":
			if len(args) != 2 {
				return nil, errors.New("invalid ProbeTCP arguments")
			}
			var port int32
			if err := json.Unmarshal(args[1], &port); err != nil {
				return nil, err
			}
			_, _ = recorder.ProbeTCP(ref, port)
		case "ProbeHTTP":
			if len(args) != 2 {
				return nil, errors.New("invalid ProbeHTTP arguments")
			}
			var request program.HTTPProbe
			if err := json.Unmarshal(args[1], &request); err != nil {
				return nil, err
			}
			_, _ = recorder.ProbeHTTP(ref, request)
		case "Logs":
			if len(args) != 3 {
				return nil, errors.New("invalid Logs arguments")
			}
			var container string
			var since time.Duration
			if err := json.Unmarshal(args[1], &container); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(args[2], &since); err != nil {
				return nil, err
			}
			_, _ = recorder.Logs(ref, container, since)
		default:
			return nil, fmt.Errorf("unknown recorded primitive %q", call.Method)
		}
	}
	return recorder.Calls, nil
}
