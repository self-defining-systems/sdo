package sdktest

import (
	"encoding/json"
	"sync/atomic"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// ReplayProgramReadView overlays bounded host replies ahead of any snapshot
// fallback, including for SDK methods that cannot return an error.
func (v *ReplayReadView) ReplayProgramReadView(source program.ReadView) program.ReadView {
	var limits map[string][]json.RawMessage
	for _, calls := range [][]PrimitiveCall{v.Snapshot.Readings, v.Calls} {
		for _, call := range calls {
			if call.FrameLimited() {
				if limits == nil {
					limits = make(map[string][]json.RawMessage)
				}
				limits[call.Method] = append(limits[call.Method], call.Arguments)
			}
		}
	}
	if limits == nil {
		return source
	}
	return &replayFrameView{ReadView: source, limits: limits}
}

type replayFrameView struct {
	program.ReadView
	limits  map[string][]json.RawMessage
	aborted atomic.Bool
}

func (v *replayFrameView) abort() {
	v.aborted.Store(true)
	panic(sdk.ErrReadReplyLimit)
}

func (v *replayFrameView) ReadAbortError() error {
	if v.aborted.Load() {
		return sdk.ErrReadReplyLimit
	}
	return nil
}

func (v *replayFrameView) limited(method string, arguments []any) bool {
	if len(v.limits[method]) == 0 {
		return false
	}
	args, err := json.Marshal(arguments)
	if err != nil {
		return false
	}
	for _, limited := range v.limits[method] {
		if sameJSON(limited, args) {
			return true
		}
	}
	return false
}
