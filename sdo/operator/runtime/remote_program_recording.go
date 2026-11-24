package runtime

import (
	"encoding/json"

	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Stage records for one call through the existing view stack. Only the transport
// can decide which result the program saw. Keep the cycle cache and all read
// restrictions in place while deferring writes to recording sinks.
func readForRemoteProgram(view program.ReadView, call programread.Message) programread.Message {
	var capture programReadCapture
	answer := programread.Read(capture.stage(view), call)
	if len(capture.sinks) > 0 {
		answer.OnReply = func(bounded programread.Message) {
			if programread.IsFrameLimited(bounded) {
				capture.recordLimit(call, bounded)
				return
			}
			for _, publish := range capture.pending {
				publish()
			}
		}
	}
	return answer
}

type programReadCapture struct {
	sinks   []*sdktest.RecordingReadView
	pending []func()
}

func (c *programReadCapture) stage(view program.ReadView) program.ReadView {
	switch v := view.(type) {
	case *CycleReadView:
		return c.stage(v.RecordingReadView)
	case *sdktest.RecordingReadView:
		c.sinks = append(c.sinks, v)
		return v.StageRecords(c.stage(v.ReadView), func(publish func()) {
			c.pending = append(c.pending, publish)
		})
	case interface {
		TransformReadSource(func(program.ReadView) program.ReadView) program.ReadView
	}:
		return v.TransformReadSource(c.stage)
	case *cachedReadView:
		return &cachedReadView{ReadView: c.stage(v.ReadView), readCache: v.readCache}
	case mitigationReadView:
		v.ReadView = c.stage(v.ReadView)
		return v
	case boundedProbeView:
		v.ReadView = c.stage(v.ReadView)
		return v
	case adoptionConflictView:
		v.ReadView = c.stage(v.ReadView)
		return v
	case onboardingReadView:
		v.ReadView = c.stage(v.ReadView)
		return v
	default:
		return view
	}
}

func (c *programReadCapture) recordLimit(call, reply programread.Message) {
	// Project the bounded SDK result with the same recorder as an ordinary
	// observation. It reads only the reply already sent, never the source again.
	recorder := &sdktest.RecordingReadView{ReadView: &programread.View{
		Exchange: func(programread.Message) (programread.Message, error) { return reply, nil },
	}}
	programread.Read(recorder, call)
	records := recorder.SnapshotCalls()
	if len(records) == 0 {
		// Some methods read from a corpus snapshot by default. Retain the
		// unavailable reply so replay can't replace it with that snapshot value.
		args, _ := json.Marshal(append([]json.RawMessage{}, call.Arguments...))
		records = []sdktest.PrimitiveCall{{
			Method: call.Method, Arguments: args, Result: json.RawMessage("null"),
			Error: reply.Error, ErrorClass: reply.ErrorClass,
		}}
	}
	for i := range records {
		records[i].FrameLimited = true
		if call.Method != "QueryMetrics" {
			records[i].Error = reply.Error
			records[i].ErrorClass = reply.ErrorClass
		}
	}
	for _, sink := range c.sinks {
		sink.RecordExternalCalls(records)
	}
}
