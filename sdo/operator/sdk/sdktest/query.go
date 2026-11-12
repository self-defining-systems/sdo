package sdktest

import (
	"errors"
	"fmt"

	"sdo.dev/operator/sdk/program"
)

// Read records the query and its bounded typed answer. A recording holds only
// the fields the query named: a store row is the projection, a probe row the
// extracted fields, and no response body or other column is kept.
func (v *RecordingReadView) Read(q program.Query) (program.Observation, error) {
	if err := q.Validate(); err != nil {
		return program.Observation{}, err
	}
	o, err := v.ReadView.Read(q)
	if err != nil {
		err = fmt.Errorf("structured read unavailable")
		o = program.Observation{}
	} else if o, err = o.Normalize(); err != nil {
		o = program.Observation{}
	}
	v.record("Read", []any{q}, o, err)
	return o, err
}

// Read serves a recorded answer; a query with no recorded answer is an
// UnrecordedError ("no recorded case").
func (v *ReplayReadView) Read(q program.Query) (program.Observation, error) {
	if err := q.Validate(); err != nil {
		return program.Observation{}, err
	}
	var o program.Observation
	if err := v.Recorded("Read", []any{q}, &o); err != nil {
		var unrecorded *UnrecordedError
		if errors.As(err, &unrecorded) {
			return program.Observation{}, err
		}
		return program.Observation{}, fmt.Errorf("structured read unavailable")
	}
	return o.Normalize()
}

func (s Snapshot) Read(q program.Query) (program.Observation, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).Read(q)
}
