package sdktest

import (
	"errors"
	"fmt"

	"sdo.dev/operator/sdk/program"
)

// AppMap records the accepted application map the wrapped view read.
func (v *RecordingReadView) AppMap() (program.AppMap, error) {
	m, err := v.ReadView.AppMap()
	v.record("AppMap", []any{}, m, err)
	return m, err
}

// DataChecks records the accepted data checks the wrapped view read.
func (v *RecordingReadView) DataChecks() (map[string]string, error) {
	checks, err := v.ReadView.DataChecks()
	v.record("DataChecks", []any{}, checks, err)
	return checks, err
}

// AppMap replays the snapshot's ApplicationMap, else the recorded read.
func (v *ReplayReadView) AppMap() (program.AppMap, error) {
	if v.ApplicationMap != nil {
		return *v.ApplicationMap, nil
	}
	var m program.AppMap
	if err := v.Recorded("AppMap", []any{}, &m); err != nil {
		var unrecorded *UnrecordedError
		if errors.As(err, &unrecorded) {
			return program.AppMap{}, fmt.Errorf("application map unavailable: %w", err)
		}
		return program.AppMap{}, err
	}
	return m, nil
}

// DataChecks replays the snapshot's AcceptedDataChecks, else the recorded read.
func (v *ReplayReadView) DataChecks() (map[string]string, error) {
	if v.AcceptedDataChecks != nil {
		return v.AcceptedDataChecks, nil
	}
	var checks map[string]string
	if err := v.Recorded("DataChecks", []any{}, &checks); err != nil {
		var unrecorded *UnrecordedError
		if errors.As(err, &unrecorded) {
			return nil, fmt.Errorf("data checks unavailable: %w", err)
		}
		return nil, err
	}
	return checks, nil
}

func (s Snapshot) AppMap() (program.AppMap, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).AppMap()
}

func (s Snapshot) DataChecks() (map[string]string, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).DataChecks()
}
