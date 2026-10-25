package sdktest

import (
	"fmt"
	"sdo.dev/operator/sdk/program"
)

func (v *RecordingReadView) DatabaseUsers(t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	r, err := v.ReadView.DatabaseUsers(t)
	if err != nil {
		err = fmt.Errorf("database catalog observation unavailable")
		r = nil
	} else {
		r, err = program.NormalizeDatabaseUsers(r)
	}
	v.record("DatabaseUsers", []any{t}, r, err)
	return r, err
}
func (v *ReplayReadView) DatabaseUsers(t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	var r []program.DatabaseUser
	if err := v.replay("DatabaseUsers", []any{t}, &r); err != nil {
		return nil, fmt.Errorf("database catalog observation unavailable")
	}
	return program.NormalizeDatabaseUsers(r)
}

func (s Snapshot) DatabaseUsers(t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).DatabaseUsers(t)
}
