package memoryhost

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sync"
	"time"
)

// checkedView prevents programs swallowing an unavailable primitive and
// returning a plan anyway. This is the same fail-closed rule as diagnosis: only a
// read SDO could not observe (unavailable) or one the program may not make
// (invalid) fails Mitigate, because a plan built over them has no later check on
// its inputs. A refused or absent read is an observation, so a fallback over a
// refused credential builds its plan.
type checkedView struct {
	program.ReadView
	mu  sync.Mutex
	err error
}

func (v *checkedView) check(err error) {
	if err == nil {
		return
	}
	if class := sdk.ReadErrorClass(err); class != sdk.ReadClassUnavailable && class != sdk.ReadClassInvalid {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.err == nil {
		v.err = err
	}
}
func (v *checkedView) failure() error { v.mu.Lock(); defer v.mu.Unlock(); return v.err }
func (v *checkedView) Logs(r sdk.ObjectRef, c string, s time.Duration) (string, error) {
	got, err := v.ReadView.Logs(r, c, s)
	v.check(err)
	return got, err
}
func (v *checkedView) ProbeTCP(r sdk.ObjectRef, p int32) (program.ProbeResult, error) {
	got, err := v.ReadView.ProbeTCP(r, p)
	v.check(err)
	return got, err
}
func (v *checkedView) ProbeHTTP(r sdk.ObjectRef, p program.HTTPProbe) (program.ProbeResult, error) {
	got, err := v.ReadView.ProbeHTTP(r, p)
	v.check(err)
	return got, err
}
func (v *checkedView) DiffAgainstSource(r sdk.ObjectRef) (program.Diff, error) {
	got, err := v.ReadView.DiffAgainstSource(r)
	v.check(err)
	return got, err
}
func (v *checkedView) DiffAgainstBaseline(r sdk.ObjectRef) (program.Diff, error) {
	got, err := v.ReadView.DiffAgainstBaseline(r)
	v.check(err)
	return got, err
}

func (v *checkedView) SecretEqual(left, right program.SecretKey) (bool, error) {
	equal, err := v.ReadView.SecretEqual(left, right)
	v.check(err)
	return equal, err
}
