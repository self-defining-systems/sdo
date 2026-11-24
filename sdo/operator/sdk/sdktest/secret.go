package sdktest

import "sdo.dev/operator/sdk/program"

func (v *RecordingReadView) SecretEqual(left, right program.SecretKey) (bool, error) {
	equal, err := v.ReadView.SecretEqual(left, right)
	if err != nil {
		err = safeReadError("Secret comparison unavailable", err)
	}
	v.record("SecretEqual", []any{left, right}, equal, err)
	return equal, err
}

func (v *ReplayReadView) SecretEqual(left, right program.SecretKey) (bool, error) {
	var equal bool
	err := v.Recorded("SecretEqual", []any{left, right}, &equal)
	return equal, err
}

func (s Snapshot) SecretEqual(left, right program.SecretKey) (bool, error) {
	return (&ReplayReadView{Calls: s.Readings}).SecretEqual(left, right)
}
