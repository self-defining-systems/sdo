package sdktest

import (
	"encoding/json"
	"sdo.dev/operator/sdk/program"
)

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

// A captured Secret comparison can verify diagnosis before and after a
// restore without simulating credential bytes or unrelated plan effects.
func observesSecretRestore(plan program.Plan, c CorpusCase) bool {
	for _, call := range c.PhaseRecordings("open") {
		if call.Method != "SecretEqual" || call.Error != "" || string(call.Result) != "false" {
			continue
		}
		var keys []program.SecretKey
		if json.Unmarshal(call.Arguments, &keys) != nil || len(keys) != 2 ||
			keys[0].Accepted || !keys[1].Accepted || keys[0].Ref != keys[1].Ref || keys[0].Key != keys[1].Key {
			continue
		}
		for _, action := range plan.Actions {
			switch restore := action.(type) {
			case program.ReconcileFromSource:
				if restore.Ref == keys[0].Ref {
					return true
				}
			case *program.ReconcileFromSource:
				if restore != nil && restore.Ref == keys[0].Ref {
					return true
				}
			}
		}
	}
	return false
}
