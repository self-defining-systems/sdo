package program

import "sdo.dev/operator/sdk"

// SecretKey identifies a v1 Secret key in the application namespace.
// Accepted selects the incident's accepted rendered source instead of the live object.
type SecretKey struct {
	Ref      sdk.ObjectRef `json:"ref"`
	Key      string        `json:"key"`
	Accepted bool          `json:"accepted,omitempty"`
}

func (v snapshotReadView) SecretEqual(left, right SecretKey) (bool, error) {
	if source, ok := v.DetectionContext.(interface {
		SecretEqual(SecretKey, SecretKey) (bool, error)
	}); ok {
		return source.SecretEqual(left, right)
	}
	return false, sdk.ErrUnavailable
}

func (v *checkedReadView) SecretEqual(left, right SecretKey) (bool, error) {
	equal, err := v.ReadView.SecretEqual(left, right)
	v.check("SecretEqual", err)
	return equal, err
}
