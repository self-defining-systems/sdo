package runtime

import (
	"bytes"
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (v LiveReadView) SecretEqual(left, right program.SecretKey) (bool, error) {
	if left.Accepted {
		return false, sdk.Classified("left Secret key must be live", sdk.ErrInvalidRead)
	}
	for _, key := range []program.SecretKey{left, right} {
		ref := key.Ref
		if ref.Kind != "Secret" || ref.APIVersion != "v1" || ref.Namespace == "" ||
			ref.Namespace != v.Namespace() || ref.Namespace == v.ProtectedNamespace || ref.Name == "" || key.Key == "" {
			return false, sdk.Classified("Secret comparison requires v1 Secret keys in the incident namespace", sdk.ErrInvalidRead)
		}
	}
	ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
	defer cancel()
	a, err := v.secretKey(ctx, left)
	if err != nil {
		return false, err
	}
	if left == right {
		return true, nil
	}
	b, err := v.secretKey(ctx, right)
	return err == nil && bytes.Equal(a, b), err
}

func (v LiveReadView) secretKey(ctx context.Context, key program.SecretKey) ([]byte, error) {
	var data map[string][]byte
	if key.Accepted {
		source, ok := v.Source.(secretDataSource)
		if v.SourceRevision != nil {
			v.SourceCommit = v.SourceRevision()
		}
		if !ok || v.SourceCommit == "" {
			return nil, sdk.Classified("accepted Secret source unavailable", sdk.ErrUnavailable)
		}
		var err error
		data, err = source.AcceptedSecretData(ctx, v.SourceCommit, v.Namespace(), key.Ref)
		if err != nil {
			return nil, sdk.Classified("accepted Secret read failed", sdk.ErrUnavailable)
		}
	} else {
		if v.Client == nil {
			return nil, sdk.Classified("live Secret reader unavailable", sdk.ErrUnavailable)
		}
		object, err := v.Client.CoreV1().Secrets(key.Ref.Namespace).Get(ctx, key.Ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, sdk.Classified("live Secret read failed", sdk.ErrUnavailable)
		}
		data = object.Data
	}
	value, ok := data[key.Key]
	if !ok {
		return nil, sdk.Classified("Secret key missing", sdk.ErrUnavailable)
	}
	return value, nil
}

func (v *cachedReadView) SecretEqual(left, right program.SecretKey) (bool, error) {
	var equal bool
	err := v.read("SecretEqual", []any{left, right}, &equal, func() (any, error) {
		return v.ReadView.SecretEqual(left, right)
	})
	return equal, err
}
