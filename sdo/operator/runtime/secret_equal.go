package runtime

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	var version string
	a, err := v.secretKey(ctx, left, &version)
	leftMissing := errors.Is(err, sdk.ErrObjectAbsent)
	if err != nil && !leftMissing {
		return false, err
	}
	if left == right {
		return !leftMissing, nil
	}
	b, err := v.secretKey(ctx, right, nil)
	if errors.Is(err, sdk.ErrObjectAbsent) {
		return false, nil
	}
	equal := err == nil && !leftMissing && bytes.Equal(a, b)
	if err == nil && !equal && right.Accepted && left.Ref == right.Ref && left.Key == right.Key && version != "" && v.secretComparisons != nil {
		v.secretComparisons.mu.Lock()
		v.secretComparisons.versions[left] = version
		v.secretComparisons.mu.Unlock()
	}
	return equal, err
}

func (v LiveReadView) secretKey(ctx context.Context, key program.SecretKey, version *string) ([]byte, error) {
	var data map[string][]byte
	if key.Accepted {
		source, ok := v.Source.(secretDataSource)
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
			if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
				return nil, sdk.Classified("live Secret read refused", sdk.ErrRefused)
			}
			return nil, sdk.Classified("live Secret read failed", sdk.ErrUnavailable)
		}
		if version != nil {
			*version = object.ResourceVersion
		}
		data = object.Data
	}
	value, ok := data[key.Key]
	if !ok {
		if !key.Accepted {
			return nil, sdk.ErrObjectAbsent
		}
		return nil, sdk.Classified("accepted Secret key missing", sdk.ErrUnavailable)
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

type secretComparisons struct {
	mu       sync.Mutex
	versions map[program.SecretKey]string
}

func (v LiveReadView) bindSecretRestore(action *program.ReconcileFromSource) error {
	if v.secretComparisons == nil || len(action.Keys) == 0 || v.SourceCommit == "" {
		return errors.New("Secret restore requires compared keys from the incident view")
	}
	v.secretComparisons.mu.Lock()
	defer v.secretComparisons.mu.Unlock()
	action.SecretCommit = v.SourceCommit
	action.SecretResourceVersion = ""
	for _, key := range action.Keys {
		version := v.secretComparisons.versions[program.SecretKey{Ref: action.Ref, Key: key}]
		if version == "" || action.SecretResourceVersion != "" && action.SecretResourceVersion != version {
			return errors.New("Secret restore keys lack a shared mismatched observation")
		}
		action.SecretResourceVersion = version
	}
	return nil
}
