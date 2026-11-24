package rendered

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"

	"sdo.dev/operator/render"
	"sdo.dev/operator/renderenv"
	"sdo.dev/operator/sdk"
)

// ErrSecretSourceNoLiteral means the accepted render declares the Secret with
// no literal data (an external or injected value), so there is nothing to
// restore it from.
var ErrSecretSourceNoLiteral = errors.New("accepted source declares no literal data for the Secret")

// ErrSecretSourceNondeterministic means two renders of the accepted tree give
// the Secret different data (a generated password): a re-render is not the
// accepted value.
var ErrSecretSourceNondeterministic = errors.New("the accepted render of the Secret is not deterministic")

// AcceptedSecretData returns the literal data of one Secret as the deployer
// renders the accepted revision (SecretsInclude), for the restore step only:
// the controller writes it to the API server and nothing records it. The tree
// is rendered twice and the data must be equal, so a generated value is
// refused instead of replaced by another. Errors never carry a value.
func (s Source) AcceptedSecretData(ctx context.Context, revision, namespace string, ref sdk.ObjectRef) (map[string][]byte, error) {
	lookup, err := s.AcceptedSecretInventory(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	return lookup(ref)
}

// AcceptedSecretInventory produces one private incident lookup. The existing
// two-sample check still rejects generated values; key reads do no rendering.
func (s Source) AcceptedSecretInventory(ctx context.Context, revision, namespace string) (func(sdk.ObjectRef) (map[string][]byte, error), error) {
	first, err := s.renderSecretInventory(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	second, err := s.renderSecretInventory(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	return func(ref sdk.ObjectRef) (map[string][]byte, error) {
		if first[ref] == nil {
			return nil, ErrAcceptedManifestAbsent
		}
		a, err := secretLiteralData(first[ref])
		if err != nil {
			return nil, err
		}
		b, err := secretLiteralData(second[ref])
		if err != nil || !maps.EqualFunc(a, b, bytes.Equal) {
			return nil, ErrSecretSourceNondeterministic
		}
		return a, nil
	}, nil
}

func (s Source) renderSecretInventory(ctx context.Context, revision, namespace string) (map[sdk.ObjectRef]map[string]any, error) {
	if err := s.requireDesired(namespace); err != nil {
		return nil, err
	}
	effective, err := s.effectiveAt(ctx, revision)
	if err != nil {
		return nil, classifySource(err)
	}
	defer effective.Close()
	fsys, err := GitTreeFS(ctx, effective)
	if err != nil {
		return nil, classifySource(err)
	}
	deployment, err := legacyReadSource(fsys, s.Deployment)
	if err != nil {
		return nil, classifySource(err)
	}
	set, err := Desired(ctx, fsys, renderenv.For(s.Environment, namespace, render.SecretsInclude), s.Catalog, deployment)
	if err != nil {
		return nil, classifySource(err)
	}
	out := make(map[sdk.ObjectRef]map[string]any)
	for _, object := range set.Objects() {
		if object.Ref.Kind == "Secret" && object.Ref.Namespace == namespace && !object.Hook {
			out[object.Ref] = object.Object.Object
		}
	}
	return out, nil
}

// secretLiteralData decodes data and stringData (which wins) of a Secret.
func secretLiteralData(object map[string]any) (map[string][]byte, error) {
	out := map[string][]byte{}
	if data, ok := object["data"].(map[string]any); ok {
		for key, value := range data {
			text, _ := value.(string)
			decoded, err := base64.StdEncoding.DecodeString(text)
			if err != nil {
				return nil, fmt.Errorf("accepted Secret key %q is not base64", key)
			}
			out[key] = decoded
		}
	}
	if data, ok := object["stringData"].(map[string]any); ok {
		for key, value := range data {
			text, _ := value.(string)
			out[key] = []byte(text)
		}
	}
	if len(out) == 0 {
		return nil, ErrSecretSourceNoLiteral
	}
	return out, nil
}
