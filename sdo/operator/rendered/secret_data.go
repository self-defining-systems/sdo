package rendered

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/render"
	"sdo.dev/operator/renderenv"
	"sdo.dev/operator/sdk"
)

// SecretSourceReason says why accepted source cannot supply a Secret's data.
type SecretSourceReason string

const (
	// SecretNotDeclared: the accepted manifests do not declare the Secret.
	SecretNotDeclared SecretSourceReason = "secret_not_declared"
	// SecretNotLiteral: the Secret is declared, but its data is not written
	// literally in a rendered manifest (a generator input, an external secret
	// store, or no data at all). SDO never loads such inputs.
	SecretNotLiteral SecretSourceReason = "secret_not_literal"
	// SecretSourceUnavailable: the accepted source could not be rendered.
	SecretSourceUnavailable SecretSourceReason = "secret_source_unavailable"
)

// SecretSourceError is a typed refusal. Its text never carries a value or a
// renderer message (which could quote one).
type SecretSourceError struct {
	Ref    sdk.ObjectRef
	Reason SecretSourceReason
}

func (e *SecretSourceError) Error() string {
	name := e.Ref.Namespace + "/" + e.Ref.Name
	switch e.Reason {
	case SecretNotDeclared:
		return fmt.Sprintf("%s: accepted source does not declare Secret %s", e.Reason, name)
	case SecretNotLiteral:
		return fmt.Sprintf("%s: accepted source declares Secret %s without literal data (generated or external secret material is never loaded); it cannot be restored from source", e.Reason, name)
	}
	return fmt.Sprintf("%s: accepted source for Secret %s could not be rendered", e.Reason, name)
}

// SecretData is the literal data accepted source declares for one Secret.
// The values live only in Go memory: every printable and serialized form names
// keys only, and only the Go Secret writer reads Values.
type SecretData struct {
	values map[string][]byte
	// Type and Labels are the declared Secret's, used when it must be created.
	Type   string
	Labels map[string]string
}

// Keys returns the declared data keys, sorted.
func (d SecretData) Keys() []string {
	keys := make([]string, 0, len(d.values))
	for key := range d.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Values returns a copy of the declared values for the Go Secret writer.
func (d SecretData) Values() map[string][]byte {
	out := make(map[string][]byte, len(d.values))
	for key, value := range d.values {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

// Digests maps each key to a short sha256 digest of its declared value.
func (d SecretData) Digests() map[string]string {
	out := make(map[string]string, len(d.values))
	for key, value := range d.values {
		out[key] = SecretValueDigest(value)
	}
	return out
}

// SecretValueDigest is the short digest receipts name instead of a value.
func SecretValueDigest(value []byte) string {
	sum := sha256.Sum256(value)
	return fmt.Sprintf("sha256:%x", sum[:6])
}

func (d SecretData) String() string   { return "SecretData{keys: [" + strings.Join(d.Keys(), " ") + "]}" }
func (d SecretData) GoString() string { return d.String() }

// Format keeps every fmt verb on the key-only form.
func (d SecretData) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte(d.String())) }

// MarshalJSON writes keys only.
func (d SecretData) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"keys": d.Keys()})
}

// SecretKeyDiff names the keys whose live value differs from, is missing
// from, or is not declared by accepted source. It holds no value.
type SecretKeyDiff struct {
	Differs []string `json:"differs,omitempty"`
	Missing []string `json:"missing,omitempty"`
	Extra   []string `json:"extra,omitempty"`
}

// Changed reports any difference.
func (d SecretKeyDiff) Changed() bool { return len(d.Differs)+len(d.Missing)+len(d.Extra) > 0 }

// String is the key-only evidence an agent reads.
func (d SecretKeyDiff) String() string {
	var parts []string
	for _, part := range []struct {
		name string
		keys []string
	}{{"differ", d.Differs}, {"missing", d.Missing}, {"not declared", d.Extra}} {
		if len(part.keys) > 0 {
			parts = append(parts, fmt.Sprintf("%s [%s]", part.name, strings.Join(part.keys, " ")))
		}
	}
	if len(parts) == 0 {
		return "data matches accepted source"
	}
	return "data keys " + strings.Join(parts, "; ")
}

// Compare compares declared data with live data inside Go.
func (d SecretData) Compare(live map[string][]byte) SecretKeyDiff {
	var diff SecretKeyDiff
	for _, key := range d.Keys() {
		value, ok := live[key]
		switch {
		case !ok:
			diff.Missing = append(diff.Missing, key)
		case subtle.ConstantTimeCompare(value, d.values[key]) != 1:
			diff.Differs = append(diff.Differs, key)
		}
	}
	for key := range live {
		if _, ok := d.values[key]; !ok {
			diff.Extra = append(diff.Extra, key)
		}
	}
	sort.Strings(diff.Extra)
	return diff
}

// AcceptedSecretData renders revision with SecretsLiteral and returns the
// literal data the accepted manifests declare for ref. It is the only read that
// resolves Secret values; it never caches them and never returns a renderer
// error (which could quote one).
func (s Source) AcceptedSecretData(ctx context.Context, revision, namespace string, ref sdk.ObjectRef) (SecretData, error) {
	refuse := func(reason SecretSourceReason) (SecretData, error) {
		return SecretData{}, &SecretSourceError{Ref: ref, Reason: reason}
	}
	if ref.APIVersion != "v1" || ref.Kind != "Secret" || ref.Name == "" || ref.Namespace != namespace {
		return refuse(SecretNotDeclared)
	}
	if err := s.requireDesired(namespace); err != nil {
		return refuse(SecretSourceUnavailable)
	}
	effective, err := s.effectiveAt(ctx, revision)
	if err != nil {
		return refuse(SecretSourceUnavailable)
	}
	defer effective.Close()
	fsys, err := GitTreeFS(ctx, effective)
	if err != nil {
		return refuse(SecretSourceUnavailable)
	}
	deployment, err := legacyReadSource(fsys, s.Deployment)
	if err != nil {
		return refuse(SecretSourceUnavailable)
	}
	set, err := Desired(ctx, fsys, renderenv.For(s.Environment, namespace, render.SecretsLiteral), s.Catalog, deployment)
	if err != nil {
		return refuse(SecretSourceUnavailable)
	}
	for _, object := range set.Objects() {
		if object.Hook || object.Ref != ref {
			continue
		}
		data := SecretData{values: map[string][]byte{}}
		data.Type, _ = object.Object.Object["type"].(string)
		data.Labels = object.Object.GetLabels()
		encoded, _ := object.Object.Object["data"].(map[string]any)
		for key, value := range encoded {
			text, ok := value.(string)
			decoded, err := base64.StdEncoding.DecodeString(text)
			if !ok || err != nil {
				return refuse(SecretNotLiteral)
			}
			data.values[key] = decoded
		}
		literal, _ := object.Object.Object["stringData"].(map[string]any)
		for key, value := range literal {
			text, ok := value.(string)
			if !ok {
				return refuse(SecretNotLiteral)
			}
			data.values[key] = []byte(text)
		}
		if len(data.values) == 0 {
			return refuse(SecretNotLiteral)
		}
		return data, nil
	}
	return refuse(SecretNotDeclared)
}
