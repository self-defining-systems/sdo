package sdk

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// ProjectCABundle replaces certificate bytes with an idempotent digest.
// It is shared by typed observations, raw API reads and capture projection.
func ProjectCABundle(ca []byte) []byte {
	if len(ca) == 0 {
		return nil
	}
	if len(ca) == 71 && strings.HasPrefix(string(ca), "sha256:") {
		if _, err := hex.DecodeString(string(ca[7:])); err == nil {
			return append([]byte(nil), ca...)
		}
	}
	digest := sha256.Sum256(ca)
	return []byte("sha256:" + hex.EncodeToString(digest[:]))
}

// ProjectWebhookCABundles removes raw caBundle bytes from arbitrary JSON trees.
// It projects embedded last-applied JSON too and mutates only the detached tree.
func ProjectWebhookCABundles(value any) {
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			ProjectWebhookCABundles(item)
		}
	case map[string]any:
		// kubectl stores a second object document as a string. Project it at
		// this same boundary; malformed copies cannot be safe evidence.
		const lastApplied = "kubectl.kubernetes.io/last-applied-configuration"
		if embedded, ok := v[lastApplied]; ok {
			text, valid := embedded.(string)
			if valid {
				projected, err := ProjectWebhookCAJSON([]byte(text))
				if err == nil {
					v[lastApplied] = string(projected)
				} else {
					delete(v, lastApplied)
				}
			} else {
				delete(v, lastApplied)
			}
		}
		if encoded, ok := v["caBundle"].(string); ok && encoded != "" {
			ca, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				ca = []byte(encoded)
			}
			v["caBundle"] = base64.StdEncoding.EncodeToString(ProjectCABundle(ca))
		}
		for _, item := range v {
			ProjectWebhookCABundles(item)
		}
	}
}

// ProjectWebhookCAJSON retains the Kubernetes response shape and numeric values
// while hiding CA bytes before a raw read can reach programs or recordings.
func ProjectWebhookCAJSON(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var parts [][]byte
	for {
		var value any
		if err := decoder.Decode(&value); err != nil {
			if err == io.EOF && len(parts) > 0 {
				return bytes.Join(parts, []byte("\n")), nil
			}
			return nil, fmt.Errorf("webhook CA JSON projection unavailable")
		}
		// Watch responses use a JSON string containing newline-delimited envelopes.
		if text, ok := value.(string); ok {
			projected, err := ProjectWebhookCAJSON([]byte(text))
			if err != nil {
				return nil, err
			}
			value = string(projected)
		} else {
			ProjectWebhookCABundles(value)
		}
		part, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
}
