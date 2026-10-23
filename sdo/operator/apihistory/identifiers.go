package apihistory

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

type identifierGrammar func(string) []string

// The single field-to-grammar policy for every retained identity. Shared shapes
// use semantic field names so references, templates and lists cannot diverge.
// These are disclosure constraints, sometimes stricter than API admission:
// custom metrics use qualified names and env names use C identifiers.
var identifierGrammars = map[string]identifierGrammar{
	"metadata.name":            validation.IsDNS1123Subdomain,
	"Service.metadata.name":    validation.IsDNS1035Label,
	"metadata.namespace":       validation.IsDNS1123Label,
	"metadata.resourceVersion": decimalIdentifier,
	"label.key":                validation.IsQualifiedName,
	"selector.key":             validation.IsQualifiedName,
	"data.key":                 validation.IsConfigMapKey,
	"container.name":           validation.IsDNS1123Label,
	"env.name":                 validation.IsCIdentifier,
	"volume.name":              validation.IsDNS1123Label,
	"node.name":                validation.IsDNS1123Subdomain,
	"container.port.name":      validation.IsValidPortName,
	"service.port.name":        validation.IsDNS1123Label,
	"endpoint.port.name":       validation.IsDNS1123Label,
	"reference.name":           validation.IsDNS1123Subdomain,
	"reference.namespace":      validation.IsDNS1123Label,
	"reference.key":            validation.IsConfigMapKey,
	"reference.kind":           validation.IsCIdentifier,
	"reference.apiVersion":     apiVersionIdentifier,
	"metric.name":              validation.IsQualifiedName,
	"resource.name":            validation.IsQualifiedName,
	"Pod.condition.type":       validation.IsQualifiedName,
	"request.group":            validation.IsDNS1123Subdomain,
	"request.version":          validation.IsDNS1035Label,
	"request.resource":         validation.IsDNS1123Label,
	"request.namespace":        validation.IsDNS1123Label,
	"request.name":             validation.IsDNS1123Subdomain,
	"request.subresource":      validation.IsDNS1123Label,
}

func decimalIdentifier(s string) []string {
	if s != "" && strings.IndexFunc(s, func(r rune) bool { return r < '0' || r > '9' }) < 0 {
		return nil
	}
	return []string{"not a decimal identifier"}
}
func apiVersionIdentifier(s string) []string {
	parts := strings.Split(s, "/")
	if len(parts) == 1 {
		return validation.IsDNS1035Label(s)
	}
	if len(parts) == 2 && len(validation.IsDNS1123Subdomain(parts[0])) == 0 {
		return validation.IsDNS1035Label(parts[1])
	}
	return []string{"not a group/version identifier"}
}
func identifierField(field string) projection {
	grammar, ok := identifierGrammars[field]
	if !ok {
		panic("missing identifier grammar: " + field)
	}
	return func(v any) (any, bool) {
		s, ok := v.(string)
		if !ok || len(grammar(s)) != 0 {
			return nil, false
		}
		return s, true
	}
}
func identifierKeys(field string) projection {
	valid := identifierField(field)
	return func(v any) (any, bool) {
		input, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		output := make(map[string]any, len(input))
		for key := range input {
			if _, ok := valid(key); ok {
				output[key] = ""
			}
		}
		return output, true
	}
}

// Invalid opaque identities preserve equality within this history. Domains keep
// unrelated identity classes from correlating, and a private per-history key
// prevents offline dictionary matching from the public incident ID or a digest.
func (s *Store) opaqueIdentifier(field string) projection {
	valid := identifierField(field)
	return func(v any) (any, bool) {
		if value, ok := valid(v); ok {
			return value, true
		}
		raw, ok := v.(string)
		if !ok {
			return nil, false
		}
		mac := hmac.New(sha256.New, s.identityKey)
		mac.Write([]byte(field))
		mac.Write([]byte{0})
		mac.Write([]byte(raw))
		return "opaque:" + hex.EncodeToString(mac.Sum(nil)), true
	}
}

// Store owns sanitization so all recorders, including metadata-only responses,
// receive the same boundary. Never feed sanitized tokens back through this path.
func (s *Store) projectEvent(e Event) Event {
	for _, field := range []struct {
		policy string
		target *string
		opaque bool
	}{
		{"request.group", &e.Group, false}, {"request.version", &e.Version, false},
		{"request.resource", &e.Resource, false}, {"request.namespace", &e.Namespace, false},
		{"request.name", &e.Name, true}, {"request.subresource", &e.Subresource, true},
		{"metadata.resourceVersion", &e.ResourceVersion, true},
	} {
		if *field.target == "" {
			continue
		}
		projection := identifierField(field.policy)
		if field.opaque {
			projection = s.opaqueIdentifier(field.policy)
		}
		value, ok := projection(*field.target)
		if !ok {
			*field.target = ""
			e.Partial = true
		} else {
			if value != *field.target {
				e.Partial = true
			}
			*field.target = value.(string)
		}
	}
	return e
}

func historyIdentityKey(directory string) ([]byte, error) {
	path := filepath.Join(directory, "identity.key")
	key, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		key = make([]byte, 32)
		if _, err = rand.Read(key); err != nil {
			return nil, err
		}
		f, createErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if errors.Is(createErr, os.ErrExist) {
			return historyIdentityKey(directory)
		}
		if createErr != nil {
			return nil, createErr
		}
		_, err = f.Write(key)
		closeErr := f.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	} else if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, errors.New("invalid history identity key")
	}
	return key, nil
}
