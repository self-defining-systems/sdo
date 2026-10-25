package runtime

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/program"
)

// CredentialReferenceError identifies an unavailable reference without exposing
// its contents or an upstream API response. Reason is a fixed diagnostic code.
type CredentialReferenceError struct {
	Reference program.CredentialRef
	Reason    string
}

func (e *CredentialReferenceError) Error() string {
	if k := e.Reference.ConfigMap; k != nil {
		return fmt.Sprintf("credential config_map %s key %s assignment %s: %s", k.Name, k.Key, k.Assignment, e.Reason)
	}
	return fmt.Sprintf("credential env %s: %s", e.Reference.Env, e.Reason)
}

// Literal single-line assignments only; no shell is evaluated.
var credentialAssignment = regexp.MustCompile(`^(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)[ \t]*=[ \t]*(.*)$`)
var bareCredential = regexp.MustCompile(`^[A-Za-z0-9_./:@%+,=!~^#-]+$`)

func assignmentCredential(content, name string) (string, bool) {
	matches := 0
	value := ""
	valid := true
	for _, line := range strings.Split(content, "\n") {
		m := credentialAssignment.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil || m[1] != name {
			continue
		}
		matches++
		raw := strings.TrimSpace(m[2])
		if len(raw) > 0 && (raw[0] == '\'' || raw[0] == '"') {
			quote := raw[0]
			end := strings.IndexByte(raw[1:], quote)
			if end < 0 {
				valid = false
				continue
			}
			end++
			tail := strings.TrimSpace(raw[end+1:])
			if tail != "" && !strings.HasPrefix(tail, "#") {
				valid = false
				continue
			}
			value = raw[1:end]
			if strings.ContainsAny(value, "\r\x00") || (quote == '"' && strings.ContainsAny(value, "\\$`")) {
				valid = false
			}
		} else {
			if at := strings.Index(raw, " #"); at >= 0 {
				raw = strings.TrimSpace(raw[:at])
			}
			if !bareCredential.MatchString(raw) {
				valid = false
				continue
			}
			value = raw
		}
	}
	return value, matches == 1 && valid && value != ""
}

func (c TypedPlanClient) resolveCredential(ctx context.Context, namespace string, ref program.CredentialRef) (string, error) {
	fail := func(reason string) (string, error) {
		return "", &CredentialReferenceError{Reference: ref, Reason: reason}
	}
	if err := ref.Validate(); err != nil {
		return fail("invalid reference")
	}
	if err := c.checkPlanNamespace(namespace); err != nil {
		return fail("target namespace refused")
	}
	if ref.ConfigMap == nil {
		return fail("env is resolved inside the container")
	}
	if c.Client == nil {
		return fail("ConfigMap transport unavailable")
	}
	k := ref.ConfigMap
	cm, err := c.Client.CoreV1().ConfigMaps(namespace).Get(ctx, k.Name, metav1.GetOptions{})
	if err != nil {
		return fail("ConfigMap read unavailable; check name and existing RBAC")
	}
	value, ok := cm.Data[k.Key]
	if !ok {
		if b, found := cm.BinaryData[k.Key]; found {
			value, ok = string(b), true
		}
	}
	if !ok {
		return fail("key missing")
	}
	if k.Assignment != "" {
		value, ok = assignmentCredential(value, k.Assignment)
		if !ok {
			return fail("expected exactly one nonempty literal assignment; expansion and duplicates refused")
		}
	}
	if value == "" || strings.ContainsRune(value, 0) || !utf8.ValidString(value) {
		return fail("key is empty or invalid")
	}
	return value, nil
}

func databaseSafeError(err error, fallback string) error {
	var ref *CredentialReferenceError
	if errors.As(err, &ref) {
		return ref
	}
	return fmt.Errorf("%s", fallback)
}
