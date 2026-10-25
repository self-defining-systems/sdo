package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
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
	cm, err := c.credentialConfigMap(ctx, namespace, k.Name)
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

// credentialConfigMap bypasses REST Request's response transformation: at V(10)
// client-go logs entire successful and failed response bodies. Reuse the same
// authenticated transport/RBAC while decoding bounded bytes privately here.
func (c TypedPlanClient) credentialConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	rawClient := c.Client.CoreV1().RESTClient()
	client, ok := rawClient.(*rest.RESTClient)
	if ok && client == nil {
		// client-go's in-memory fake has no HTTP transport and no body logger.
		return c.Client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	}
	if !ok || client.Client == nil {
		return nil, fmt.Errorf("credential API transport unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.Get().Namespace(namespace).Resource("configmaps").Name(name).URL().String(), nil)
	if err != nil {
		return nil, fmt.Errorf("credential API request unavailable")
	}
	request.Header.Set("Accept", "application/json")
	transport := *client.Client
	transport.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := transport.Do(request)
	if err != nil {
		return nil, fmt.Errorf("credential API request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("credential API read refused")
	}
	const capBytes = 2 << 20
	body, err := io.ReadAll(io.LimitReader(response.Body, capBytes+1))
	if err != nil || len(body) > capBytes {
		return nil, fmt.Errorf("credential API response unavailable")
	}
	var value corev1.ConfigMap
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, fmt.Errorf("credential API response invalid")
	}
	return &value, nil
}
