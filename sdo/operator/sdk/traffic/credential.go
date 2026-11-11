package traffic

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// Credential names a value the application keeps in a Secret of its own
// namespace, for a scenario that must authenticate (an API token, the
// password of an account the application ships). Only the prober reads the
// value, at run time, the way a Pod takes a Secret key by env; it puts the
// value in each iteration's State under As, so a step sends it with
// "{As}" in a header, path, query, form or JSON body, or with WithBasicAuth.
// Operational memory and agents hold the reference, never the value, and no
// sample carries it.
type Credential struct {
	Secret string `json:"secret"`
	Key    string `json:"key"`
	As     string `json:"as"`
}

// CredentialSource reads one key of a Secret in the application namespace.
type CredentialSource interface {
	Value(ctx context.Context, secret, key string) (string, error)
}

var (
	secretKeyPattern = regexp.MustCompile(`^[-._a-zA-Z0-9]{1,253}$`)
	stateKeyPattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,63}$`)
)

func (s Scenario) validateCredentials() error {
	seen := map[string]bool{}
	for _, c := range s.Credentials {
		switch {
		case !namePattern.MatchString(c.Secret):
			return fmt.Errorf("scenario %s: credential secret %q must be a Secret name", s.ID, c.Secret)
		case !secretKeyPattern.MatchString(c.Key):
			return fmt.Errorf("scenario %s: credential key %q must be a Secret data key", s.ID, c.Key)
		case !stateKeyPattern.MatchString(c.As):
			return fmt.Errorf("scenario %s: credential As %q must be a State key (letters, digits, '_', '.', '-'), used as {%s}", s.ID, c.As, c.As)
		case seen[c.As]:
			return fmt.Errorf("scenario %s: two credentials set State key %q", s.ID, c.As)
		}
		seen[c.As] = true
	}
	return nil
}

// credentialState resolves scenario's credentials into State values.
func (e *Engine) credentialState(ctx context.Context, scenario Scenario) (State, error) {
	if len(scenario.Credentials) == 0 {
		return nil, nil
	}
	if e.credentials == nil {
		return nil, fmt.Errorf("credentials: this prober reads no Secrets, so scenario %s cannot authenticate", scenario.ID)
	}
	values := State{}
	for _, c := range scenario.Credentials {
		value, err := e.credentials.Value(ctx, c.Secret, c.Key)
		if err != nil {
			return nil, fmt.Errorf("credential %s key %s: %v", c.Secret, c.Key, err)
		}
		if value == "" {
			return nil, fmt.Errorf("credential %s key %s is empty", c.Secret, c.Key)
		}
		values[c.As] = value
	}
	return values, nil
}

// redact replaces every credential value in the sample's text.
func (sample *Sample) redact(values State) {
	for _, value := range values {
		for _, field := range []*string{&sample.Request, &sample.Error, &sample.CleanupError, &sample.Step} {
			*field = strings.ReplaceAll(*field, value, "[credential]")
		}
	}
}
