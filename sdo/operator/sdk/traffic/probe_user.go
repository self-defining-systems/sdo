package traffic

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// ProbeUser names a test user a scenario signs in as. SDO provisions the user
// through the application's own identity procedure (the application map's
// procedure marked probe_identity) with a password SDO generates; nothing is
// read from an application Secret. The engine puts the password in each
// iteration's State under As, so a setup step signs in with "{As}" in a
// header, path, query, form or JSON body, or with WithBasicAuth, and saves the
// session or token the steps send. Name contains the scenario's Marker.
// Operational memory and agents hold the name, never the password, and no
// sample carries it.
type ProbeUser struct {
	Name string `json:"name"`
	As   string `json:"as"`
}

// Passwords returns the password of a provisioned probe user. An error
// means the user cannot be used yet (provisioning is pending or failed); the
// iteration is then invalid and sends nothing.
type Passwords interface {
	Password(ctx context.Context, user string) (string, error)
}

var (
	probeUserPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.@-]{0,63}$`)
	stateKeyPattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,63}$`)
)

// passwordPlaceholder stands for a password in planned requests and samples.
const passwordPlaceholder = "[password]"

func (s Scenario) validateUsers() error {
	if len(s.Users) > 0 && (strings.TrimSpace(s.Marker) == "" || len(s.Marker) < 6) {
		return fmt.Errorf("scenario %s: a scenario that signs in as probe users needs a Marker of at least 6 characters, contained in every user name", s.ID)
	}
	keys, names := map[string]bool{}, map[string]bool{}
	for _, u := range s.Users {
		switch {
		case !probeUserPattern.MatchString(u.Name):
			return fmt.Errorf("scenario %s: probe user %q must be one word of letters, digits, '_', '.', '@' or '-'", s.ID, u.Name)
		case !strings.Contains(u.Name, s.Marker):
			return fmt.Errorf("scenario %s: probe user %q must contain the scenario's Marker %q", s.ID, u.Name, s.Marker)
		case !stateKeyPattern.MatchString(u.As):
			return fmt.Errorf("scenario %s: probe user As %q must be a State key (letters, digits, '_', '.', '-'), used as {%s}", s.ID, u.As, u.As)
		case keys[u.As]:
			return fmt.Errorf("scenario %s: two probe users set State key %q", s.ID, u.As)
		case names[u.Name]:
			return fmt.Errorf("scenario %s: probe user %q is declared twice", s.ID, u.Name)
		}
		keys[u.As], names[u.Name] = true, true
	}
	return nil
}

// userState resolves scenario's probe users into State values.
func (e *Engine) userState(ctx context.Context, scenario Scenario) (State, error) {
	if len(scenario.Users) == 0 {
		return nil, nil
	}
	if e.passwords == nil {
		return nil, fmt.Errorf("probe users: this prober provisions no probe identities, so scenario %s cannot sign in", scenario.ID)
	}
	values := State{}
	for _, u := range scenario.Users {
		value, err := e.passwords.Password(ctx, u.Name)
		if err != nil {
			return nil, fmt.Errorf("probe user %s: %v", u.Name, err)
		}
		if value == "" {
			return nil, fmt.Errorf("probe user %s has an empty password", u.Name)
		}
		values[u.As] = value
	}
	return values, nil
}

// redact replaces every password in the sample's text.
func (sample *Sample) redact(values State) {
	for _, value := range values {
		for _, field := range []*string{&sample.Request, &sample.Error, &sample.CleanupError, &sample.Step} {
			*field = strings.ReplaceAll(*field, value, passwordPlaceholder)
		}
	}
}
