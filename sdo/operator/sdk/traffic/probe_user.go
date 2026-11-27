package traffic

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
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
//
// Token, when set, names the State key for the token returned by the identity
// procedure. Use it directly (for example "{token}" in a header). Declare
// As for a password, Token for a token, or both. Both values are secret.
//
// SSHKey, when set, names the State key under which the engine puts the
// user's SSH public key as an authorized-keys line ("ssh-ed25519 AAAA..."),
// so a setup step registers it with the application ("{SSHKey}"). SDO
// derives the key pair from the same seed as the password, so it is the same
// on every host and after a restart; register it with OrExisting. The private
// key never leaves the prober: it signs the scenario's ssh steps, at most one
// user per scenario sets SSHKey, and a scenario with an ssh step needs one.
type ProbeUser struct {
	Name   string `json:"name"`
	As     string `json:"as,omitempty"`
	Token  string `json:"token,omitempty"`
	SSHKey string `json:"sshKey,omitempty"`
}

// Passwords returns the password of a provisioned probe user. An error
// means the user cannot be used yet (provisioning is pending or failed); the
// iteration is then invalid and sends nothing.
type Passwords interface {
	Password(ctx context.Context, user string) (string, error)
}

// ProbeTokens returns the API token of a provisioned probe user. A Passwords
// source that also implements ProbeTokens serves users that declare Token.
type ProbeTokens interface {
	Token(ctx context.Context, user string) (string, error)
}

// IdentityPendingError is what a probe identity source returns while
// the user's provisioning is under way. It is an environment wait, not a
// measurement of the application: Wait is how long one provisioning run may
// take, so a one-shot evaluation can wait for it instead of reporting a
// health failure.
type IdentityPendingError struct {
	Reason string
	Wait   time.Duration
}

func (e *IdentityPendingError) Error() string { return "identity pending: " + e.Reason }

// ProbeKeys returns the SSH public key line of a provisioned probe user. A
// Passwords source that also implements ProbeKeys serves scenarios whose
// users declare SSHKey; an error means the user cannot be used yet.
type ProbeKeys interface {
	SSHPublicKey(ctx context.Context, user string) (string, error)
}

var (
	probeUserPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.@-]{0,63}$`)
	stateKeyPattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,63}$`)
)

// passwordPlaceholder stands for a password in planned requests and samples.
const passwordPlaceholder = "[password]"

// publicKeyPlaceholder stands for an SSH public key line in planned requests.
const publicKeyPlaceholder = "[ssh-public-key]"

func (s Scenario) validateUsers() error {
	if len(s.Users) > 0 && (strings.TrimSpace(s.Marker) == "" || len(s.Marker) < 6) {
		return fmt.Errorf("scenario %s: a scenario that signs in as probe users needs a Marker of at least 6 characters, contained in every user name", s.ID)
	}
	keys, names, keyUsers := map[string]bool{}, map[string]bool{}, 0
	for _, u := range s.Users {
		switch {
		case !probeUserPattern.MatchString(u.Name):
			return fmt.Errorf("scenario %s: probe user %q must be one word of letters, digits, '_', '.', '@' or '-'", s.ID, u.Name)
		case !strings.Contains(u.Name, s.Marker):
			return fmt.Errorf("scenario %s: probe user %q must contain the scenario's Marker %q", s.ID, u.Name, s.Marker)
		case u.As == "" && u.Token == "":
			return fmt.Errorf("scenario %s: probe user %q needs As or Token", s.ID, u.Name)
		case names[u.Name]:
			return fmt.Errorf("scenario %s: probe user %q is declared twice", s.ID, u.Name)
		}
		names[u.Name] = true
		if u.SSHKey != "" {
			keyUsers++
		}
		for _, key := range []string{u.As, u.Token, u.SSHKey} {
			if key == "" {
				continue
			}
			if !stateKeyPattern.MatchString(key) {
				return fmt.Errorf("scenario %s: probe user State key %q must use letters, digits, '_', '.' or '-'", s.ID, key)
			}
			if keys[key] {
				return fmt.Errorf("scenario %s: State key %q is set by two probe-user values", s.ID, key)
			}
			keys[key] = true
		}
	}
	if keyUsers > 1 {
		return fmt.Errorf("scenario %s: at most one probe user sets SSHKey; it signs the scenario's ssh steps", s.ID)
	}
	if keyUsers == 0 && s.hasSSHStep() {
		return fmt.Errorf("scenario %s: an ssh step signs in with a probe user's key; declare a probe user with SSHKey", s.ID)
	}
	return nil
}

func (s Scenario) hasSSHStep() bool {
	for _, steps := range [][]Step{s.Setup, s.Steps, s.Cleanup} {
		for _, step := range steps {
			if s.stepTarget(step).Scheme == "ssh" {
				return true
			}
		}
	}
	return false
}

// keyUser is the probe user whose key signs the scenario's ssh steps.
func (s Scenario) keyUser() (ProbeUser, bool) {
	for _, u := range s.Users {
		if u.SSHKey != "" {
			return u, true
		}
	}
	return ProbeUser{}, false
}

// userState resolves scenario's probe users into State values: passwords
// under As, tokens under Token and public key lines under SSHKey.
func (e *Engine) userState(ctx context.Context, scenario Scenario) (passwords State, keys State, err error) {
	if len(scenario.Users) == 0 {
		return nil, nil, nil
	}
	if e.passwords == nil {
		return nil, nil, fmt.Errorf("probe users: this prober provisions no probe identities, so scenario %s cannot sign in", scenario.ID)
	}
	passwords, keys = State{}, State{}
	for _, u := range scenario.Users {
		if u.As != "" {
			value, err := e.passwords.Password(ctx, u.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("probe user %s: %w", u.Name, err)
			}
			if value == "" {
				return nil, nil, fmt.Errorf("probe user %s has an empty password", u.Name)
			}
			passwords[u.As] = value
		}
		if u.Token != "" {
			source, ok := e.passwords.(ProbeTokens)
			if !ok {
				return nil, nil, fmt.Errorf("probe user %s: this prober provisions no tokens", u.Name)
			}
			token, err := source.Token(ctx, u.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("probe user %s: %w", u.Name, err)
			}
			if token == "" {
				return nil, nil, fmt.Errorf("probe user %s has an empty token", u.Name)
			}
			passwords[u.Token] = token
		}
		if u.SSHKey == "" {
			continue
		}
		source, ok := e.passwords.(ProbeKeys)
		if !ok {
			return nil, nil, fmt.Errorf("probe user %s: this prober provisions no SSH keys", u.Name)
		}
		line, err := source.SSHPublicKey(ctx, u.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("probe user %s SSH key: %w", u.Name, err)
		}
		keys[u.SSHKey] = line
	}
	return passwords, keys, nil
}

// redact replaces probe credentials in each recorded text field.
func (sample *Sample) redact(values State) {
	for _, field := range []*string{&sample.Request, &sample.Error, &sample.Observed, &sample.CleanupError, &sample.Step} {
		*field = redactCredentials(*field, values)
	}
}

func redactCredentials(text string, values State) string {
	if text == "" || len(values) == 0 {
		return text
	}
	forms := make([]string, 0, len(values)*4)
	for _, value := range values {
		quoted := strconv.Quote(value)
		forms = append(forms, value, url.QueryEscape(value), url.PathEscape(value), quoted[1:len(quoted)-1])
	}
	sort.Slice(forms, func(i, j int) bool { return len(forms[i]) > len(forms[j]) })
	pairs := make([]string, 0, 2*len(forms))
	for _, form := range forms {
		pairs = append(pairs, form, "[redacted]")
	}
	return strings.NewReplacer(pairs...).Replace(text)
}

// identityWait is the provisioning wait an error carries, zero when the error
// is not an identity-pending one.
func identityWait(err error) time.Duration {
	var pending *IdentityPendingError
	if errors.As(err, &pending) {
		return max(pending.Wait, time.Second)
	}
	return 0
}
