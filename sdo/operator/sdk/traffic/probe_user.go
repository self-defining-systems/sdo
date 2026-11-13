package traffic

import (
	"context"
	"errors"
	"fmt"
	"regexp"
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
// SSHKey, when set, names the State key under which the engine puts the
// user's SSH public key as an authorized-keys line ("ssh-ed25519 AAAA..."),
// so a setup step registers it with the application ("{SSHKey}"). SDO
// derives the key pair from the same seed as the password, so it is the same
// on every host and after a restart; register it with OrExisting. The private
// key never leaves the prober: it signs the scenario's ssh steps, at most one
// user per scenario sets SSHKey, and a scenario with an ssh step needs one.
type ProbeUser struct {
	Name   string `json:"name"`
	As     string `json:"as"`
	SSHKey string `json:"sshKey,omitempty"`
}

// Passwords returns the password of a provisioned probe user. An error
// means the user cannot be used yet (provisioning is pending or failed); the
// iteration is then invalid and sends nothing.
type Passwords interface {
	Password(ctx context.Context, user string) (string, error)
}

// IdentityPendingError is what a Passwords or ProbeKeys source returns while
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
		if u.SSHKey != "" {
			keyUsers++
			switch {
			case !stateKeyPattern.MatchString(u.SSHKey):
				return fmt.Errorf("scenario %s: probe user SSHKey %q must be a State key (letters, digits, '_', '.', '-'), used as {%s}", s.ID, u.SSHKey, u.SSHKey)
			case keys[u.SSHKey] || u.SSHKey == u.As:
				return fmt.Errorf("scenario %s: State key %q is set by two probe-user values", s.ID, u.SSHKey)
			}
			keys[u.SSHKey] = true
		}
		switch {
		case !probeUserPattern.MatchString(u.Name):
			return fmt.Errorf("scenario %s: probe user %q must be one word of letters, digits, '_', '.', '@' or '-'", s.ID, u.Name)
		case !strings.Contains(u.Name, s.Marker):
			return fmt.Errorf("scenario %s: probe user %q must contain the scenario's Marker %q", s.ID, u.Name, s.Marker)
		case !stateKeyPattern.MatchString(u.As):
			return fmt.Errorf("scenario %s: probe user As %q must be a State key (letters, digits, '_', '.', '-'), used as {%s}", s.ID, u.As, u.As)
		case keys[u.As] || (u.SSHKey != "" && u.As == u.SSHKey):
			return fmt.Errorf("scenario %s: two probe users set State key %q", s.ID, u.As)
		case names[u.Name]:
			return fmt.Errorf("scenario %s: probe user %q is declared twice", s.ID, u.Name)
		}
		keys[u.As], names[u.Name] = true, true
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
// under As and public key lines under SSHKey. Only the passwords are secret.
func (e *Engine) userState(ctx context.Context, scenario Scenario) (passwords State, keys State, err error) {
	if len(scenario.Users) == 0 {
		return nil, nil, nil
	}
	if e.passwords == nil {
		return nil, nil, fmt.Errorf("probe users: this prober provisions no probe identities, so scenario %s cannot sign in", scenario.ID)
	}
	passwords, keys = State{}, State{}
	for _, u := range scenario.Users {
		value, err := e.passwords.Password(ctx, u.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("probe user %s: %w", u.Name, err)
		}
		if value == "" {
			return nil, nil, fmt.Errorf("probe user %s has an empty password", u.Name)
		}
		passwords[u.As] = value
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

// redact replaces every password in the sample's text.
func (sample *Sample) redact(values State) {
	for _, value := range values {
		for _, field := range []*string{&sample.Request, &sample.Error, &sample.Observed, &sample.CleanupError, &sample.Step} {
			*field = strings.ReplaceAll(*field, value, passwordPlaceholder)
		}
	}
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
