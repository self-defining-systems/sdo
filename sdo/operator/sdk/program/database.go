package program

import (
	"fmt"
	"regexp"
	"sdo.dev/operator/sdk"
	"sort"
	"strings"
)

const MongoDB = "mongodb"

// CredentialRef selects exactly one reference kind. Values are resolved only by
// the executor and never enter plans or recordings.
type CredentialRef struct {
	Env       string                  `json:"env,omitempty"`
	ConfigMap *ConfigMapCredentialRef `json:"config_map,omitempty"`
}

// ConfigMapCredentialRef selects a key in the target Pod namespace. Assignment
// optionally selects exactly one literal NAME=value line (shell or dotenv).
// Expansion, command substitution and duplicate assignments are refused.
type ConfigMapCredentialRef struct {
	Name       string `json:"name"`
	Key        string `json:"key"`
	Assignment string `json:"assignment,omitempty"`
}

// DatabaseAuth authenticates catalog access inside the selected container.
// Nil Auth uses the database's existing local unauthenticated bootstrap policy.
type DatabaseAuth struct {
	Database string        `json:"database"`
	Username CredentialRef `json:"username"`
	Password CredentialRef `json:"password"`
}

// DatabaseTarget selects one MongoDB catalog on a named application Pod.
// Shell is mongo (default) or mongosh. Connections are local to that container.
type DatabaseTarget struct {
	Engine    string        `json:"engine"`
	Pod       sdk.ObjectRef `json:"pod"`
	Container string        `json:"container"`
	Database  string        `json:"database"`
	Auth      *DatabaseAuth `json:"auth,omitempty"`
	Shell     string        `json:"shell,omitempty"`
}

type DatabaseRole struct {
	Database string `json:"database"`
	Name     string `json:"name"`
}

// DatabaseUser contains only catalog identity and directly granted roles.
// Password hashes, credentials and authentication restrictions are excluded.
type DatabaseUser struct {
	Name  string         `json:"name"`
	Roles []DatabaseRole `json:"roles"`
}

// EnsureDatabaseUser creates a missing user or restores its exact direct roles.
// Password is read inside the container only when creating the user. Existing
// passwords and all unrelated users are preserved. This effect is irreversible.
type EnsureDatabaseUser struct {
	Target   DatabaseTarget
	User     string
	Roles    []DatabaseRole
	Password CredentialRef
	Reason   string
}

func (EnsureDatabaseUser) action() {}

var credentialEnv = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (r CredentialRef) Validate() error {
	if (r.Env != "") == (r.ConfigMap != nil) {
		return fmt.Errorf("credential requires exactly one env or config_map reference")
	}
	if r.ConfigMap != nil {
		k := r.ConfigMap
		if k.Name == "" || k.Key == "" || strings.ContainsAny(k.Name+k.Key, "/\\\x00\n\r") || (k.Assignment != "" && !credentialEnv.MatchString(k.Assignment)) {
			return fmt.Errorf("credential config_map requires a name, key and optional assignment name")
		}
		return nil
	}
	if !credentialEnv.MatchString(r.Env) {
		return fmt.Errorf("credential requires an environment variable reference")
	}
	return nil
}
func databaseNameValid(s string) bool {
	return s != "" && !strings.ContainsAny(s, "/\\. \"$:\x00\n\r\t") && !strings.HasPrefix(s, "-") && len(s) <= 63
}

func (t DatabaseTarget) Validate() error {
	if t.Engine != MongoDB {
		return fmt.Errorf("database catalog supports mongodb only")
	}
	if t.Pod.APIVersion != "v1" || t.Pod.Kind != "Pod" || t.Pod.Namespace == "" || t.Pod.Name == "" || t.Container == "" {
		return fmt.Errorf("database catalog requires a named v1 Pod and container")
	}
	if !databaseNameValid(t.Database) || (t.Shell != "" && t.Shell != "mongo" && t.Shell != "mongosh") {
		return fmt.Errorf("database catalog requires a valid database and mongo or mongosh shell")
	}
	if t.Auth != nil {
		if !databaseNameValid(t.Auth.Database) {
			return fmt.Errorf("authentication database is invalid")
		}
		if err := t.Auth.Username.Validate(); err != nil {
			return err
		}
		if err := t.Auth.Password.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (u DatabaseUser) Validate() error {
	if strings.TrimSpace(u.Name) == "" || strings.ContainsAny(u.Name, "\x00\n\r") {
		return fmt.Errorf("database user name is required")
	}
	seen := map[DatabaseRole]bool{}
	for _, r := range u.Roles {
		if !databaseNameValid(r.Database) || strings.TrimSpace(r.Name) == "" || strings.ContainsAny(r.Name, "\x00\n\r") {
			return fmt.Errorf("database role requires database and name")
		}
		if seen[r] {
			return fmt.Errorf("duplicate database role")
		}
		seen[r] = true
	}
	return nil
}

func (a EnsureDatabaseUser) Validate() error {
	if err := a.Target.Validate(); err != nil {
		return err
	}
	if err := (DatabaseUser{Name: a.User, Roles: a.Roles}).Validate(); err != nil {
		return err
	}
	return a.Password.Validate()
}

// NormalizeDatabaseUsers validates, sorts and defensively copies nonsecret
// catalog observations. Duplicate users or roles are refused.
func NormalizeDatabaseUsers(users []DatabaseUser) ([]DatabaseUser, error) {
	out := make([]DatabaseUser, len(users))
	seen := map[string]bool{}
	for i, u := range users {
		if err := u.Validate(); err != nil {
			return nil, err
		}
		if seen[u.Name] {
			return nil, fmt.Errorf("duplicate database user")
		}
		seen[u.Name] = true
		out[i] = DatabaseUser{Name: u.Name, Roles: append([]DatabaseRole{}, u.Roles...)}
		sort.Slice(out[i].Roles, func(a, b int) bool {
			x, y := out[i].Roles[a], out[i].Roles[b]
			return x.Database < y.Database || x.Database == y.Database && x.Name < y.Name
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ApplyEnsureDatabaseUser projects a typed catalog mutation on a recorded open
// catalog. It never reads credentials or invents an unavailable observation.
func ApplyEnsureDatabaseUser(users []DatabaseUser, a EnsureDatabaseUser) ([]DatabaseUser, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	out, err := NormalizeDatabaseUsers(users)
	if err != nil {
		return nil, err
	}
	found := false
	for i := range out {
		if out[i].Name == a.User {
			out[i].Roles = append([]DatabaseRole{}, a.Roles...)
			found = true
		}
	}
	if !found {
		out = append(out, DatabaseUser{Name: a.User, Roles: a.Roles})
	}
	return NormalizeDatabaseUsers(out)
}

func validateDatabaseAction(action Action) error {
	switch a := action.(type) {
	case EnsureDatabaseUser:
		return a.Validate()
	case *EnsureDatabaseUser:
		if a == nil {
			return fmt.Errorf("nil EnsureDatabaseUser")
		}
		return a.Validate()
	}
	return nil
}

func (v snapshotReadView) DatabaseUsers(t DatabaseTarget) ([]DatabaseUser, error) {
	if source, ok := v.DetectionContext.(interface {
		DatabaseUsers(DatabaseTarget) ([]DatabaseUser, error)
	}); ok {
		return source.DatabaseUsers(t)
	}
	return nil, fmt.Errorf("database catalog observation unavailable")
}
func (v *checkedReadView) DatabaseUsers(t DatabaseTarget) ([]DatabaseUser, error) {
	r, err := v.ReadView.DatabaseUsers(t)
	v.check(err)
	return r, err
}
