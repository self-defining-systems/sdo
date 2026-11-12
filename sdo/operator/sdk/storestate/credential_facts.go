package storestate

import (
	"regexp"
	"sort"

	"sdo.dev/operator/sdk"
)

// Value-free credential facts for a Diagnose. Each helper reads only frozen
// results the view already carries and returns refusal classes, principal and
// user names and counts. None of them reads, derives or returns a credential,
// a password hash or a log line.

// RefusedStore is a store Pod that read healthy before and now refuses the
// credential its own container is configured with (the read signs in with the
// container's root or admin variable first, so this is the admin credential).
type RefusedStore struct {
	Component string        `json:"component"`
	Engine    Engine        `json:"engine"`
	Pod       sdk.ObjectRef `json:"pod"`
	// Class is auth-failed or auth-required.
	Class string `json:"class"`
}

// CredentialRefused lists the store Pods whose own read is refused as
// auth-failed or auth-required against a healthy baseline. A Pod the view has
// no baseline for is never listed: it was never read healthy, so the refusal
// is a coverage gap, not a change.
func CredentialRefused(view sdk.DetectionContext) []RefusedStore {
	var out []RefusedStore
	for _, store := range Changes(view) {
		if !store.Baselined || store.Error != "" {
			continue
		}
		if class := credentialRefusal(store.Changes); class != "" {
			out = append(out, RefusedStore{Component: store.Component, Engine: store.Engine, Pod: store.Pod, Class: class})
		}
	}
	return out
}

func credentialRefusal(changes []Change) string {
	for _, c := range changes {
		if c.Key().Kind == KindRefusal && c.After != nil && (c.After.Value == "auth-failed" || c.After.Value == "auth-required") {
			return c.After.Value
		}
	}
	return ""
}

// PrincipalChange names a principal whose password change time moved since the
// baseline. Only the time is read, never the password.
type PrincipalChange struct {
	Component string        `json:"component"`
	Engine    Engine        `json:"engine"`
	Pod       sdk.ObjectRef `json:"pod"`
	Principal string        `json:"principal"`
}

// PasswordChanged lists the principals whose recorded password change time
// differs from the healthy baseline. Today only mysql records it.
func PasswordChanged(view sdk.DetectionContext) []PrincipalChange {
	var out []PrincipalChange
	for _, store := range Changes(view) {
		if !store.Baselined || store.Error != "" {
			continue
		}
		for _, c := range store.Changes {
			if k := c.Key(); k.Kind == KindSetting && k.Name == "password-changed" && c.Before != nil && c.After != nil {
				out = append(out, PrincipalChange{Component: store.Component, Engine: store.Engine, Pod: store.Pod, Principal: k.Scope})
			}
		}
	}
	return out
}

// AuthFailure is one user a store refused to sign in, with how many log lines
// said so.
type AuthFailure struct {
	User  string `json:"user"`
	Count int    `json:"count"`
}

// MaxAuthFailures bounds the users AuthFailures returns.
const MaxAuthFailures = 16

// authFailurePatterns is the per-engine log pattern of a refused sign-in. Group
// 1 is the user name. The patterns match the store's own wording, which names
// the user and never the value.
var authFailurePatterns = map[Engine][]*regexp.Regexp{
	EnginePostgres: {regexp.MustCompile(`password authentication failed for user "([^"\s]{1,64})"`)},
	EngineMySQL:    {regexp.MustCompile(`Access denied for user '([^'\s]{1,64})'`)},
	EngineMongoDB: {
		regexp.MustCompile(`"msg":"Authentication failed".*?"user":"([^"\s]{1,64})"`),
		regexp.MustCompile(`Authentication failed.*? user[: ]+"?([^"\s,]{1,64})`),
	},
}

// AuthFailures counts, by user, the refused sign-ins in a store Pod's logs. It
// returns names and counts only, most refused first. An engine without a
// pattern (redis does not log the refused user) returns nil.
func AuthFailures(engine Engine, logs string) []AuthFailure {
	patterns := authFailurePatterns[engine]
	if len(patterns) == 0 {
		return nil
	}
	counts := map[string]int{}
	for _, line := range splitLines(logs) {
		for _, p := range patterns {
			if m := p.FindStringSubmatch(line); m != nil {
				counts[m[1]]++
				break
			}
		}
	}
	out := make([]AuthFailure, 0, len(counts))
	for user, n := range counts {
		out = append(out, AuthFailure{User: user, Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].User < out[j].User
	})
	if len(out) > MaxAuthFailures {
		out = out[:MaxAuthFailures]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

// RestoreBy says how a refused credential comes back. A store that still
// accepts SDO's sign-in has the in-place path instead: Restorable and
// RestoreStoreItem.
type RestoreBy string

const (
	// RestoreByRestart: the credential is configuration the store reads at
	// start, so restarting the store applies the configured one, and its data
	// survives the restart.
	RestoreByRestart RestoreBy = "restart"
	// RestoreNone: nothing in SDO restores it; escalate.
	RestoreNone RestoreBy = "none"
)

// RestorePath reports how a refused store's credential is restored. A redis
// requirepass is read at start: a restart restores it when the healthy
// baseline shows the data persists (appendonly) and the Pod mounts a claim.
// The check reads the baseline, because the current config section is unread
// while the read is refused. Every other engine has no restart-applied
// credential, so a refusal alone is RestoreNone.
func RestorePath(view sdk.DetectionContext, store RefusedStore) RestoreBy {
	if store.Engine != EngineRedis {
		return RestoreNone
	}
	baseline, ok := Baseline(view, store.Component, store.Pod)
	if !ok {
		return RestoreNone
	}
	if item, found := baseline.Lookup(Key{Kind: KindSetting, Name: "appendonly"}); !found || item.Value != "yes" {
		return RestoreNone
	}
	for _, pod := range view.Pods() {
		if pod.Namespace != store.Pod.Namespace || pod.Name != store.Pod.Name {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				return RestoreByRestart
			}
		}
	}
	return RestoreNone
}
