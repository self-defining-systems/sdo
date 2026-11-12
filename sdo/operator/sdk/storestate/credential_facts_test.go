package storestate_test

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

// refusedRedisView is a redis store that read healthy (appendonly as given) and
// now refuses its own credential. withClaim gives its Pod a PVC volume.
func refusedRedisView(t *testing.T, appendonly string, withClaim bool) sdktest.Snapshot {
	t.Helper()
	d := detector(t, config(t))
	healthy := strings.Replace(healthyRedis, "appendonly\nno", "appendonly\n"+appendonly, 1)
	view := promotedSnapshot(t, d, healthy, reading(noAuthRedis()))
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "cache-0"}}
	if withClaim {
		pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-cache-0"}}}}
	}
	view.PodList = []corev1.Pod{pod}
	return view
}

// The store refusing the credential its own container is configured with, on a
// Pod that read healthy before, is value-free evidence of a changed credential.
func TestCredentialRefusedNamesTheStoreThatRefusesItsOwnCredential(t *testing.T) {
	view := refusedRedisView(t, "yes", true)
	got := storestate.CredentialRefused(view)
	if len(got) != 1 || got[0].Component != "cache" || got[0].Pod.Name != "cache-0" || got[0].Class != "auth-required" {
		t.Fatalf("CredentialRefused = %+v", got)
	}
	d := detector(t, config(t))
	if got := storestate.CredentialRefused(promotedSnapshot(t, d, healthyRedis, reading(healthyRedis))); len(got) != 0 {
		t.Fatalf("a store answering its read: %+v", got)
	}
	// Refused before it ever read healthy is a coverage gap, not a change.
	if got := storestate.CredentialRefused(snapshot(t, d, reading(noAuthRedis()))); len(got) != 0 {
		t.Fatalf("a store never read healthy: %+v", got)
	}
}

// A redis password is read at start: restarting the store applies the configured
// one, so long as its data survives, which the baseline's persistence and a
// claim decide. Otherwise nothing restores it.
func TestRestorePathRedisCredentialByRestartOnlyWhenDataSurvives(t *testing.T) {
	for name, test := range map[string]struct {
		appendonly string
		claim      bool
		want       storestate.RestoreBy
	}{
		"persistent with a claim":   {"yes", true, storestate.RestoreByRestart},
		"persistent without claim":  {"yes", false, storestate.RestoreNone},
		"claim without persistence": {"no", true, storestate.RestoreNone},
	} {
		t.Run(name, func(t *testing.T) {
			view := refusedRedisView(t, test.appendonly, test.claim)
			refused := storestate.CredentialRefused(view)
			if len(refused) != 1 {
				t.Fatalf("refused = %+v", refused)
			}
			if got := storestate.RestorePath(view, refused[0]); got != test.want {
				t.Fatalf("RestorePath = %q, want %q", got, test.want)
			}
		})
	}
}

// A store with no restart-applied credential has nothing to restore from a
// refusal alone.
func TestRestorePathOtherEnginesAreNone(t *testing.T) {
	got := storestate.RestorePath(sdktest.Snapshot{}, storestate.RefusedStore{Component: "db", Engine: storestate.EnginePostgres, Class: "auth-failed"})
	if got != storestate.RestoreNone {
		t.Fatalf("RestorePath = %q", got)
	}
}

// Store log lines name the user and never the value; AuthFailures returns names
// and counts, never text.
func TestAuthFailuresNamesUsersAndCountsOnly(t *testing.T) {
	for name, test := range map[string]struct {
		engine storestate.Engine
		logs   string
		want   []storestate.AuthFailure
	}{
		"postgres": {storestate.EnginePostgres,
			"2026-01-01 FATAL:  password authentication failed for user \"app\"\nDETAIL: Connection matched pg_hba.conf line 99 and the password was hunter2\n" +
				"2026-01-01 FATAL:  password authentication failed for user \"app\"\n2026-01-01 FATAL:  password authentication failed for user \"ops\"\n",
			[]storestate.AuthFailure{{User: "app", Count: 2}, {User: "ops", Count: 1}}},
		"mysql": {storestate.EngineMySQL,
			"2026-01-01 [Warning] Access denied for user 'shop'@'10.0.0.4' (using password: YES)\n2026-01-01 [Warning] Access denied for user 'shop'@'10.0.0.5' (using password: NO)\n",
			[]storestate.AuthFailure{{User: "shop", Count: 2}}},
		"mongodb": {storestate.EngineMongoDB,
			`{"t":{"$date":"x"},"c":"ACCESS","msg":"Authentication failed","attr":{"mechanism":"SCRAM-SHA-256","user":"rate","error":"AuthenticationFailed"}}` + "\n",
			[]storestate.AuthFailure{{User: "rate", Count: 1}}},
		"none":  {storestate.EnginePostgres, "LOG: checkpoint complete\n", nil},
		"redis": {storestate.EngineRedis, "WRONGPASS invalid username-password pair\n", nil},
	} {
		t.Run(name, func(t *testing.T) {
			got := storestate.AuthFailures(test.engine, test.logs)
			if len(got) != len(test.want) {
				t.Fatalf("AuthFailures = %+v, want %+v", got, test.want)
			}
			for i := range got {
				if got[i] != test.want[i] {
					t.Fatalf("AuthFailures = %+v, want %+v", got, test.want)
				}
			}
		})
	}
}

// A mysql account's password-change time moving since the baseline names the
// account, never its password.
func TestPasswordChangedNamesTheAccount(t *testing.T) {
	d := detector(t, func() storestate.Config { c := config(t); c.Engine = storestate.EngineMySQL; return c }())
	changed := strings.Replace(healthyMySQL, "app\t%\t2026-01-01T00:00:00", "app\t%\t2026-02-02T00:00:00", 1)
	view := promotedSnapshot(t, d, healthyMySQL, reading(changed))
	got := storestate.PasswordChanged(view)
	if len(got) != 1 || got[0].Component != "cache" || got[0].Principal != "app@%" {
		t.Fatalf("PasswordChanged = %+v", got)
	}
	if got := storestate.PasswordChanged(promotedSnapshot(t, d, healthyMySQL, reading(healthyMySQL))); len(got) != 0 {
		t.Fatalf("unchanged: %+v", got)
	}
}
