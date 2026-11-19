package memoryhost

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func parityTarget(user string) program.DatabaseTarget {
	return program.DatabaseTarget{
		Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store-a-0"}, Container: "store", Database: "app",
		Auth: &program.DatabaseAuth{Database: "admin", Username: program.CredentialRef{Env: user + "_USER"}, Password: program.CredentialRef{Env: user + "_PASS"}},
	}
}

// adminFailsView fails the admin account with err and answers the root account
// with a catalog that lacks the application user.
type adminFailsView struct {
	program.ReadView
	err error
}

func (v adminFailsView) DatabaseUsers(t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if t.Auth != nil && t.Auth.Username.Env == "ADMIN_USER" {
		return nil, v.err
	}
	return []program.DatabaseUser{{Name: "other"}}, nil
}

// fallbackProgram matches when the application account is missing from the
// first catalog that answers.
type fallbackProgram struct{}

func (fallbackProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "fallback"} }
func (fallbackProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	users, err := v.DatabaseUsers(parityTarget("ADMIN"))
	if err != nil {
		users, err = v.DatabaseUsers(parityTarget("ROOT"))
		if err != nil {
			return program.Verdict{}, err
		}
	}
	for _, u := range users {
		if u.Name == "app" {
			return program.Verdict{Result: program.NoMatch}, nil
		}
	}
	return program.Verdict{Result: program.Match}, nil
}

// Live, replay and the memory host's receipts give a program the same verdict for
// the same world, including when a credential is refused and when it is unavailable.
func TestLiveReplayAndHostAgreeOnAFallbackDiagnoseOverAFailedLogin(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want program.Result
	}{
		{"refused login falls back and matches", fmt.Errorf("rejected: %w", sdk.ErrRefused), program.Match},
		{"unavailable login still matches, with the failure attached", fmt.Errorf("timeout: %w", sdk.ErrUnavailable), program.Match},
	} {
		t.Run(c.name, func(t *testing.T) {
			snapshot := sdktest.Snapshot{NamespaceName: "app"}
			recorder := &sdktest.RecordingReadView{ReadView: adminFailsView{ReadView: snapshot.RuntimeReadView(), err: c.err}}
			live := program.RunDiagnose(context.Background(), fallbackProgram{}, recorder, program.Bindings{}, time.Second)

			replay := program.RunDiagnose(context.Background(), fallbackProgram{}, &sdktest.ReplayReadView{Snapshot: snapshot, Calls: recorder.Calls}, program.Bindings{}, time.Second)

			receipts, err := EncodeReceipts(recorder.Calls)
			if err != nil {
				t.Fatal(err)
			}
			calls, err := DecodeReceipts(receipts)
			if err != nil {
				t.Fatal(err)
			}
			host := program.RunDiagnose(context.Background(), fallbackProgram{}, &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}, program.Bindings{}, time.Second)

			for name, got := range map[string]program.Verdict{"live": live, "replay": replay, "host": host} {
				if got.Result != c.want {
					t.Errorf("%s verdict = %s (%+v), want %s", name, got.Result, got.Evidence, c.want)
				}
			}
			if len(live.Evidence) != len(replay.Evidence) || len(live.Evidence) != len(host.Evidence) {
				t.Fatalf("evidence differs: live %+v replay %+v host %+v", live.Evidence, replay.Evidence, host.Evidence)
			}
		})
	}
}
