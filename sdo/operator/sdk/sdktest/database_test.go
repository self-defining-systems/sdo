package sdktest

import (
	"encoding/json"
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

type catalogRead struct {
	program.ReadView
	fail bool
}

func (v catalogRead) DatabaseUsers(program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if v.fail {
		return nil, fmt.Errorf("fake-password-canary")
	}
	return []program.DatabaseUser{{Name: "app", Roles: []program.DatabaseRole{{Database: "app", Name: "readWrite"}}}}, nil
}
func TestDatabaseRecordingReplayAndRedaction(t *testing.T) {
	target := program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "database"}, Container: "mongo", Database: "app", Auth: &program.DatabaseAuth{Database: "admin", Username: program.CredentialRef{Env: "ADMIN_USER"}, Password: program.CredentialRef{Env: "ADMIN_PASSWORD"}}}
	for _, fail := range []bool{false, true} {
		record := &RecordingReadView{ReadView: catalogRead{ReadView: program.ReadFrom(Snapshot{NamespaceName: "app"}), fail: fail}}
		users, err := record.DatabaseUsers(target)
		if (err != nil) != fail {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(record.RecordedReads())
		if strings.Contains(string(raw), "fake-password-canary") {
			t.Fatal("credential leaked to recording")
		}
		replay := &ReplayReadView{Calls: record.RecordedReads()}
		got, err := replay.DatabaseUsers(target)
		if (err != nil) != fail {
			t.Fatal(err)
		}
		if !fail && (len(got) != len(users) || got[0].Name != "app") {
			t.Fatalf("%+v", got)
		}
		if _, err = replay.DatabaseUsers(target); err == nil {
			t.Fatal("reused consumed observation")
		}
	}
}
