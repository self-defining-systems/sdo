//go:build integration

package storestate_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

// The mysql restore commands run the client with the baseline statements, in
// one session for a grants replay, and print the restored marker.
func TestMysqlRestoreCommandsRunTheBaselineStatements(t *testing.T) {
	payload := mysqlPayload(t)
	for name, test := range map[string]struct {
		replace []string
		key     storestate.Key
		reply   string
		want    []string
	}{
		"variable":        {[]string{"max_connections\t151", "max_connections\t5"}, storestate.Key{Kind: storestate.KindSetting, Name: "max_connections"}, "0\n", []string{"SET GLOBAL max_connections = 151"}},
		"persisted":       {[]string{"max_connections\t151", "max_connections\t5"}, storestate.Key{Kind: storestate.KindSetting, Name: "max_connections"}, "1\n", []string{"SET PERSIST max_connections = 151"}},
		"string variable": {[]string{"sql_mode\tSTRICT_TRANS_TABLES", "sql_mode\t"}, storestate.Key{Kind: storestate.KindSetting, Name: "sql_mode"}, "0\n", []string{"SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES'"}},
		"write block":     {[]string{"super_read_only\tOFF", "super_read_only\tON"}, storestate.Key{Kind: storestate.KindWriteBlock, Name: "super-read-only"}, "0\n", []string{"SET GLOBAL super_read_only = 'OFF'"}},
		"account lock":    {[]string{"00:00:00\tunlocked global=1", "00:00:00\tlocked global=1"}, storestate.Key{Kind: storestate.KindPrincipal, Name: "app@%"}, "", []string{"ALTER USER 'app'@'%' ACCOUNT UNLOCK"}},
		"grants":          {[]string{"schema=18", "schema=0"}, storestate.Key{Kind: storestate.KindPrincipal, Name: "app@%"}, "", []string{"REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'app'@'%'; GRANT USAGE ON *.* TO `app`@`%`; GRANT ALL PRIVILEGES ON `shop`.* TO `app`@`%`"}},
	} {
		t.Run(name, func(t *testing.T) {
			baseline, changes := mysqlChanges(t, test.replace...)
			cmds, err := storestate.RestoreCommands(storestate.EngineMySQL, storestate.Endpoint{}, changes[test.key], baseline, storestate.RestoreOptions{Payload: payload.Lookup})
			if err != nil || len(cmds) != 1 {
				t.Fatalf("cmds = %q, err = %v", cmds, err)
			}
			out, record := runStub(t, cmds[0], map[string]string{"MYSQL_PASSWORD": "synthetic-pw"}, test.reply, "mysql")
			for _, want := range test.want {
				if !strings.Contains(record, want) {
					t.Errorf("the client never ran %q:\n%s", want, record)
				}
			}
			if !strings.Contains(out, storestate.RestoredMarker) {
				t.Fatalf("output = %q", out)
			}
			if strings.Contains(record, "synthetic-pw") {
				t.Fatal("the password reached the client's arguments")
			}
		})
	}
}
