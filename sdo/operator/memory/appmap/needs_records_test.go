package appmap

import "testing"

func TestNeedsExistingRecordsFollowsDataAndEscape(t *testing.T) {
	data := Map{Components: []Component{{Name: "db", HoldsData: true}}}
	if !data.NeedsExistingRecords() {
		t.Fatal("a map with a data component needs existing records")
	}
	data.NoUserRecords = "users keep none"
	if data.NeedsExistingRecords() {
		t.Fatal("no_user_records is the escape")
	}
	if (Map{Components: []Component{{Name: "web"}}}).NeedsExistingRecords() {
		t.Fatal("a map without data needs none")
	}
}
