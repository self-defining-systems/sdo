package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/evidencesink"
)

func TestPreparedIncidentSessionNamesItsIncident(t *testing.T) {
	root := t.TempDir()
	const incident = "incident/with:unsafe characters"
	for range 2 {
		if _, err := PrepareIncidentSession(root, "", incident); err != nil {
			t.Fatal(err)
		}
	}
	session, err := IncidentSessionSubPath("", incident)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, session, evidencesink.IncidentIdentityFile))
	if err != nil {
		t.Fatal(err)
	}
	var identity struct {
		IncidentID string `json:"incident_id"`
	}
	if err := json.Unmarshal(data, &identity); err != nil || identity.IncidentID != incident {
		t.Fatalf("identity = %s, %v", data, err)
	}
	if !evidencesink.Allowed(filepath.ToSlash(filepath.Join(session[len(".sdo-runtime/"):], evidencesink.IncidentIdentityFile))) {
		t.Fatal("the incident identity file is not allowed evidence")
	}
}
