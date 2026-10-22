package memory

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// MarkIncidentAPIRecovery is called only with a controller-verified closure.
// It never derives recovery from API writes or responder claims.
func MarkIncidentAPIRecovery(repositoryMount, incidentID string, verifiedAt time.Time) error {
	if verifiedAt.IsZero() {
		return errors.New("API history recovery requires controller verification time")
	}
	relative, err := PrepareIncidentSession(repositoryMount, "", incidentID)
	if err != nil {
		return err
	}
	directory := filepath.Join(repositoryMount, relative, "api-history")
	b, err := json.Marshal(struct {
		VerifiedAt time.Time `json:"verified_at"`
	}{verifiedAt.UTC()})
	if err != nil {
		return err
	}
	tmp := filepath.Join(directory, "recovery.tmp")
	if err = os.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(directory, "recovery.json"))
}
