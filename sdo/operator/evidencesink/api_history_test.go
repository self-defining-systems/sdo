package evidencesink

import "testing"

// The agent API recording (events and manifest, never blobs or the identity key) is run evidence.
func TestAgentAPIRecordingIsMirroredWithoutBlobsOrKeys(t *testing.T) {
	for _, name := range []string{
		"sessions/abc/api-history/events.jsonl",
		"sessions/abc/api-history/manifest.json",
		"sessions/abc/api-history-reflection/events.jsonl",
		"tasks/key/api-history/events.jsonl",
		"tasks/key/api-history/manifest.json",
	} {
		if !Allowed(name) {
			t.Errorf("%s is run evidence and must be admitted", name)
		}
		if target, ok := Place(name); !ok || target == "" {
			t.Errorf("%s has no sink placement", name)
		}
		if parts := splitDirs(name); !descend(parts) {
			t.Errorf("scan must descend into %s", parts)
		}
	}
	for _, name := range []string{
		"sessions/abc/api-history/identity.key",
		"sessions/abc/api-history/blobs/" + "0123456789abcdef",
		"sessions/abc/api-history/mutation.lock",
		"sessions/abc/api-history/mutation-1.json",
		"sessions/abc/api-history/incident-verified.json",
		"tasks/key/api-history/identity.key",
		"sessions/abc/api-history/events.jsonl.tmp",
	} {
		if Allowed(name) {
			t.Errorf("%s must stay out of the sink", name)
		}
	}
}

func splitDirs(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' {
			return name[:i]
		}
	}
	return name
}
