package apihistory

import "strings"

// Receipt kinds of the agent's writes as derived from the recording. A write that created, or later changed, an
// object the agent made in the incident carries the helper form: the object belongs to no cause, so cause
// binding leaves it out unless it is itself a cause resource.
const (
	// WriteKindPrefix opens every kind derived from the recording ("api-patch", "api-exec", ...).
	WriteKindPrefix = "api-"
	// HelperKindPrefix opens the kind of a write to a helper object ("api-helper-create").
	HelperKindPrefix = "api-helper-"
	// GapKind is the receipt that stands for a lost write record. It is a write at its time and no object.
	GapKind = "api-gap"
	// WriteIDPrefix opens the action ID of every receipt derived from the recording.
	WriteIDPrefix = "sdo-api-"
)

// HelperKind reports a write to an object that belongs to no cause.
func HelperKind(kind string) bool { return strings.HasPrefix(kind, HelperKindPrefix) }

// DerivedWriteKind reports a kind derived from the recording.
func DerivedWriteKind(kind string) bool { return strings.HasPrefix(kind, WriteKindPrefix) }
