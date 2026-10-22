package runtime

import "time"

// scriptedBaseline returns the diff each step's time selects.
type scriptedBaseline struct {
	changes func(now time.Time) []StateChange
}

func (b *scriptedBaseline) Observe(time.Time, bool) {}

func (b *scriptedBaseline) Changes(now time.Time) *StateChanges {
	return &StateChanges{BaselineAt: time.Unix(0, 0).UTC(), ObservedAt: now.UTC(), Changes: b.changes(now)}
}

var selectorFault = StateChange{Kind: "Service", Name: "frontend", Change: StateChangeModified}
