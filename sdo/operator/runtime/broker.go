package runtime

import (
	"context"
	"errors"
)

// ErrControllerRolloutRestartRequired tells the supervisor to launch the
// accepted detector revision before replaying the durable ACK effect.
var ErrControllerRolloutRestartRequired = errors.New("controller rollout requires restart before closure acknowledgment")

type rolloutPendingBroker interface {
	RolloutPending(ClosureReceipt) (bool, error)
}

type IncidentWorkspace struct {
	IncidentID string
	Worktree   string
	BaseCommit string
}

type ClosureReceipt struct {
	IncidentID       string
	Worktree         string
	BaseCommit       string
	ProposalCommit   string
	OutcomeCommit    string
	ReflectionCommit string
	AckToken         string
}

type IncidentBroker interface {
	PrepareIncident(context.Context, string) (IncidentWorkspace, error)
	ProcessClosure(context.Context, IncidentClosure) (ClosureReceipt, error)
	AcknowledgeClosure(context.Context, ClosureReceipt) error
	// ReleaseIncident reaps the worktree of an incident that was prepared and
	// then superseded or cancelled before any closure, so the only other
	// cleanup path (AcknowledgeClosure) never ran. It must be a no-op for an
	// incident that is closing or already acknowledged, and idempotent.
	ReleaseIncident(context.Context, string) error
}

type workspaceCompletion struct {
	workspace IncidentWorkspace
	err       error
}

type closureCompletion struct {
	receipt ClosureReceipt
	err     error
}

type acknowledgmentCompletion struct {
	err error
}

type releaseCompletion struct {
	incidentID string
	err        error
}
