package runtime

import (
	"context"
)

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
}

// revisionBoundIncidentBroker is the native workspace boundary. Other embedded
// transports may retain their own workspace preparation implementation.
type revisionBoundIncidentBroker interface {
	PrepareIncidentAt(context.Context, string, string) (IncidentWorkspace, error)
}

type workspaceCompletion struct {
	workspace IncidentWorkspace
	err       error
}

type closureCompletion struct {
	effectID string
	receipt  ClosureReceipt
	err      error
}

type acknowledgmentCompletion struct {
	effectID string
	err      error
}
