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
