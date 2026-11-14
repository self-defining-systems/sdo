package runtime

import (
	"context"
	"log/slog"

	"sdo.dev/operator/provenance"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

// runsRepositoryCode lists the commands that compile or execute playbook code
// from the responder's checkout; their results name the commit they ran on.
func runsRepositoryCode(command string) bool {
	switch command {
	case "diagnose", "menu", "plan", "apply", "mitigate", "rollback":
		return true
	}
	return false
}

// stampProvenance records on a result the commit (or a snapshot of the dirty
// working tree) and operator build the command ran on. The automatic stage runs
// accepted programs, so it names the accepted commit instead of a checkout.
func (l *ResponderLoop) stampProvenance(ctx context.Context, request ToolRequest, response *ToolResponse) {
	if !runsRepositoryCode(request.Command) {
		return
	}
	record := &provenance.Record{OperatorBuild: provenance.OperatorBuild()}
	switch {
	case request.stage:
		record.Commit = l.Request.SourceCommit
	case l.WorkingProgramRoot != "":
		read := l.RepositoryState
		if read == nil {
			read = lifecycleio.RepositoryState
		}
		state, err := read(ctx, l.WorkingProgramRoot)
		if err != nil {
			slog.WarnContext(ctx, "tool_provenance_unavailable", "command", request.Command, "error", err)
		}
		record.Commit, record.Dirty, record.Snapshot = state.Commit, state.Dirty, state.Snapshot
	default:
		record.Commit = l.Request.SourceCommit
	}
	response.Provenance = record
}
