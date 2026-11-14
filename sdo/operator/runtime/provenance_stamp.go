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
	commit, dirty, snapshot := l.checkoutState(ctx, request.stage, request.Command)
	record := &provenance.Record{OperatorBuild: provenance.OperatorBuild(), Commit: commit, Dirty: dirty, Snapshot: snapshot}
	response.Provenance = record
}

// checkoutState names the code a command is running: the accepted commit for
// the automatic stage (it runs accepted programs), otherwise the responder's
// checkout HEAD, whether the tree differs from it, and a snapshot commit of
// that tree.
func (l *ResponderLoop) checkoutState(ctx context.Context, stage bool, command string) (commit string, dirty bool, snapshot string) {
	if stage || l.WorkingProgramRoot == "" {
		return l.Request.SourceCommit, false, ""
	}
	read := l.RepositoryState
	if read == nil {
		read = lifecycleio.RepositoryState
	}
	state, err := read(ctx, l.WorkingProgramRoot)
	if err != nil {
		slog.WarnContext(ctx, "tool_provenance_unavailable", "command", command, "error", err)
	}
	return state.Commit, state.Dirty, state.Snapshot
}
