package runtime

import (
	"context"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func (l *ResponderLoop) openMenuSession(_ context.Context, response *ToolResponse) {
	l.openDirectMenuSession(response)
}
func (l *ResponderLoop) checkpointMenuSession(_ context.Context, checkpoint *contracts.MitigationSessionCheckpoint, response *ToolResponse) {
	l.openDirectMenuSession(response)
	if checkpoint != nil {
		l.menuSession.Provider = checkpoint.Provider
		l.menuSession.ProviderSessionId = checkpoint.ProviderSessionId
		l.menuSession.CheckpointDigest = checkpoint.CheckpointDigest
		response.Session = proto.Clone(l.menuSession).(*contracts.MitigationSession)
	}
}
func cloneSelectionReport(report *contracts.MitigationReport) *contracts.MitigationReport {
	if report == nil {
		return nil
	}
	return proto.Clone(report).(*contracts.MitigationReport)
}
func (l *ResponderLoop) selectionReport(_ context.Context, query *contracts.MitigationReportQuery, response *ToolResponse) {
	if query != nil {
		for _, report := range l.menuReports {
			if report.AttemptId == query.AttemptId {
				response.Report = cloneSelectionReport(report)
				response.MenuExecution = cloneMenuExecution(l.menuExecutions[report.AttemptId])
				return
			}
		}
	}
	response.Error = "unknown mitigation report"
}
