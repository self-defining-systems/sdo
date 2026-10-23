package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
)

// MenuRepairActions projects executor effects into ordinary repair evidence.
func (l *ResponderLoop) MenuRepairActions() []RepairActionReceipt {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]RepairActionReceipt(nil), l.menuRepairs...)
}

func (l *ResponderLoop) updateMenuRollback(applicationID string, result PlanApplicationResult) {
	for attempt, executions := range l.menuExecutions {
		for i := range executions {
			if executions[i].ApplicationID != applicationID {
				continue
			}
			executions[i].RolledBack = result.RolledBack
			executions[i].ChangesRemain = result.Applied && !result.RolledBack
			if result.RollbackError != "" {
				executions[i].Error = boundedToolText(result.RollbackError)
			}
		}
		l.menuExecutions[attempt] = executions
	}
	if !result.RolledBack {
		return
	}
	for i := range l.menuRepairs {
		if strings.HasPrefix(l.menuRepairs[i].ActionID, applicationID+"-a") {
			l.menuRepairs[i].Success = false
		}
	}
	for _, report := range l.menuReports {
		for _, round := range report.Rounds {
			for _, action := range round.Actions {
				if strings.HasPrefix(action.ActionId, applicationID+"-a") && action.State == contracts.MitigationActionReceipt_STATE_APPLIED {
					action.CompensationState = contracts.MitigationActionReceipt_STATE_APPLIED
				}
			}
		}
	}
}
