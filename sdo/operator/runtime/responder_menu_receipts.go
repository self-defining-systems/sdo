package runtime

import "strings"

// MenuRepairActions projects executor effects into ordinary repair evidence.
func (l *ResponderLoop) MenuRepairActions() []RepairActionReceipt {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]RepairActionReceipt(nil), l.menuRepairs...)
}

func (l *ResponderLoop) updateMenuRollback(applicationID string, result PlanApplicationResult) {
	if !result.RolledBack {
		return
	}
	for i := range l.menuRepairs {
		if strings.HasPrefix(l.menuRepairs[i].ActionID, applicationID+"-a") {
			l.menuRepairs[i].Success = false
		}
	}
}
