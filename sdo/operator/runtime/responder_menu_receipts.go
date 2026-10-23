package runtime

// MenuRepairActions projects executor effects into ordinary repair evidence.
func (l *ResponderLoop) MenuRepairActions() []RepairActionReceipt {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]RepairActionReceipt(nil), l.menuRepairs...)
}
