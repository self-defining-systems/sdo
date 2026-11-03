package runtime

type fixedObjectChanges struct{ changes []ObjectChange }

func (f *fixedObjectChanges) TakeObjectChanges() []ObjectChange {
	changes := f.changes
	f.changes = nil
	return changes
}
