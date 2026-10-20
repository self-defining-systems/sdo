package program

import "fmt"

// ProgramRegistry is the compiled program catalog available to the controller.
type ProgramRegistry interface {
	List() []ProgramSpec
	Diagnoser(id string) (Diagnoser, bool)
	Mitigator(id string) (Mitigator, bool)
}

type registry struct {
	specs      []ProgramSpec
	diagnosers map[string]Diagnoser
	mitigators map[string]Mitigator
}

func NewRegistry(diagnosers []Diagnoser, mitigators []Mitigator) (ProgramRegistry, error) {
	r := &registry{diagnosers: map[string]Diagnoser{}, mitigators: map[string]Mitigator{}}
	for _, diagnoser := range diagnosers {
		if diagnoser == nil || diagnoser.Spec().ID == "" {
			return nil, fmt.Errorf("program diagnoser needs an ID")
		}
		spec := diagnoser.Spec()
		if _, exists := r.diagnosers[spec.ID]; exists {
			return nil, fmt.Errorf("duplicate program %q", spec.ID)
		}
		r.diagnosers[spec.ID] = diagnoser
		r.specs = append(r.specs, spec)
	}
	for _, mitigator := range mitigators {
		if mitigator == nil {
			return nil, fmt.Errorf("nil program mitigator")
		}
		id := mitigator.Spec().ID
		if _, exists := r.diagnosers[id]; !exists {
			return nil, fmt.Errorf("mitigator %q has no diagnoser", id)
		}
		if _, exists := r.mitigators[id]; exists {
			return nil, fmt.Errorf("duplicate mitigator %q", id)
		}
		r.mitigators[id] = mitigator
	}
	return r, nil
}

func (r *registry) List() []ProgramSpec { return append([]ProgramSpec(nil), r.specs...) }
func (r *registry) Diagnoser(id string) (Diagnoser, bool) {
	value, ok := r.diagnosers[id]
	return value, ok
}
func (r *registry) Mitigator(id string) (Mitigator, bool) {
	value, ok := r.mitigators[id]
	return value, ok
}
