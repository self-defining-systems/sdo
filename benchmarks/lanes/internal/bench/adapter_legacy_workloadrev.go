package bench

import "encoding/json"

// The workload revision of a lane or a step is stored as workload_rev. Lane and step files written before the rename
// hold it as fuzzops_rev: reading takes that as the same revision (the migration, done as each file is read and
// rewritten at the next save), and writing keeps the old key too, so a manager rolled back to the previous build still
// finds it. Drop the old key when no build that reads only it can run.

// PreparedFields and StepFields have Prepared's and Step's fields and none of their methods.
type (
	PreparedFields Prepared
	StepFields     Step
)

// MarshalJSON writes the revision under workload_rev and the old key.
func (p Prepared) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		PreparedFields
		Legacy string `json:"fuzzops_rev,omitempty"`
	}{PreparedFields(p), p.WorkloadRev})
}

// UnmarshalJSON reads workload_rev, else the old key.
func (p *Prepared) UnmarshalJSON(b []byte) error {
	aux := struct {
		*PreparedFields
		Legacy string `json:"fuzzops_rev"`
	}{PreparedFields: (*PreparedFields)(p)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if p.WorkloadRev == "" {
		p.WorkloadRev = aux.Legacy
	}
	return nil
}

// MarshalJSON writes the revision under workload_rev and the old key.
func (s Step) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		StepFields
		Legacy string `json:"fuzzops_rev,omitempty"`
	}{StepFields(s), s.WorkloadRev})
}

// UnmarshalJSON reads workload_rev, else the old key.
func (s *Step) UnmarshalJSON(b []byte) error {
	aux := struct {
		*StepFields
		Legacy string `json:"fuzzops_rev"`
	}{StepFields: (*StepFields)(s)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if s.WorkloadRev == "" {
		s.WorkloadRev = aux.Legacy
	}
	return nil
}
