package sdk

import "time"

type DriftClassification string

const (
	DriftFault       DriftClassification = "fault"
	DriftUnexplained DriftClassification = "unexplained"
)

type DriftField struct {
	Path string `json:"path"`
	Old  string `json:"old,omitempty"`
	New  string `json:"new,omitempty"`
}

type DriftEvent struct {
	Ref            ObjectRef           `json:"ref"`
	Fields         []DriftField        `json:"fields"`
	Source         string              `json:"source"`
	Manager        string              `json:"manager,omitempty"`
	Operation      string              `json:"operation,omitempty"`
	ManagerAt      *time.Time          `json:"manager_at,omitempty"`
	ObservedAt     time.Time           `json:"observed_at"`
	Classification DriftClassification `json:"classification"`
	ProgramID      string              `json:"program_id,omitempty"`
}
