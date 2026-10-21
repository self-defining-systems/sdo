package sdk

import "time"

type DriftClassification string

const (
	DriftFault       DriftClassification = "fault"
	DriftUnexplained DriftClassification = "unexplained"
)

type DriftField struct {
	Path   string `json:"path"`
	Old    string `json:"old,omitempty"`
	New    string `json:"new,omitempty"`
	Source string `json:"source,omitempty"`
}

type DriftEvidence struct {
	Kind        string `json:"kind"`
	Observation string `json:"observation"`
}

type DriftEvent struct {
	Ref                    ObjectRef           `json:"ref"`
	Fields                 []DriftField        `json:"fields"`
	Source                 string              `json:"source"`
	Manager                string              `json:"manager,omitempty"`
	Operation              string              `json:"operation,omitempty"`
	ManagerAt              *time.Time          `json:"manager_at,omitempty"`
	ObservedAt             time.Time           `json:"observed_at"`
	Classification         DriftClassification `json:"classification"`
	ProgramID              string              `json:"program_id,omitempty"`
	InvestigationStatus    string              `json:"investigation_status,omitempty"`
	InvestigationVerdict   string              `json:"investigation_verdict,omitempty"`
	InvestigationReason    string              `json:"investigation_reason,omitempty"`
	InvestigationEvidence  []DriftEvidence     `json:"investigation_evidence,omitempty"`
	InvestigationCaptureID string              `json:"investigation_capture_id,omitempty"`
}
