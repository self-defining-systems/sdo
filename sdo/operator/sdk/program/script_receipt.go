package program

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"time"
)

type ScriptAPIReceipt struct {
	Written     []sdk.ObjectRef `json:"written,omitempty"`
	Subresource string          `json:"subresource,omitempty"`
	Method      string          `json:"method"`
	Path        string          `json:"path"`
	Status      int             `json:"status"`
	StartedAt   time.Time       `json:"started_at"`
	CompletedAt time.Time       `json:"completed_at"`
	Omission    string          `json:"omission,omitempty"`
}

type ScriptObjectCapture struct {
	BeforeAbsent bool                       `json:"before_absent,omitempty"`
	AfterAbsent  bool                       `json:"after_absent,omitempty"`
	ImagesRef    string                     `json:"images_ref,omitempty"`
	Written      bool                       `json:"written,omitempty"`
	Ref          sdk.ObjectRef              `json:"ref"`
	Before       *unstructured.Unstructured `json:"before,omitempty"`
	After        *unstructured.Unstructured `json:"after,omitempty"`
}

// ScriptExecutionResult retains measured evidence, never invented effect replies.
// Kubernetes preimages permit future explicit undo; arbitrary external effects
// are irreversible and must never trigger automatic compensation.
type ScriptExecutionResult struct {
	TargetFindings []string `json:"target_findings,omitempty"`
	// Scope is the selected repair scope, not a claim of observed object writes.
	Scope         []sdk.ObjectRef            `json:"scope,omitempty"`
	ResultRef     string                     `json:"result_ref,omitempty"`
	Inputs        map[string]ValueProvenance `json:"inputs,omitempty"`
	ApplicationID string                     `json:"application_id"`
	JobName       string                     `json:"job_name"`
	JobUID        string                     `json:"job_uid"`
	ScriptDigest  string                     `json:"script_digest"`
	DurationMS    int64                      `json:"duration_ms,omitempty"`
	ExitCode      *int32                     `json:"exit_code,omitempty"`
	TimedOut      bool                       `json:"timed_out,omitempty"`
	Receipts      []ScriptAPIReceipt         `json:"receipts,omitempty"`
	Objects       []ScriptObjectCapture      `json:"objects,omitempty"`
	CaptureGaps   []string                   `json:"capture_gaps,omitempty"`
}

// RecordedRepairReceipt is evidence of a successful repair. ScriptDigest,
// when present, identifies the exact RunScript bytes that ran.
type RecordedRepairReceipt struct {
	ScriptDigest string          `json:"script_digest,omitempty"`
	ID           string          `json:"id"`
	IncidentID   string          `json:"incident_id"`
	Success      bool            `json:"success"`
	StartedAt    time.Time       `json:"started_at"`
	CompletedAt  time.Time       `json:"completed_at"`
	Resources    []sdk.ObjectRef `json:"resources,omitempty"`
}
