package program

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"time"
)

type ScriptAPIReceipt struct {
	Subresource string    `json:"subresource,omitempty"`
	Method      string    `json:"method"`
	Path        string    `json:"path"`
	Status      int       `json:"status"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Omission    string    `json:"omission,omitempty"`
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
	Scope           []sdk.ObjectRef            `json:"scope,omitempty"`
	ResultRef       string                     `json:"result_ref,omitempty"`
	StdoutBytes     int                        `json:"stdout_bytes,omitempty"`
	StderrBytes     int                        `json:"stderr_bytes,omitempty"`
	Inputs          map[string]ValueProvenance `json:"inputs,omitempty"`
	ApplicationID   string                     `json:"application_id"`
	JobName         string                     `json:"job_name"`
	JobUID          string                     `json:"job_uid"`
	ScriptDigest    string                     `json:"script_digest"`
	ExitCode        *int32                     `json:"exit_code,omitempty"`
	Stdout          string                     `json:"stdout,omitempty"`
	Stderr          string                     `json:"stderr,omitempty"`
	StdoutTruncated bool                       `json:"stdout_truncated,omitempty"`
	StderrTruncated bool                       `json:"stderr_truncated,omitempty"`
	TimedOut        bool                       `json:"timed_out,omitempty"`
	Receipts        []ScriptAPIReceipt         `json:"receipts,omitempty"`
	Objects         []ScriptObjectCapture      `json:"objects,omitempty"`
	CaptureGaps     []string                   `json:"capture_gaps,omitempty"`
}

// ScriptOriginReceipt is protected historical evidence. A parameterized script
// links to the exact bytes the controller executed.
type ScriptOriginReceipt struct {
	ScriptDigest string          `json:"script_digest,omitempty"`
	ID           string          `json:"id"`
	IncidentID   string          `json:"incident_id"`
	Success      bool            `json:"success"`
	StartedAt    time.Time       `json:"started_at"`
	CompletedAt  time.Time       `json:"completed_at"`
	Resources    []sdk.ObjectRef `json:"resources,omitempty"`
}
