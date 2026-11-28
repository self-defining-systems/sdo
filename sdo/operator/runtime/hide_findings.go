package runtime

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"sdo.dev/operator/contracts/settings"
)

// detection.hide_findings_from_agent (with detection.pages_only) is a
// benchmark arm that measures what SDO's detector findings are worth to the
// agent. The agent is told the page and nothing SDO's detectors found; it keeps
// its cluster access (kubectl, pod logs, exec) and the application's own
// metrics, logs and traces. Every agent-reachable surface is covered here or
// at its owner:
//
//   - the opening incident: admitPage opens it with no clue (page.go), and the
//     encoded agent task is hiddenFindingsRequest of it (kubernetes_job_dispatcher.go);
//     the task's request ConfigMap holds that same payload;
//   - the agent's sdo commands: hiddenToolResponse below, at ResponderLoop.Handle;
//   - investigators: none start (run.go), since each is briefed on a candidate node;
//   - the Kubernetes proxy every agent call goes through refuses SDO's control
//     namespace and cluster-wide lists of kinds that can hold SDO state
//     (kubeproxy.Config.HiddenNamespaces, from the sidecar's --hidden-namespaces).
//
// With the setting off nothing here runs.

// FindingsHiddenText is every hidden sdo command's answer.
const FindingsHiddenText = "SDO's detector output is hidden in this run. Use kubectl, pod logs and exec, and the application's own metrics, logs and traces (sdo metrics, sdo logs, sdo traces, sdo source)."

// hiddenFindingsCommands are the sdo commands whose answer stays available: the
// application's own telemetry, intended source state and the worker's session
// protocol (not agent commands).
var hiddenFindingsAllowed = []string{"source", "metrics", "logs", "traces", "session-open", "session-checkpoint"}

// hiddenFindingsRequest is the incident as the agent's task carries it: its
// identity, workspace, deadline, page and application context, and nothing a
// detector, the automatic stage or an earlier response found.
func hiddenFindingsRequest(request IncidentRequest) IncidentRequest {
	request.OpeningKeys, request.StandingKeys, request.JoinedKeys = nil, nil, nil
	request.Findings, request.DriftEvents, request.DetectorHistory = nil, nil, nil
	request.SurfacedPlaybooks, request.RelevantOutcomes = nil, nil
	request.PassingTests, request.RecentChanges, request.StateChanges = nil, nil, nil
	request.InitialProgramRuns, request.PlaybookCoverage = nil, nil
	request.PriorResponseProgress, request.PriorStages = nil, nil
	return request
}

// hiddenToolResponse answers one sdo command under hide_findings_from_agent.
// Allowed commands run as usual and lose every structured detector field;
// verify keeps only its healthy bit; detect --wait waits only for Recovered;
// every other command answers FindingsHiddenText.
func (l *ResponderLoop) hiddenToolResponse(ctx context.Context, request ToolRequest) ToolResponse {
	switch {
	case slices.Contains(hiddenFindingsAllowed, request.Command):
		return withoutDetectorFields(l.handle(ctx, request))
	case request.Command == "verify":
		verified := l.handle(ctx, request)
		state := "unhealthy"
		if verified.Healthy {
			state = "healthy"
		}
		response := ToolResponse{Command: request.Command, Healthy: verified.Healthy, VerificationStatus: verified.VerificationStatus,
			Summary: "verification: " + state + " (details are hidden in this run)"}
		if verified.Error != "" {
			response.Error, response.Summary = "verification unavailable", "verification unavailable"
		}
		return response
	case request.Command == "detect" && request.WaitSeconds != 0:
		return l.hiddenWait(ctx, request)
	}
	return ToolResponse{Command: request.Command, Text: FindingsHiddenText, Summary: FindingsHiddenText}
}

// hiddenWait is `sdo detect --wait` with findings hidden: it returns when the
// incident is Recovered or the time passes, and says only which.
func (l *ResponderLoop) hiddenWait(ctx context.Context, request ToolRequest) ToolResponse {
	if request.WaitSeconds < 0 || request.WaitSeconds > detectMaxWaitSeconds {
		return ToolResponse{Command: request.Command, Error: fmt.Sprintf("--wait must be between 1 and %d seconds", detectMaxWaitSeconds)}
	}
	clock := timerClock(l.WaitClock)
	started := clock.Now()
	deadline := started.Add(time.Duration(request.WaitSeconds * float64(time.Second)))
	line := ""
	for {
		if l.detectIncidentRecovered() {
			line = fmt.Sprintf("wait: after %ds, recovered: SDO is ending your incident work", int64(clock.Now().Sub(started).Round(time.Second)/time.Second))
			break
		}
		remaining := deadline.Sub(clock.Now())
		if remaining <= 0 || waitOnClock(ctx, clock, min(detectPollInterval, remaining)) != nil {
			line = fmt.Sprintf("wait: not recovered after %ds", int64(clock.Now().Sub(started).Round(time.Second)/time.Second))
			break
		}
	}
	text := strings.Join([]string{line, FindingsHiddenText}, "\n")
	return ToolResponse{Command: request.Command, Text: text, Summary: line}
}

// withoutDetectorFields clears every structured field that carries a finding,
// a detector state or a verification of the alert set.
func withoutDetectorFields(response ToolResponse) ToolResponse {
	response.Drift, response.BeforeFindings, response.ControllerFindings, response.NewSinceDispatch = nil, nil, nil, nil
	response.Findings, response.DriftEvents, response.DetectorStates, response.ProgramRuns = nil, nil, nil, nil
	response.Progress, response.IncidentVerified, response.PriorStages, response.VerificationBlockers = nil, nil, nil, nil
	response.MitigationHealth, response.Burst, response.Healthy = nil, nil, false
	return response
}

// agentInvestigators is response.investigators, or none under
// hide_findings_from_agent: every investigator is briefed on a detector's
// candidate node.
func agentInvestigators(policy settings.Config) int {
	if policy.Detection.HideFindingsFromAgent {
		return 0
	}
	return policy.Response.Investigators
}
