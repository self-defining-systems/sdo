package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/worker_registry"
)

// investigatorStopTimeout bounds the Job deletion of one stop.
const investigatorStopTimeout = 30 * time.Second

// startInvestigators arms the incident's investigators once the responder's session is running. Nothing starts
// until the delay passes with no write; the returned stop ends them and deletes their Jobs, and the dispatcher
// calls it when the responder returns.
func (d KubernetesJobDispatcher) startInvestigators(ctx context.Context, request IncidentRequest, task *contracts.AgentTask, loop *ResponderLoop, reportDir string) (stop func()) {
	board := &InvestigationBoard{Persist: func(id, node, report string) error {
		return persistInvestigatorReport(reportDir, id, node, report)
	}}
	loop.Investigations = board
	d.Investigators.begin(ctx, investigatorPlan{
		IncidentID: request.IncidentID,
		HasWrite:   loop.hasKnownWrite,
		Briefs:     loop.investigatorBriefs,
		Launch: func(ctx context.Context, id string, brief investigatorBrief) (string, error) {
			return d.runInvestigator(ctx, task, loop, id, brief)
		},
		Board: board,
	})
	return func() {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), investigatorStopTimeout)
		defer cancel()
		if err := d.stopInvestigators(stopCtx, request.IncidentID); err != nil {
			slog.Warn("investigator_stop", "incident_id", request.IncidentID, "error", err)
		}
	}
}

// investigationsDirName is the responder session's directory of investigator reports; the evidence allowlist
// admits its .txt files, so a grader reads each report beside the incident's other evidence.
const investigationsDirName = "investigations"

// persistInvestigatorReport writes one finished report under the incident's session directory (dir), atomically.
// An empty dir stores nothing.
func persistInvestigatorReport(dir, id, node, report string) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return err
	}
	text := fmt.Sprintf("investigator: %s\nnode: %s\n\n%s\n", id, node, strings.TrimSpace(report))
	temp, err := os.CreateTemp(dir, ".report-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.WriteString(text); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(temp.Name(), 0o644); err != nil {
		return err
	}
	return os.Rename(temp.Name(), filepath.Join(dir, id+".txt"))
}

// stopInvestigators ends the incident's investigators and deletes their Jobs; with none running it does nothing.
func (d KubernetesJobDispatcher) stopInvestigators(ctx context.Context, incidentID string) error {
	return d.Investigators.stop(ctx, incidentID, func(ctx context.Context, id string) error {
		return d.stopExecutions(ctx, id, 0)
	})
}

// investigatorTask derives an investigator's task from the responder's own: a separate identity (so its Job,
// session directory and proxy scope are its own), the Go brief, and the time box as the deadline.
func investigatorTask(base *contracts.AgentTask, id, brief string) (*contracts.AgentTask, error) {
	typed := new(contracts.IncidentRequest)
	if err := base.GetRequest().UnmarshalTo(typed); err != nil {
		return nil, fmt.Errorf("decode responder task: %w", err)
	}
	typed.IncidentId = id
	typed.InvestigatorBrief = brief
	typed.PreparedAgentTask = ""
	typed.ResponseDeadline = timestamppb.New(lifecycleio.Now().Add(investigatorTimeLimit))
	packed, err := anypb.New(typed)
	if err != nil {
		return nil, fmt.Errorf("pack investigator request: %w", err)
	}
	task := proto.Clone(base).(*contracts.AgentTask)
	task.TaskId, task.Request = id, packed
	task.Workspace.ProposalId = id
	return task, nil
}

// runInvestigator runs one investigator Job to its report. It uses the responder's worker machinery with its
// own session directory and a read-only tool server, and never returns an IncidentResult: only the report text.
func (d KubernetesJobDispatcher) runInvestigator(ctx context.Context, base *contracts.AgentTask, loop *ResponderLoop, id string, brief investigatorBrief) (string, error) {
	task, err := investigatorTask(base, id, brief.Text)
	if err != nil {
		return "", err
	}
	if err := d.Registry.ValidateTask(task); err != nil {
		return "", fmt.Errorf("validate investigator task: %w", err)
	}
	payload, err := wire.Canonical(task)
	if err != nil {
		return "", err
	}
	environment := make(map[string]string, len(d.Environment))
	for name, value := range d.Environment {
		environment[name] = value
	}
	if d.DispatchEnvironment != nil {
		for name, value := range d.DispatchEnvironment(ctx) {
			environment[name] = value
		}
	}
	sessionSubPath, err := memory.PrepareIncidentAPIAttempt(d.RepositoryMountPath, d.RepositoryPVCSubPath, id)
	if err != nil {
		return "", fmt.Errorf("prepare investigator session: %w", err)
	}
	base2 := d.RepositoryPVCSubPath
	if base2 == "" {
		base2 = "."
	}
	relative, err := filepath.Rel(base2, sessionSubPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("investigator tool session is outside repository mount")
	}
	server := SessionToolServer{Directory: filepath.Join(d.RepositoryMountPath, relative, "tools"), Handle: loop.HandleInvestigatorRead}
	if err := server.Prepare(); err != nil {
		return "", fmt.Errorf("prepare investigator tools: %w", err)
	}
	serveCtx, stopServing := context.WithCancel(ctx)
	var serving sync.WaitGroup
	serving.Add(1)
	go func() { defer serving.Done(); _ = server.Serve(serveCtx) }()
	defer func() { stopServing(); serving.Wait() }()
	environment["SDO_SESSION_SCRATCH"] = "/sdo/session/scratch"
	environment["SDO_RESPONDER_USAGE_DIR"] = "/sdo/session/usage"
	environment["SDO_TOOLS_DIR"] = "/sdo/session/tools"
	environment["SDO_RESPONDER_SESSION"] = "1"
	executor := worker_registry.KubernetesExecutor{
		APIProxyImage: d.APIProxyImage, Recording: d.Recording, ToolchainImage: d.ToolchainImage,
		Client: d.Client, Namespace: d.Namespace, Registry: d.Registry,
		ServiceAccount: d.ServiceAccount, RepositoryPVC: d.RepositoryPVC,
		RepositoryMountPath: d.RepositoryMountPath, RepositoryPVCSubPath: d.RepositoryPVCSubPath,
		CredentialsSecret: d.CredentialsSecret, Environment: environment, PollInterval: d.PollInterval, SessionPVCSubPath: sessionSubPath,
		CapturedCorpusReadOnly: d.RequireMemorySnapshot,
		TaskPayload:            string(payload),
	}
	result, err := executor.Execute(ctx, task)
	if err != nil {
		return "", err
	}
	typed := new(contracts.IncidentResult)
	if err := result.GetResponse().UnmarshalTo(typed); err != nil {
		return "", fmt.Errorf("decode investigator result: %w", err)
	}
	if typed.GetStatus() != "completed" || strings.TrimSpace(typed.GetInvestigationReport()) == "" {
		return "", fmt.Errorf("investigator ended %q without a report: %s", typed.GetStatus(), typed.GetError())
	}
	return typed.GetInvestigationReport(), nil
}
