package worker_registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
)

const standbyRoot = ".sdo-runtime/standby"

// WarmBinding is durable before work is published. A retry rejoins this Job;
// it must never launch a second worker after the claim may have been observed.
type WarmBinding struct {
	JobName    string    `json:"job_name"`
	IncidentID string    `json:"incident_id"`
	Directory  string    `json:"directory"`
	Digest     string    `json:"request_digest"`
	ClaimedAt  time.Time `json:"claimed_at"`
}

type standbyMove struct {
	Original string `json:"original"`
	Staged   string `json:"staged"`
	Backup   string `json:"backup"`
}
type standbyJournal struct {
	Binding WarmBinding   `json:"binding"`
	Moves   []standbyMove `json:"moves"`
}

// StandbyPool owns one waiting one-shot responder per application. The claim
// channel is the same private PVC filesystem transport as responder tools.
// Only the controller mounts its parent; workers see their own read-only inbox.
type StandbyPool struct {
	Executor             KubernetesExecutor
	AgentID, Application string
	ResponseBudget       time.Duration
	mu                   sync.Mutex
	current              *WarmBinding
	notify               chan struct{}
	done                 chan struct{}
}

func NewStandbyPool(e KubernetesExecutor, agentID, application string) *StandbyPool {
	return &StandbyPool{Executor: e, AgentID: agentID, Application: application, notify: make(chan struct{}, 1), done: make(chan struct{})}
}

func (p *StandbyPool) root() string {
	return filepath.Join(p.Executor.RepositoryMountPath, standbyRoot, JobName(p.Application))
}

// Start runs only after controller leadership. Old unclaimed daemons are
// retired; claimed Jobs and their canonical session directories survive.
func (p *StandbyPool) Start(ctx context.Context) error {
	if err := p.recoverUnclaimed(ctx); err != nil {
		return err
	}
	if err := p.Ensure(ctx); err != nil {
		return err
	}
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				p.retireIdle()
				return
			case <-ticker.C:
			case <-p.notify:
			}
			if err := p.Ensure(ctx); err != nil {
				slog.Warn("responder_standby_replenish", "error", err)
			}
		}
	}()
	return nil
}

func (p *StandbyPool) Wait() { <-p.done }

func (p *StandbyPool) Ensure(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.current != nil {
		if err := os.WriteFile(filepath.Join(p.current.Directory, "control/owner-heartbeat"), []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0600); err != nil {
			return err
		}
		job, err := p.Executor.Client.BatchV1().Jobs(p.Executor.Namespace).Get(ctx, p.current.JobName, metav1.GetOptions{})
		if err == nil && !jobFailed(job) && !jobCompleted(job) && standbyLifetimeCovers(job, time.Now(), p.responseBudget()) {
			return nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil {
			p.deleteSlot(ctx, p.current)
		}
		p.current = nil
	}
	registration, err := p.Executor.Registry.Resolve(p.AgentID, "")
	if err != nil {
		taskType, typeErr := p.Executor.Registry.TaskType(p.AgentID, &contracts.IncidentRequest{})
		if typeErr != nil {
			return typeErr
		}
		registration, err = p.Executor.Registry.Resolve(p.AgentID, taskType)
	}
	if err != nil {
		return err
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	name := "sdo-standby-" + hex.EncodeToString(random[:])
	directory := filepath.Join(p.root(), name)
	for _, sub := range []string{"checkout", "session/api-history", "session/corpus", "session/startup", "session/late-findings", "session/tools/requests", "session/tools/responses", "control", "ready"} {
		if err := os.MkdirAll(filepath.Join(directory, sub), 0700); err != nil {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(directory, "session/api-history.generation"), []byte("standby"), 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "control/owner-heartbeat"), []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0600); err != nil {
		return err
	}
	relative, err := filepath.Rel(p.Executor.RepositoryMountPath, directory)
	if err != nil {
		return err
	}
	sub := path.Join(p.Executor.RepositoryPVCSubPath, filepath.ToSlash(relative))
	e := p.Executor
	e.SessionPVCSubPath = path.Join(sub, "session")
	e.standbyMode = true
	e.standbySubPath = sub
	request, _ := anypb.New(&contracts.IncidentRequest{})
	task := &contracts.AgentTask{Request: request, Workspace: &contracts.ProposalWorkspace{CheckoutPath: "/sdo/proposal"}}
	if err := e.ensureJob(ctx, task, registration.GetKubernetesJob(), name, requestSource{PVCSubPath: path.Join(sub, "control")}, name+"-result", path.Join(sub, "checkout")); err != nil {
		return err
	}
	p.current = &WarmBinding{JobName: name, Directory: directory}
	slog.Info("responder_standby_created", "application", p.Application, "job", name)
	return nil
}

// TryBind never waits for readiness. A successful reservation is single-use.
// Directory renames retain the inodes already bind-mounted by the kubelet, so
// the worker gets the canonical broker checkout/session with the same narrow
// mounts and read-only corpus/history/tools boundaries as a cold Job.
func (p *StandbyPool) TryBind(ctx context.Context, e KubernetesExecutor, task *contracts.AgentTask) (KubernetesExecutor, error) {
	if binding, err := e.loadWarmBinding(task); err != nil {
		return e, err
	} else if binding != nil {
		e.WarmBinding = binding
		return e, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.current == nil || e.SessionPVCSubPath == "" || e.Environment["SDO_RESPONSE_CONTINUATION"] != "" || !validWarmPaths(e, task) {
		return e, nil
	}
	releaseStorage, available := memory.TrySessionStorageBinding()
	if !available {
		return e, nil
	}
	defer releaseStorage()
	slot := p.current
	for _, name := range []string{"agent", "history"} {
		info, err := os.Stat(filepath.Join(slot.Directory, "ready", name))
		if err != nil || time.Since(info.ModTime()) > 2*time.Second {
			return e, nil
		}
	}
	job, jobErr := e.Client.BatchV1().Jobs(e.Namespace).Get(ctx, slot.JobName, metav1.GetOptions{})
	budget := p.responseBudget()
	incident := new(contracts.IncidentRequest)
	if task.GetRequest().UnmarshalTo(incident) == nil && incident.GetResponseDeadline() != nil {
		budget = time.Until(incident.GetResponseDeadline().AsTime())
	}
	if task.GetDeadline() != nil {
		budget = time.Until(task.GetDeadline().AsTime())
		if budget < 0 {
			return e, nil
		}
	}
	if jobErr != nil || jobFailed(job) || jobCompleted(job) || !standbyLifetimeCovers(job, time.Now(), budget) {
		return e, nil
	}
	// A prepared cold request/result/Job owns this immutable task already.
	// Rejoin it instead of moving a live worker's checkout/session to standby.
	// This also keeps controller-side refresh limited to never-executed proposals.
	lookupCtx, cancelLookup := context.WithTimeout(ctx, 100*time.Millisecond)
	priorName, lookupErr := e.taskJobName(lookupCtx, task)
	if lookupErr != nil {
		cancelLookup()
		return e, nil
	}
	for _, suffix := range []string{"-request", "-result"} {
		_, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(lookupCtx, priorName+suffix, metav1.GetOptions{})
		if err == nil || !apierrors.IsNotFound(err) {
			cancelLookup()
			return e, nil
		}
	}
	_, priorErr := e.Client.BatchV1().Jobs(e.Namespace).Get(lookupCtx, priorName, metav1.GetOptions{})
	cancelLookup()
	if priorErr == nil || !apierrors.IsNotFound(priorErr) {
		return e, nil
	}
	reservation, err := os.OpenFile(filepath.Join(slot.Directory, "reservation"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return e, nil
	}
	reservation.Close()
	p.current = nil
	defer func() {
		select {
		case p.notify <- struct{}{}:
		default:
		}
	}()
	stopLease := holdStandbyReservation(ctx, slot)
	bound := false
	defer func() {
		if !bound {
			stopLease()
		}
	}()
	request := new(contracts.IncidentRequest)
	if err := task.GetRequest().UnmarshalTo(request); err != nil {
		return e, err
	}
	slot.IncidentID = request.GetIncidentId()
	slot.Digest = taskRequestDigest(task)
	slot.ClaimedAt = time.Now().UTC()
	sessionRelative, err := filepath.Rel(sessionPVCBase(e.RepositoryPVCSubPath), e.SessionPVCSubPath)
	if err != nil {
		return e, err
	}
	moves := []standbyMove{{task.GetWorkspace().GetCheckoutPath(), filepath.Join(slot.Directory, "checkout"), filepath.Join(slot.Directory, "backup-checkout")}, {filepath.Join(e.RepositoryMountPath, sessionRelative), filepath.Join(slot.Directory, "session"), filepath.Join(slot.Directory, "backup-session")}}
	journal := standbyJournal{Binding: *slot, Moves: moves}
	for _, move := range moves {
		if err := copyStandbyTree(ctx, move.Original, move.Staged); err != nil {
			p.deleteSlot(ctx, slot)
			slog.Warn("responder_standby_fallback", "error", err)
			return e, nil
		}
	}
	// The proposal was issued from accepted state. Refresh it at this exact
	// immutable commit before publishing a task; never use standby-era state.
	if output, err := exec.CommandContext(ctx, "git", "-C", moves[0].Staged, "reset", "--hard", task.GetWorkspace().GetBaseCommit()).CombinedOutput(); err != nil {
		p.deleteSlot(ctx, slot)
		slog.Warn("responder_standby_fallback", "error", fmt.Errorf("refresh accepted checkout: %w: %s", err, output))
		return e, nil
	}
	if err := writeStandbyJSON(filepath.Join(slot.Directory, "binding-journal.json"), journal); err != nil {
		return e, err
	}
	for _, move := range moves {
		if err := os.Rename(move.Original, move.Backup); err != nil {
			if rollbackErr := p.rollback(journal); rollbackErr != nil {
				return e, rollbackErr
			}
			p.deleteSlot(ctx, slot)
			return e, nil
		}
		if err := os.Rename(move.Staged, move.Original); err != nil {
			if rollbackErr := p.rollback(journal); rollbackErr != nil {
				return e, rollbackErr
			}
			p.deleteSlot(ctx, slot)
			return e, nil
		}
	}
	if err := writeStandbyJSON(e.warmBindingPath(task), slot); err != nil {
		if rollbackErr := p.rollback(journal); rollbackErr != nil {
			return e, rollbackErr
		}
		p.deleteSlot(ctx, slot)
		return e, nil
	}
	e.WarmBinding = slot
	bound = true
	return e, nil
}

func copyStandbyTree(ctx context.Context, source, destination string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("standby source is not a real directory: %s", source)
	}
	output, err := exec.CommandContext(ctx, "cp", "-a", source+"/.", destination).CombinedOutput()
	if err != nil {
		return fmt.Errorf("populate standby: %w: %s", err, output)
	}
	return nil
}

func writeStandbyJSON(filename string, value any) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(filename), ".write-")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, filename)
}

func (e KubernetesExecutor) warmBindingPath(task *contracts.AgentTask) string {
	return filepath.Join(e.RepositoryMountPath, standbyRoot, "bindings", taskRequestDigest(task)+".json")
}
func (e KubernetesExecutor) loadWarmBinding(task *contracts.AgentTask) (*WarmBinding, error) {
	if !task.GetRequest().MessageIs(&contracts.IncidentRequest{}) || e.RepositoryMountPath == "" {
		return nil, nil
	}
	data, err := os.ReadFile(e.warmBindingPath(task))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var binding WarmBinding
	if err := json.Unmarshal(data, &binding); err != nil {
		return nil, err
	}
	root := filepath.Join(e.RepositoryMountPath, standbyRoot) + string(filepath.Separator)
	if binding.Digest != taskRequestDigest(task) || !strings.HasPrefix(binding.Directory, root) || !strings.HasPrefix(binding.JobName, "sdo-standby-") {
		return nil, errors.New("invalid durable standby binding")
	}
	return &binding, nil
}

func (p *StandbyPool) rollback(j standbyJournal) error {
	for i := len(j.Moves) - 1; i >= 0; i-- {
		move := j.Moves[i]
		if _, err := os.Stat(move.Backup); err == nil {
			if _, err := os.Stat(move.Staged); errors.Is(err, os.ErrNotExist) {
				if err := os.Rename(move.Original, move.Staged); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("restore standby staged directory: %w", err)
				}
			}
			if err := os.Rename(move.Backup, move.Original); err != nil {
				return fmt.Errorf("restore canonical incident directory: %w", err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (p *StandbyPool) deleteSlot(ctx context.Context, b *WarmBinding) {
	policy := metav1.DeletePropagationBackground
	_ = p.Executor.Client.BatchV1().Jobs(p.Executor.Namespace).Delete(ctx, b.JobName, metav1.DeleteOptions{PropagationPolicy: &policy})
}
func (p *StandbyPool) retireIdle() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.current != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		p.deleteSlot(ctx, p.current)
		p.current = nil
	}
}
func (p *StandbyPool) recoverUnclaimed(ctx context.Context) error {
	entries, err := os.ReadDir(p.root())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "sdo-standby-") {
			continue
		}
		directory := filepath.Join(p.root(), entry.Name())
		data, err := os.ReadFile(filepath.Join(directory, "binding-journal.json"))
		if err == nil {
			var j standbyJournal
			if err := json.Unmarshal(data, &j); err != nil {
				return err
			}
			index := filepath.Join(p.Executor.RepositoryMountPath, standbyRoot, "bindings", j.Binding.Digest+".json")
			if _, err := os.Stat(index); err == nil {
				continue
			}
			if err := p.rollback(j); err != nil {
				return err
			}
		}
		p.deleteSlot(ctx, &WarmBinding{JobName: entry.Name(), Directory: directory})
	}
	return nil
}

// standbyClaimUnpublished proves that no daemon could have observed work.
type standbyClaimUnpublished struct{ err error }

func (e *standbyClaimUnpublished) Error() string { return e.err.Error() }
func (e *standbyClaimUnpublished) Unwrap() error { return e.err }

func (e KubernetesExecutor) publishWarmTask(ctx context.Context, task *contracts.AgentTask) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b := e.WarmBinding
	if b == nil {
		return nil
	}
	payload, err := wire.Canonical(task)
	if err != nil {
		return err
	}
	filename := filepath.Join(b.Directory, "control", TaskDataKey)
	// Rejoining a published claim never changes its task or launch environment.
	if existing, err := os.ReadFile(filename); err == nil {
		if string(existing) != string(payload) {
			return errors.New("standby already claimed by another task")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	unpublished := func(err error) error { return &standbyClaimUnpublished{err} }
	// Absence of the immutable inbox is proof the agent has never been released.
	// A dead reservation can safely use the existing cold fallback, including after restart.
	job, inspectErr := e.Client.BatchV1().Jobs(e.Namespace).Get(ctx, b.JobName, metav1.GetOptions{})
	if apierrors.IsNotFound(inspectErr) {
		return unpublished(inspectErr)
	}
	if inspectErr != nil {
		return inspectErr
	}
	budget := 15 * time.Minute
	incident := new(contracts.IncidentRequest)
	if task.GetRequest().UnmarshalTo(incident) == nil && incident.GetResponseDeadline() != nil {
		budget = time.Until(incident.GetResponseDeadline().AsTime())
	}
	if task.GetDeadline() != nil {
		budget = time.Until(task.GetDeadline().AsTime())
	}
	if jobFailed(job) || jobCompleted(job) || job.DeletionTimestamp != nil || !standbyLifetimeCovers(job, time.Now(), budget) {
		return unpublished(errors.New("unpublished standby is no longer alive or lacks response lifetime"))
	}
	for _, name := range []string{"agent", "history"} {
		info, err := os.Stat(filepath.Join(b.Directory, "ready", name))
		if err != nil || time.Since(info.ModTime()) > 2*time.Second {
			return unpublished(errors.New("unpublished standby lost readiness"))
		}
	}
	// Persist execution identity on the existing Job, including requests too
	// large for ConfigMaps. This is metadata only; the immutable inbox remains
	// the sole release point and no worker permission changes.
	incidentIdentity := new(contracts.IncidentRequest)
	if err := task.GetRequest().UnmarshalTo(incidentIdentity); err != nil {
		return unpublished(err)
	}
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations["sdo.dev/incident-id"] = incidentIdentity.GetIncidentId()
	job.Annotations["sdo.dev/task-id"] = task.GetTaskId()
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).Update(ctx, job, metav1.UpdateOptions{}); err != nil {
		return unpublished(err)
	}
	environment := cloneEnvironment(e.Environment)
	environment["SDO_REQUEST_SHA256"] = taskRequestDigest(task)
	environment["SDO_RESULT_CONFIGMAP"] = b.JobName + "-result"
	environment["SDO_NAMESPACE"] = e.Namespace
	environment["SDO_WORKSPACE_ALIAS"] = "/sdo/proposal"
	environment["SDO_STARTUP_TIMING_PATH"] = "/sdo/session/startup-workers/" + b.JobName + "/startup-worker.json"
	if err := e.projectWarmLateFindings(ctx); err != nil {
		return unpublished(err)
	}
	if err := writeStandbyJSON(filepath.Join(b.Directory, "control/environment.json"), environment); err != nil {
		return unpublished(err)
	}
	file, err := os.CreateTemp(filepath.Dir(filename), ".task-")
	if err != nil {
		return unpublished(err)
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return unpublished(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return unpublished(err)
	}
	if err := file.Close(); err != nil {
		return unpublished(err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Link(temporary, filename); err != nil {
		if existing, readErr := os.ReadFile(filename); readErr == nil {
			if string(existing) == string(payload) {
				return nil
			}
			return errors.New("conflicting standby claim was already published")
		}
		return unpublished(fmt.Errorf("publish standby claim: %w", err))
	}
	// Backups are no longer needed once the immutable binding is published.
	for _, name := range []string{"backup-checkout", "backup-session"} {
		_ = os.RemoveAll(filepath.Join(b.Directory, name))
	}
	slog.Info("responder_standby_claimed", "job", b.JobName, "task_id", task.GetTaskId(), "claimed_at", b.ClaimedAt)
	return nil
}

// abandonUnpublished keeps the canonical mounted directories: their inodes
// may already be held by the tool server. Only an unpublished claim may take
// this path; a published/uncertain claim always rejoins its original Job.
func (e KubernetesExecutor) abandonUnpublished(ctx context.Context, task *contracts.AgentTask) error {
	b := e.WarmBinding
	if err := os.Remove(e.warmBindingPath(task)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(filepath.Join(b.Directory, "binding-journal.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, name := range []string{"backup-checkout", "backup-session"} {
		_ = os.RemoveAll(filepath.Join(b.Directory, name))
	}
	policy := metav1.DeletePropagationBackground
	return e.Client.BatchV1().Jobs(e.Namespace).Delete(ctx, b.JobName, metav1.DeleteOptions{PropagationPolicy: &policy})
}

// ExistingBinding identifies a restart/retry that must retain its API-history generation.
func (e KubernetesExecutor) HasBoundStandby(task *contracts.AgentTask) (bool, error) {
	binding, err := e.loadWarmBinding(task)
	return binding != nil, err
}

func validWarmPaths(e KubernetesExecutor, task *contracts.AgentTask) bool {
	checkout := path.Clean(task.GetWorkspace().GetCheckoutPath())
	root := strings.TrimSuffix(path.Clean(e.RepositoryMountPath), "/")
	if !path.IsAbs(checkout) || !path.IsAbs(root) || !strings.HasPrefix(checkout, root+"/") {
		return false
	}
	relative, err := filepath.Rel(sessionPVCBase(e.RepositoryPVCSubPath), e.SessionPVCSubPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, "../") || filepath.IsAbs(relative) {
		return false
	}
	session := filepath.Join(e.RepositoryMountPath, relative)
	return checkout != session && !strings.HasPrefix(checkout, session+"/") && !strings.HasPrefix(session, checkout+"/")
}

const standbyExpiryMargin = time.Minute

func (p *StandbyPool) responseBudget() time.Duration {
	if p.ResponseBudget > 0 {
		return p.ResponseBudget
	}
	return 15 * time.Minute
}
func standbyLifetimeCovers(job *batchv1.Job, now time.Time, budget time.Duration) bool {
	if job.Spec.ActiveDeadlineSeconds == nil {
		return true
	}
	start := job.CreationTimestamp.Time
	if job.Status.StartTime != nil {
		start = job.Status.StartTime.Time
	}
	// An unscheduled new Job has not begun consuming its active deadline.
	if start.IsZero() {
		return true
	}
	return start.Add(time.Duration(*job.Spec.ActiveDeadlineSeconds)*time.Second).Sub(now) >= budget+standbyExpiryMargin
}

// Keep a reservation alive while checkout copying and tool-server publication
// run. The lease ends at immutable task publication or controller cancellation.
func holdStandbyReservation(ctx context.Context, b *WarmBinding) context.CancelFunc {
	leaseCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
			}
			if _, err := os.Stat(filepath.Join(b.Directory, "control", TaskDataKey)); err == nil {
				return
			}
			if err := os.WriteFile(filepath.Join(b.Directory, "control/owner-heartbeat"), []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0600); err != nil {
				return
			}
		}
	}()
	return cancel
}
