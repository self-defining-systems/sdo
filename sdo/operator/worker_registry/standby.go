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

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/anypb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
)

const (
	standbyRoot = ".sdo-runtime/standby"
	// standbyClaimFile in the pod's inbox marks it taken; the idle heartbeat
	// expiry then yields to the claim grace.
	standbyClaimFile = "claimed"
)

// WarmBinding is durable once the pod holds the incident directories. A retry
// rejoins this Job; it must never launch a second worker after the task may
// have been published.
type WarmBinding struct {
	JobName    string    `json:"job_name"`
	IncidentID string    `json:"incident_id"`
	Directory  string    `json:"directory"`
	Digest     string    `json:"request_digest"`
	ClaimedAt  time.Time `json:"claimed_at"`
}

// StandbyPool owns one waiting one-shot responder per application: an early
// claimant for a task that is otherwise run by a cold Job. The inbox is the
// same private PVC filesystem transport as responder tools; only the controller
// mounts its parent, workers see their own read-only inbox.
type StandbyPool struct {
	Executor             KubernetesExecutor
	AgentID, Application string
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

// Start runs only after controller leadership. Idle daemons from a previous
// controller are retired; a Job that already holds a published task is an
// ordinary responder and survives.
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
		if err == nil && !jobFailed(job) && !jobCompleted(job) {
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

// TryBind never waits for readiness. The claim is one create-only file, so
// exactly one claimant wins. The kubelet bind-mounted the idle pod's directory
// inodes, so the canonical checkout and session are copied into them and the
// two directories are exchanged in one atomic rename each: the pod then holds
// the canonical paths with the same narrow mounts and read-only
// corpus/history/tools boundaries as a cold Job. Every state in between leaves
// a valid canonical directory, so a failure needs no rollback: the cold Job
// mounts whatever the canonical path holds.
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
	if jobErr != nil || jobFailed(job) || jobCompleted(job) {
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
	claim, err := os.OpenFile(filepath.Join(slot.Directory, "control", standbyClaimFile), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return e, nil
	}
	claim.Close()
	p.current = nil
	defer func() {
		select {
		case p.notify <- struct{}{}:
		default:
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
	for _, adopted := range [][2]string{
		{task.GetWorkspace().GetCheckoutPath(), filepath.Join(slot.Directory, "checkout")},
		{filepath.Join(e.RepositoryMountPath, sessionRelative), filepath.Join(slot.Directory, "session")},
	} {
		if err := adoptIntoPod(ctx, adopted[0], adopted[1]); err != nil {
			p.deleteSlot(ctx, slot)
			slog.Warn("responder_standby_fallback", "error", err)
			return e, nil
		}
	}
	if err := writeStandbyJSON(e.warmBindingPath(task), slot); err != nil {
		p.deleteSlot(ctx, slot)
		slog.Warn("responder_standby_fallback", "error", err)
		return e, nil
	}
	e.WarmBinding = slot
	return e, nil
}

// adoptIntoPod copies the canonical directory into the pod-held one and swaps
// them. The pod-held inode is now at the canonical path; the old canonical
// tree, now at the pod's former path, is stale and removed.
func adoptIntoPod(ctx context.Context, canonical, held string) error {
	if info, err := os.Lstat(held); err != nil || !info.IsDir() {
		return fmt.Errorf("standby directory is not a real directory: %s", held)
	}
	if err := copyStandbyTree(ctx, canonical, held); err != nil {
		return err
	}
	if err := unix.Renameat2(unix.AT_FDCWD, canonical, unix.AT_FDCWD, held, unix.RENAME_EXCHANGE); err != nil {
		return fmt.Errorf("exchange %s with the standby directory: %w", canonical, err)
	}
	return os.RemoveAll(held)
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

func (p *StandbyPool) deleteSlot(ctx context.Context, b *WarmBinding) {
	policy := metav1.DeletePropagationBackground
	_ = p.Executor.Client.BatchV1().Jobs(p.Executor.Namespace).Delete(ctx, b.JobName, metav1.DeleteOptions{PropagationPolicy: &policy})
	_ = os.RemoveAll(b.Directory)
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

// recoverUnclaimed retires every standby whose task was never published. A
// published one is an ordinary running responder and is left alone.
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
		if _, err := os.Stat(filepath.Join(directory, "control", TaskDataKey)); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
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
		identity, err := e.loadIncidentExecution(ctx, task)
		if err != nil {
			return err
		}
		if identity != nil {
			if identity.JobName != b.JobName {
				return errors.New("published standby execution identity changed")
			}
			return nil
		}
		uid := ""
		job, err := e.Client.BatchV1().Jobs(e.Namespace).Get(ctx, b.JobName, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil {
			uid = string(job.UID)
		}
		return e.ensureIncidentExecution(ctx, task, b.JobName, uid)
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
	if jobFailed(job) || jobCompleted(job) || job.DeletionTimestamp != nil {
		return unpublished(errors.New("unpublished standby is no longer alive"))
	}
	for _, name := range []string{"agent", "history"} {
		info, err := os.Stat(filepath.Join(b.Directory, "ready", name))
		if err != nil || time.Since(info.ModTime()) > 2*time.Second {
			return unpublished(errors.New("unpublished standby lost readiness"))
		}
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
	if err := e.ensureIncidentExecution(ctx, task, b.JobName, string(job.UID)); err != nil {
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
	slog.Info("responder_standby_claimed", "job", b.JobName, "task_id", task.GetTaskId(), "claimed_at", b.ClaimedAt)
	return nil
}

// abandonUnpublished keeps the canonical directories: their inodes may already
// be held by the tool server. Only an unpublished claim may take this path; a
// published/uncertain claim always rejoins its original Job.
func (e KubernetesExecutor) abandonUnpublished(ctx context.Context, task *contracts.AgentTask) error {
	b := e.WarmBinding
	if err := e.removeUnpublishedIncidentExecution(ctx, task); err != nil {
		return err
	}
	if err := os.Remove(e.warmBindingPath(task)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
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
