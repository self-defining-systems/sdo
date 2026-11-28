package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/sdk/program"
)

// RunScriptRunner is the fixed Job entrypoint. The child uses the same recording
// proxy as responder kubectl, with first-write preimages and settled postimages.
func RunScriptRunner(ctx context.Context) (runErr error) {
	namespace, indexName := processio.Environment("SDO_SCRIPT_NAMESPACE"), processio.Environment("SDO_SCRIPT_INDEX")
	storage := processio.Environment("SDO_SCRIPT_STORAGE_NAMESPACE")
	if namespace == "" || storage == "" || indexName == "" {
		return errors.New("script application identity is required")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	config.QPS, config.Burst = 50, 100
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	index, err := client.CoreV1().ConfigMaps(storage).Get(ctx, indexName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	job, err := client.BatchV1().Jobs(storage).Get(ctx, indexName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	script := processio.Environment("SDO_SCRIPT_TEXT")
	out := program.ScriptExecutionResult{ApplicationID: index.Data["application_id"], JobName: indexName, JobUID: string(job.UID), ScriptDigest: program.ScriptDigest(script)}
	// Every return before the script's first command is a refusal: say so, or
	// the controller can only report an unknown outcome.
	started := false
	defer func() {
		if runErr != nil && !started {
			runErr = refuseScript(ctx, client, storage, indexName, out.JobUID, runErr)
		}
	}()
	if err := scriptIdentityConflict(index, job, namespace, out); err != nil {
		return err
	}
	_ = json.Unmarshal([]byte(index.Data["inputs"]), &out.Inputs)
	deadline, err := time.Parse(time.RFC3339Nano, index.Data["deadline"])
	if err != nil {
		return err
	}
	remaining := max(0, processio.Until(deadline))
	drainDeadline := deadline.Add(-min(time.Second, remaining/5))
	scriptDeadline := drainDeadline.Add(-min(10*time.Second, remaining/4))
	runCtx, cancel := context.WithDeadline(ctx, scriptDeadline)
	defer cancel()
	drainCtx, stopDrain := context.WithDeadline(context.WithoutCancel(ctx), drainDeadline)
	defer stopDrain()
	reads, writes, err := apiProxyTransports(config)
	if err != nil {
		return err
	}
	upstream, err := url.Parse(config.Host)
	if err != nil {
		return err
	}
	recorder := newScriptRecorder(drainCtx, upstream, reads)
	store := scriptCaptureStore{maps: client.CoreV1().ConfigMaps(storage), namespace: storage, application: indexName, jobUID: out.JobUID, owner: metav1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID}}
	recorder.persistImage = store.save
	recorder.persistReceipt = store.receipt
	identity := out
	recorder.checkpoint = func(objects []program.ScriptObjectCapture, receipts []program.ScriptAPIReceipt) error {
		capture := identity
		capture.Objects = objects
		capture.Receipts = receipts
		data, err := json.Marshal(capture)
		if err != nil {
			return err
		}
		patch, _ := json.Marshal(map[string]any{"data": map[string]string{"capture.json": string(data)}})
		writeCtx, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		_, err = client.CoreV1().ConfigMaps(storage).Patch(writeCtx, indexName, types.MergePatchType, patch, metav1.PatchOptions{})
		return err
	}
	gate := &apihistory.MutationGate{Directory: "/sdo/history", ScriptJob: indexName}
	handler := scriptProxy(upstream, reads, writes, recorder, gate)
	server, err := processio.StartLocalServer(drainScriptRequests(runCtx, drainCtx, handler))
	if err != nil {
		return err
	}
	defer server.Close()
	listenerAddress := server.Addr()
	dir, err := os.MkdirTemp("", "sdo-script-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := os.Chmod(dir, 0755); err != nil {
		return err
	}
	kubeconfig := filepath.Join(dir, "kubeconfig")
	body := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: recorded\n  cluster:\n    server: http://%s\ncontexts:\n- name: recorded\n  context:\n    cluster: recorded\n    namespace: %s\n    user: recorded\ncurrent-context: recorded\nusers:\n- name: recorded\n  user: {}\n", listenerAddress, namespace)
	if err = os.WriteFile(kubeconfig, []byte(body), 0644); err != nil {
		return err
	}
	environment := processio.Environ()
	env := make([]string, 0, len(environment)+2)
	for _, value := range environment {
		if strings.HasPrefix(value, "SDO_SCRIPT_") || strings.HasPrefix(value, "KUBECONFIG=") {
			continue
		}
		env = append(env, value)
	}
	env = append(env, "KUBECONFIG="+kubeconfig, "SDO_KUBE_API=http://"+listenerAddress)
	started = true
	code := processio.ExecuteScript(runCtx, script, env, &out)
	cancel()
	out.ExitCode = &code
	// The script deadline bounds writes. A separate short budget settles already
	// touched objects and publishes evidence; it cannot execute script commands.
	settleCtx, stop := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer stop()
	if err := server.Shutdown(drainCtx); err != nil {
		_ = server.Close()
		recorder.Gap()
	}
	recorder.mu.Lock()
	recorder.ctx = settleCtx
	recorder.mu.Unlock()
	recorder.settle(&out)
	out.CaptureGaps = append(out.CaptureGaps, "database, filesystem and network effects outside the Kubernetes proxy are opaque; explicit undo only")
	raw, err := json.Marshal(out)
	if err != nil {
		return err
	}
	if len(raw) > 900<<10 && len(out.Objects) > 0 {
		out.CaptureGaps = append(out.CaptureGaps, "inline object inventory clipped; complete images remain in ConfigMaps labelled sdo.dev/script-capture="+indexName)
		for len(raw) > 900<<10 && len(out.Objects) > 0 {
			out.Objects = out.Objects[:len(out.Objects)/2]
			raw, err = json.Marshal(out)
			if err != nil {
				return err
			}
		}
	}
	if len(raw) > 900<<10 {
		return errors.New("script capture exceeds result storage budget")
	}
	for attempt := 0; attempt < 5; attempt++ {
		index, err = client.CoreV1().ConfigMaps(storage).Get(settleCtx, indexName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if prior := index.Data["result.json"]; prior != "" {
			if prior == string(raw) {
				return nil
			}
			return errors.New("script result already published")
		}
		index.OwnerReferences = []metav1.OwnerReference{store.owner}
		index.Data["job_uid"] = out.JobUID
		index.Data["phase"] = "completed"
		index.Data["result.json"] = string(raw)
		_, err = client.CoreV1().ConfigMaps(storage).Update(settleCtx, index, metav1.UpdateOptions{})
		if err == nil {
			return nil
		}
	}
	return err
}

// scriptIdentityConflict names every identity fact of this Job that differs
// from its reservation. Digests and names only, never script text.
func scriptIdentityConflict(index *corev1.ConfigMap, job *batchv1.Job, namespace string, out program.ScriptExecutionResult) error {
	var conflicts []error
	differ := func(clause, got, want string) {
		conflicts = append(conflicts, fmt.Errorf("%s: this Job has %q, the reservation %q", clause, got, want))
	}
	if out.JobUID == "" {
		conflicts = append(conflicts, errors.New("Job UID unavailable"))
	}
	if want := index.Data["namespace"]; want != namespace {
		differ("namespace", namespace, want)
	}
	if want := index.Data["script_digest"]; want != out.ScriptDigest {
		differ("script digest", out.ScriptDigest, want)
	}
	if got := job.Annotations["sdo.dev/script-application"]; got != out.ApplicationID {
		differ("application", got, out.ApplicationID)
	}
	if want := index.Data["job_uid"]; want != "" && want != out.JobUID {
		differ("Job UID", out.JobUID, want)
	}
	if len(conflicts) == 0 {
		return nil
	}
	return fmt.Errorf("script runner identity conflict: %w", errors.Join(conflicts...))
}

// refuseScript records that the script did not run.
func refuseScript(ctx context.Context, client kubernetes.Interface, storage, indexName, jobUID string, reason error) error {
	raw, _ := json.Marshal(program.ScriptRefusal{JobUID: jobUID, Reason: reason.Error()})
	patch, _ := json.Marshal(map[string]any{"data": map[string]string{program.ScriptRefusalKey: string(raw)}})
	write, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	if _, err := client.CoreV1().ConfigMaps(storage).Patch(write, indexName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		reason = errors.Join(reason, fmt.Errorf("record refusal: %w", err))
	}
	return fmt.Errorf("script refused, not run: %w", reason)
}

// The runner and responder share the same mutation policy.
func scriptProxy(upstream *url.URL, reads, writes http.RoundTripper, recorder kubeproxy.Recorder, gate *apihistory.MutationGate) http.Handler {
	return kubeproxy.NewRecording(upstream, reads, writes, (proxyScope{Kind: "incident"}).config(recorder, gate))
}

// drainScriptRequests lets an admitted write finish after the script stops, so
// its receipt is recorded; it runs on drainCtx, never the client's context. A
// read writes nothing, so it also ends when its client goes away: a finished
// `kubectl rollout status` leaves no watch that holds the runner's shutdown,
// and the controller waiting on the result, until the drain deadline.
func drainScriptRequests(runCtx, drainCtx context.Context, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if runCtx.Err() != nil {
			http.Error(w, "script stopped", http.StatusServiceUnavailable)
			return
		}
		ctx := drainCtx
		if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
			read, cancel := context.WithCancel(drainCtx)
			defer cancel()
			defer context.AfterFunc(r.Context(), cancel)()
			ctx = read
		}
		handler.ServeHTTP(w, r.WithContext(ctx))
	})
}
