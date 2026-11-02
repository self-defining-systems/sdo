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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/kubeproxy"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/sdk/program"
)

// RunScriptRunner is the fixed Job entrypoint. The child uses the same recording
// proxy as responder kubectl, with first-write preimages and settled postimages.
func RunScriptRunner(ctx context.Context) error {
	namespace, indexName := processio.Environment("SDO_SCRIPT_NAMESPACE"), processio.Environment("SDO_SCRIPT_INDEX")
	if namespace == "" || indexName == "" {
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
	index, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, indexName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	job, err := client.BatchV1().Jobs(namespace).Get(ctx, indexName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	script := processio.Environment("SDO_SCRIPT_TEXT")
	out := program.ScriptExecutionResult{ApplicationID: index.Data["application_id"], JobName: indexName, JobUID: string(job.UID), ScriptDigest: program.ScriptDigest(script)}
	if out.JobUID == "" || index.Data["script_digest"] != out.ScriptDigest || job.Annotations["sdo.dev/script-application"] != out.ApplicationID || index.Data["job_uid"] != "" && index.Data["job_uid"] != out.JobUID {
		return errors.New("script runner identity conflict")
	}
	_ = json.Unmarshal([]byte(index.Data["inputs"]), &out.Inputs)
	deadline, err := time.Parse(time.RFC3339Nano, index.Data["deadline"])
	if err != nil {
		return err
	}
	scriptDeadline := deadline
	if processio.Until(deadline) > 2*time.Second {
		scriptDeadline = deadline.Add(-2 * time.Second)
	}
	runCtx, cancel := context.WithDeadline(ctx, scriptDeadline)
	defer cancel()
	reads, writes, err := apiProxyTransports(config)
	if err != nil {
		return err
	}
	upstream, err := url.Parse(config.Host)
	if err != nil {
		return err
	}
	recorder := newScriptRecorder(runCtx, upstream, reads)
	store := scriptCaptureStore{maps: client.CoreV1().ConfigMaps(namespace), namespace: namespace, application: indexName, jobUID: out.JobUID}
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
		_, err = client.CoreV1().ConfigMaps(namespace).Patch(writeCtx, indexName, types.MergePatchType, patch, metav1.PatchOptions{})
		return err
	}
	handler := kubeproxy.New(upstream, reads, writes, recorder)
	server, err := processio.StartLocalServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCtx, stop := context.WithCancel(r.Context())
		defer stop()
		cancelOnDeadline := context.AfterFunc(runCtx, stop)
		defer cancelOnDeadline()
		handler.ServeHTTP(w, r.WithContext(requestCtx))
	}))
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
	kubeconfig := filepath.Join(dir, "kubeconfig")
	body := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: recorded\n  cluster:\n    server: http://%s\ncontexts:\n- name: recorded\n  context:\n    cluster: recorded\n    namespace: %s\n    user: recorded\ncurrent-context: recorded\nusers:\n- name: recorded\n  user: {}\n", listenerAddress, namespace)
	if err = os.WriteFile(kubeconfig, []byte(body), 0600); err != nil {
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
	code := executeScriptProcess(runCtx, script, env, &out)
	out.ExitCode = &code
	// The script deadline bounds writes. A separate short budget settles already
	// touched objects and publishes evidence; it cannot execute script commands.
	settleCtx, stop := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer stop()
	if err := server.Shutdown(settleCtx); err != nil {
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
	for len(raw) > 900<<10 && (len(out.Stdout) > 0 || len(out.Stderr) > 0) {
		out.Stdout = out.Stdout[:len(out.Stdout)/2]
		out.Stderr = out.Stderr[:len(out.Stderr)/2]
		out.StdoutTruncated = true
		out.StderrTruncated = true
		raw, err = json.Marshal(out)
		if err != nil {
			return err
		}
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
		index, err = client.CoreV1().ConfigMaps(namespace).Get(settleCtx, indexName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if prior := index.Data["result.json"]; prior != "" {
			if prior == string(raw) {
				return nil
			}
			return errors.New("script result already published")
		}
		index.Data["job_uid"] = out.JobUID
		index.Data["phase"] = "completed"
		index.Data["result.json"] = string(raw)
		_, err = client.CoreV1().ConfigMaps(namespace).Update(settleCtx, index, metav1.UpdateOptions{})
		if err == nil {
			return nil
		}
	}
	return err
}

func executeScriptProcess(ctx context.Context, script string, env []string, out *program.ScriptExecutionResult) int32 {
	return processio.ExecuteScript(ctx, script, env, out)
}
