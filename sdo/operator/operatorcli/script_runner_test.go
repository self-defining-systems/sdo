package operatorcli

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sdo.dev/operator/kubeproxy"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

func TestScriptProcessBoundsAndOpaqueEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "must-not-exist")
	value := "$(touch " + path + ")"
	out := program.ScriptExecutionResult{}
	code := executeScriptProcess(context.Background(), `printf '%s' "$INPUT"; printf '%070000d' 1; printf '%070000d' 1 >&2`, []string{"INPUT=" + value, "PATH=" + os.Getenv("PATH")}, &out)
	if code != 0 || !strings.HasPrefix(out.Stdout, value) || len(out.Stdout) != program.MaxScriptOutputBytes || len(out.Stderr) != program.MaxScriptOutputBytes || !out.StdoutTruncated || !out.StderrTruncated {
		t.Fatalf("bounded output: code=%d stdout=%d stderr=%d truncated=%v/%v", code, len(out.Stdout), len(out.Stderr), out.StdoutTruncated, out.StderrTruncated)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("environment became shell code")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	out = program.ScriptExecutionResult{}
	code = executeScriptProcess(ctx, `sleep 30 & wait`, os.Environ(), &out)
	if code == 0 || !out.TimedOut || time.Since(started) > time.Second {
		t.Fatalf("timeout did not kill process group: %d %+v", code, out)
	}
}

func TestScriptProxyCapturesFirstPreimageAndSettledPostimage(t *testing.T) {
	value := "broken"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "PATCH" {
			value = "fixed"
		}
		fmt.Fprintf(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":"app","name":"settings","uid":"same","resourceVersion":"1"},"data":{"value":%q}}`, value)
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)
	recorder := newScriptRecorder(context.Background(), u, http.DefaultTransport)
	proxy := httptest.NewServer(kubeproxy.New(u, http.DefaultTransport, http.DefaultTransport, recorder))
	defer proxy.Close()
	checkpointed := false
	recorder.checkpoint = func(objects []program.ScriptObjectCapture, _ []program.ScriptAPIReceipt) error {
		if len(objects) > 0 && objects[0].Before != nil {
			checkpointed = true
		}
		return nil
	}
	execution := program.ScriptExecutionResult{}
	runCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	code := executeScriptProcess(runCtx, `for i in 1 2; do curl --silent --show-error --fail --request PATCH --header 'Content-Type: application/merge-patch+json' --data '{"data":{"value":"fixed"}}' "$API/api/v1/namespaces/app/configmaps/settings" || exit; done`, append(os.Environ(), "API="+proxy.URL), &execution)
	proxy.Close()
	if code != 0 || !checkpointed {
		t.Fatalf("script/capture failed: code=%d checkpointed=%v", code, checkpointed)
	}
	result := program.ScriptExecutionResult{}
	recorder.settle(&result)
	if len(result.Objects) != 1 || len(result.Receipts) != 2 || len(result.CaptureGaps) != 0 {
		t.Fatalf("capture: %+v", result)
	}
	before, _, _ := unstructured.NestedString(result.Objects[0].Before.Object, "data", "value")
	after, _, _ := unstructured.NestedString(result.Objects[0].After.Object, "data", "value")
	if before != "broken" || after != "fixed" || !result.Objects[0].Written {
		t.Fatalf("pre/post: %s/%s", before, after)
	}
}
