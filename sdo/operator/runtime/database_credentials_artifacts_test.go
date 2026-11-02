package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// The fake API provides actual SPDY streams to the production executor. It
// checks the fixed Mongo operations and returns a synthetic catalog transition;
// it does not claim to execute MongoDB or validate live authentication.
func TestDatabaseConfigMapOneCallNeverEmitsCredentialArtifacts(t *testing.T) {
	const sentinel = "DBC_SYNTHETIC_PASSWORD_NEVER_EMIT_907a"
	capturedLogs := captureSlog(t)
	a := databaseAction()
	key := func(assignment string) program.CredentialRef {
		return program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "mongo-geo-script", Key: "k8s-geo-mongo.sh", Assignment: assignment}}
	}
	a.Target.Auth = &program.DatabaseAuth{Database: "admin", Username: key("ROOT_USER"), Password: key("ROOT_PASSWORD")}
	a.Password = key("APP_PASSWORD")
	pod := corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: a.Target.Pod.Name, Namespace: "app", UID: "one", ResourceVersion: "1"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mongo"}}}}
	cm := corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: "mongo-geo-script", Namespace: "app"}, Data: map[string]string{"k8s-geo-mongo.sh": "ROOT_USER='" + sentinel + "_USER'\nROOT_PASSWORD='" + sentinel + "_ROOT'\nAPP_PASSWORD=\"" + sentinel + "_APP\"\n"}}
	var mu sync.Mutex
	repaired := false
	refsRead, mutations := 0, 0
	var urls []string
	var logs bytes.Buffer
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		urls = append(urls, r.URL.String())
		mu.Unlock()
		if strings.Contains(r.URL.Path, "/secrets") {
			t.Error("Secret access attempted")
			http.Error(w, "forbidden", 403)
			return
		}
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(r.URL.Path, "/configmaps/") {
				mu.Lock()
				refsRead++
				mu.Unlock()
				_ = json.NewEncoder(w).Encode(cm)
			} else {
				_ = json.NewEncoder(w).Encode(pod)
			}
			return
		}
		q := r.URL.Query()
		if strings.Contains(r.URL.String(), sentinel) || !strings.Contains(strings.Join(q["command"], " "), `--eval eval(_getEnv("_SDO_DATABASE_SCRIPT"))`) || q.Get("stdin") != "true" || q.Get("stderr") == "true" || q.Get("tty") == "true" {
			t.Error("private exec stream contract violated")
			http.Error(w, "bad stream contract", 400)
			return
		}
		if _, err := httpstream.Handshake(r, w, []string{"v4.channel.k8s.io"}); err != nil {
			t.Error(err)
			return
		}
		type ready struct {
			stream  httpstream.Stream
			replied <-chan struct{}
		}
		pending := make(chan ready, 3)
		conn := spdy.NewResponseUpgrader().UpgradeResponse(w, r, func(stream httpstream.Stream, replied <-chan struct{}) error {
			pending <- ready{stream, replied}
			return nil
		})
		if conn == nil {
			return
		}
		defer conn.Close()
		var input, output, status httpstream.Stream
		count := 2
		if q.Get("stdout") == "true" {
			count++
		}
		for i := 0; i < count; i++ {
			select {
			case next := <-pending:
				<-next.replied
				switch next.stream.Headers().Get(corev1.StreamType) {
				case corev1.StreamTypeStdin:
					input = next.stream
				case corev1.StreamTypeStdout:
					output = next.stream
				case corev1.StreamTypeError:
					status = next.stream
				}
			case <-time.After(3 * time.Second):
				t.Error("stream timeout")
				return
			}
		}
		raw, err := io.ReadAll(input)
		if err != nil || (!strings.Contains(string(raw), sentinel+"_USER") || !strings.Contains(string(raw), sentinel+"_ROOT") || (output == nil && !strings.Contains(string(raw), sentinel+"_APP"))) || !strings.Contains(string(raw), "authDB.auth") {
			t.Error("executor did not privately resolve credential references")
			return
		}
		mu.Lock()
		if output == nil {
			if !strings.Contains(string(raw), "catalog.createUser") || !strings.Contains(string(raw), "catalog.updateUser") {
				t.Error("typed catalog repair absent")
			}
			repaired = true
			mutations++
		}
		users := []program.DatabaseUser{}
		if repaired {
			users = append(users, program.DatabaseUser{Name: a.User, Roles: a.Roles})
		}
		mu.Unlock()
		if output != nil {
			_ = json.NewEncoder(output).Encode(users)
			_ = output.Close()
		}
		_ = json.NewEncoder(status).Encode(metav1.Status{Status: metav1.StatusSuccess})
		_ = status.Close()
		select {
		case <-conn.CloseChan():
		case <-time.After(3 * time.Second):
		}
	}))
	server.Config.ErrorLog = log.New(&logs, "", 0)
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root := t.TempDir()
	snapshot := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{pod}, ConfigMapList: []corev1.ConfigMap{cm}}
	const incident = "synthetic-hotel-configmap"
	for _, phase := range []string{"open@detect", "open@pre-apply"} {
		if err := memory.CaptureIncidentSnapshot(root, incident, phase, snapshot, "app"); err != nil {
			t.Fatal(err)
		}
	}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client, ExecConfig: config}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: incident, Namespace: "app", RepairPolicy: "recorded-actions"}, nil, nil, nil, executor, 5)
	loop.MenuConfig = productionResponderMenu()
	loop.CorpusSessionRoot = root
	loop.saveToolState = func(_ context.Context, payload []byte) error {
		return os.WriteFile(filepath.Join(root, "tool-state.json"), payload, 0600)
	}
	loop.Handle(ctx, ToolRequest{Command: "menu"})
	call := func(target program.DatabaseTarget) ToolResponse {
		t.Helper()
		payload, marshalErr := json.Marshal(map[string]any{"command": "mitigate", "selection": map[string]any{"option_id": "EnsureDatabaseUser", "round_limit": 1}, "parameters": map[string]any{"target": target, "user": a.User, "roles": a.Roles, "password": a.Password}})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		var request ToolRequest
		if err := json.Unmarshal(payload, &request); err != nil {
			t.Fatal(err)
		}
		return loop.Handle(ctx, request)
	}
	wrong := a.Target
	wrong.Container = "wrong-mongo"
	refused := call(wrong)
	refusedJSON, _ := json.Marshal(refused)
	if !strings.Contains(string(refusedJSON), "container wrong-mongo not found; containers: [mongo]") {
		t.Fatalf("wrong target lost actionable safe diagnostic: %s", refusedJSON)
	}
	mu.Lock()
	wrongReads, wrongWrites := refsRead, mutations
	mu.Unlock()
	if wrongReads != 0 || wrongWrites != 0 || len(loop.MenuRepairActions()) != 0 {
		t.Fatal("wrong container reached credentials or writes")
	}
	response := call(a.Target)
	if response.Error != "" || response.Report == nil || response.Report.Status != contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("one-call typed repair failed: %s %+v", response.Error, response.Report)
	}
	mu.Lock()
	didRepair, readCount, writeCount := repaired, refsRead, mutations
	mu.Unlock()
	if !didRepair || readCount == 0 || writeCount != 1 || len(loop.MenuRepairActions()) != 1 {
		t.Fatal("repair or private credential resolution was bypassed")
	}
	if err := memory.CaptureIncidentSnapshot(root, incident, "fixed", snapshot, "app"); err != nil {
		t.Fatal(err)
	}
	result := completedResult(incident)
	result.RepairActions = loop.MenuRepairActions()
	result.PlanApplications = loop.PlanApplications()
	result.ProgramToolCalls = loop.ToolCalls()
	closure := nativeClosure(IncidentWorkspace{IncidentID: incident, Worktree: root, BaseCommit: "synthetic"})
	closure.Request = loop.Request
	closure.Result = &result
	derived, err := (ProtoOutcomeDeriver{}).Derive(ctx, closure, "", "scripted", "none")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := protojson.Marshal(derived.Record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "outcome.json"), outcome, 0600); err != nil {
		t.Fatal(err)
	}
	artifacts := map[string]any{"tool-response": response, "applications": loop.PlanApplications(), "receipts": loop.MenuRepairActions(), "tool-calls": loop.ToolCalls()}
	for name, value := range artifacts {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name+".json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	server.Close()
	mu.Lock()
	urlArtifact := strings.Join(urls, "\n")
	mu.Unlock()
	if err := os.WriteFile(filepath.Join(root, "transport.log"), []byte(logs.String()+capturedLogs.String()+urlArtifact), 0600); err != nil {
		t.Fatal(err)
	}
	encoded := []string{}
	for _, suffix := range []string{"_USER", "_ROOT", "_APP"} {
		encoded = append(encoded, base64.StdEncoding.EncodeToString([]byte(sentinel+suffix)))
	}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		leaked := bytes.Contains(raw, []byte(sentinel))
		for _, value := range encoded {
			leaked = leaked || bytes.Contains(raw, []byte(value))
		}
		if leaked {
			t.Errorf("credential persisted in emitted artifact %s", filepath.Base(path))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
