package operatorcli

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeArtifactsNeverRecordCredentialsOrCaches(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	files := []string{
		"sessions/id/codex/auth.json",
		"sessions/id/codex/cache/remote_plugin_catalog/catalog.json",
		"sessions/id/claude/credentials.json",
		"sessions/id/model_io/token.json",
		"codex/sessions/auth.json",
		"claude/projects/credential.jsonl",
		"sessions/id/claude/projects/app/cache/session.jsonl",
		"sessions/id/codex/sessions/2026/10/21/rollout-a.jsonl",
		"sessions/id/model_io/request/response.json",
		"sessions/id/checkpoints/request/request.json",
		"sessions/id/checkpoints/request/result.json",
		"sessions/id/checkpoints/request/file_edits.json",
		"sessions/id/session.jsonl",
		"sessions/id/token_usage.json",
		"claude/projects/project/session-a.jsonl",
		"codex/sessions/2026/10/21/rollout-a.jsonl",
	}
	for _, name := range files {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err != nil {
		t.Fatal(err)
	}
	for _, name := range files[7:] {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("missing allowed evidence %s: %v", name, err)
		}
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		name := strings.ToLower(entry.Name())
		if name != "token_usage.json" && (strings.Contains(name, "auth") || strings.Contains(name, "credential") || strings.Contains(name, "token")) {
			t.Errorf("secret-like file recorded: %s", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if strings.Contains(relative, "/cache/") || strings.Contains(relative, "/plugin/") {
			t.Errorf("cache recorded: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeArtifactsRejectsUnsafeTarEntry(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "../escape", Mode: 0600, Size: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("bad")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err == nil {
		t.Fatal("tar path traversal accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "..", "escape")); !os.IsNotExist(err) {
		t.Fatalf("unsafe file created: %v", err)
	}
}

func TestRuntimeArtifactsExtractsOnlyWhitelistedEvidence(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	content := []byte("one detector fired\n")
	if err := writer.WriteHeader(&tar.Header{Name: "telemetry/detector-firings.jsonl", Mode: 0600, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
	trace := []byte("provider session trace\n")
	if err := writer.WriteHeader(&tar.Header{Name: "sessions/8d746cb1/codex/sessions/rollout-trace.jsonl", Mode: 0600, Size: int64(len(trace))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(trace); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "telemetry", "detector-firings.jsonl"))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("runtime telemetry lost: %q %v", got, err)
	}
	got, err = os.ReadFile(filepath.Join(root, "sessions", "8d746cb1", "codex", "sessions", "rollout-trace.jsonl"))
	if err != nil || !bytes.Equal(got, trace) {
		t.Fatalf("private incident session evidence lost: %q %v", got, err)
	}
}

func TestParseRuntimeArtifactsRequiresOutputDirectory(t *testing.T) {
	cfg, err := ParseRuntimeArtifactsLaunch([]string{"runtime-artifacts", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test", "--output-dir", "/tmp/results"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OutputDir != "/tmp/results" || cfg.Sync.ControlNamespace != "sdo" {
		t.Fatalf("lost export config: %+v", cfg)
	}
	if _, err := ParseRuntimeArtifactsLaunch([]string{"runtime-artifacts", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test", "--output-dir", "relative"}); err == nil {
		t.Fatal("relative output accepted")
	}
}

func TestRuntimeArtifactsCopyAllowedCorpusHardlinksAndContinue(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	original := "sessions/drift/corpus/open@detect.json"
	linked := "sessions/incident/corpus/open@detect.json"
	for _, header := range []*tar.Header{
		{Name: original, Mode: 0600, Size: 2, Typeflag: tar.TypeReg},
		{Name: linked, Linkname: original, Typeflag: tar.TypeLink},
		{Name: "sessions/incident/model_io/request/turn.json", Mode: 0600, Size: 2, Typeflag: tar.TypeReg},
	} {
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Size > 0 {
			if _, err := writer.Write([]byte("{}")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{original, linked, "sessions/incident/model_io/request/turn.json"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || string(data) != "{}" {
			t.Fatalf("missing evidence %s: %s, %v", name, data, err)
		}
	}
}

func TestRuntimeArtifactsRejectHardlinksToCredentials(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "sessions/id/corpus/open.json", Typeflag: tar.TypeLink, Linkname: "sessions/id/codex/auth.json"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := extractRuntimeArtifacts(&archive, t.TempDir()); err == nil {
		t.Fatal("credential hardlink accepted")
	}
}

func TestRuntimeArtifactsExportsSetupUsageSeparatelyFromIncidents(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	content := []byte(`{"provider":"codex","model":"gpt-6-luna","model_requests":2,"usage":{"input_tokens":1000,"output_tokens":100,"total_cost_usd":0.00015}}` + "\n")
	name := "setup/judge-task/setup-usage.jsonl"
	if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, name))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("setup accounting lost: %q %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(root, "sessions")); !os.IsNotExist(err) {
		t.Fatalf("setup appeared in incident usage: %v", err)
	}
}

func TestRuntimeArtifactsExportsOnlyTypedStartupFiles(t *testing.T) {
	for _, base := range []string{"startup-controller.json", "startup-worker.json", "startup-daemon.json", "startup-session.json", "startup-dispatch.json"} {
		if !allowedRuntimeArtifact("sessions/id/" + base) {
			t.Fatalf("startup evidence omitted: %s", base)
		}
	}
	for _, name := range []string{"sessions/id/startup-auth.json", "sessions/id/startup-worker.json.bak", "sessions/id/nested/startup-worker.json", "sessions/id/startup.json"} {
		if allowedRuntimeArtifact(name) {
			t.Fatalf("startup allowlist expanded: %s", name)
		}
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	payload := []byte(`{"worker_ready":"2026-10-25T16:00:00Z","codex_process_started":"2026-10-25T16:00:01Z","first_model_request_sent":"2026-10-25T16:00:02Z","first_model_request_source":"codex.otel.api_request.timestamp-minus-duration"}`)
	if err := writer.WriteHeader(&tar.Header{Name: "sessions/id/startup-worker.json", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "sessions/id/startup-worker.json")); err != nil || !bytes.Contains(data, []byte("first_model_request_sent")) {
		t.Fatal("startup evidence lost", err)
	}
}

func TestRuntimeArtifactArchiveIncludesExactStartupFiles(t *testing.T) {
	root := t.TempDir()
	session := filepath.Join(root, "sessions/id")
	if err := os.MkdirAll(session, 0700); err != nil {
		t.Fatal(err)
	}
	expected := map[string]bool{}
	for _, name := range []string{"startup-controller.json", "startup-worker.json", "startup-daemon.json", "startup-session.json", "startup-dispatch.json"} {
		expected["sessions/id/"+name] = true
		if err := os.WriteFile(filepath.Join(session, name), []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"credentials.json", "startup-config.json", "startup-worker.json.bak"} {
		if err := os.WriteFile(filepath.Join(session, name), []byte("excluded-canary"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	output, err := exec.Command("sh", "-c", runtimeArtifactsArchiveScript, "sh", root).Output()
	if err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(bytes.NewReader(output))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !expected[header.Name] {
			t.Fatalf("archive included unrelated file: %s", header.Name)
		}
		delete(expected, header.Name)
	}
	if len(expected) != 0 {
		t.Fatalf("startup timing omitted by native archive: %v", expected)
	}
}

func TestProtectedControllerStartupFilesAreExported(t *testing.T) {
	for _, name := range []string{"startup-controller.json", "startup-session.json", "startup-dispatch.json"} {
		if !allowedRuntimeArtifact("sessions/id/startup/" + name) {
			t.Fatalf("protected native timing omitted: %s", name)
		}
	}
	if allowedRuntimeArtifact("sessions/id/startup/environment.json") {
		t.Fatal("startup directory exported as arbitrary JSON")
	}
}

func TestRuntimeArtifactsExportOnlyTypedCommandReports(t *testing.T) {
	allowed := "sessions/session/scratch/sdo-reports/incident/0001-mitigate.json"
	for _, name := range []string{allowed, "sessions/session/scratch/sdo-reports/incident/0002-act.json", "sessions/session/scratch/sdo-reports/incident/0003-apply.json"} {
		if !allowedRuntimeArtifact(name) {
			t.Fatalf("missing command report: %s", name)
		}
	}
	for _, name := range []string{"sessions/session/scratch/arbitrary.json", "sessions/session/scratch/sdo-reports/incident/auth.json", "sessions/session/scratch/sdo-reports/incident/0001-source.json", "sessions/session/scratch/sdo-reports/incident/deeper/0001-act.json", "sessions/session/scratch/sdo-reports/incident/.sequence"} {
		if allowedRuntimeArtifact(name) {
			t.Fatalf("untyped scratch export: %s", name)
		}
	}
	body := []byte(`{"actions":[{"command":"mitigate","report":{"status":"complete"}}],"findings":[],"fixed":[],"new":[],"unchanged":[]}`)
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: allowed, Mode: 0600, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(root, allowed))
	if err != nil || !bytes.Equal(actual, body) {
		t.Fatalf("report lost: %s %v", actual, err)
	}
}

func TestRuntimeArtifactsAllowOnlyTypedPerJobStartup(t *testing.T) {
	for _, path := range []string{"sessions/id/startup/attempts/sdo-job/startup-controller.json", "sessions/id/startup/attempts/sdo-job/startup-dispatch.json", "sessions/id/startup-workers/sdo-job/startup-worker.json"} {
		if !allowedRuntimeArtifact(path) {
			t.Fatal("per-Job timing omitted", path)
		}
	}
	for _, path := range []string{"sessions/id/startup/attempts/sdo-job/environment.json", "sessions/id/startup-workers/sdo-job/credentials.json"} {
		if allowedRuntimeArtifact(path) {
			t.Fatal("untyped attempt export", path)
		}
	}
}

// GNU tar pads its output to a 10 KiB record after the end-of-archive
// marker. The export must consume that padding so the exec stream can finish;
// live rows otherwise waited for the 2-minute exec deadline and failed.
func TestRuntimeArtifactsCopyFinishesWhenArchiveHasRecordPadding(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "sessions/id/session.jsonl", Mode: 0600, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	const record = 10240
	archive.Write(make([]byte, record-archive.Len()%record))
	execute := func(_ context.Context, _, _ string, _ []string, stdout, _ io.Writer) error {
		_, err := stdout.Write(archive.Bytes())
		return err
	}
	root := t.TempDir()
	done := make(chan error, 1)
	go func() { done <- copyRuntimeArtifacts(context.Background(), execute, "control", root) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime artifact copy did not finish after the archive ended")
	}
	if _, err := os.Stat(filepath.Join(root, "sessions/id/session.jsonl")); err != nil {
		t.Fatalf("missing exported evidence: %v", err)
	}
}
