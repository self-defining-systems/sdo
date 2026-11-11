package evidencesink

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

var epoch = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

type fixture struct {
	t        *testing.T
	runtime  string
	ledgers  string
	sinkRoot string
	clock    time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	f := &fixture{t: t, runtime: filepath.Join(root, "pvc", ".sdo-runtime"), ledgers: filepath.Join(root, "pvc", "worktrees", "native-ledger"), sinkRoot: filepath.Join(root, "sink"), clock: epoch}
	for _, directory := range []string{f.runtime, f.ledgers, f.sinkRoot} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f *fixture) write(relative, content string) {
	f.t.Helper()
	name := filepath.Join(f.runtime, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) appendTo(relative, content string) {
	f.t.Helper()
	file, err := os.OpenFile(filepath.Join(f.runtime, filepath.FromSlash(relative)), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := file.WriteString(content); err != nil {
		f.t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		f.t.Fatal(err)
	}
	// Coarse filesystem timestamps must not hide the change: size differs anyway.
}

func (f *fixture) ledger(incidentID string, acknowledged bool) {
	f.t.Helper()
	ledger := &contracts.BrokerLedger{IncidentId: incidentID, Acknowledged: acknowledged}
	if acknowledged {
		ledger.AcknowledgedAt = timestamppb.New(epoch.Add(5 * time.Minute))
		ledger.Closure = &contracts.IncidentClosure{DetectedAt: timestamppb.New(epoch), VerifiedAt: timestamppb.New(epoch.Add(4 * time.Minute))}
		ledger.OutcomeDocument = &contracts.OutcomeRecord{Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{CauseClass: "config-value-wrong"}, {CauseClass: "config-value-wrong"}}}
	}
	data, err := protojson.Marshal(ledger)
	if err != nil {
		f.t.Fatal(err)
	}
	name := filepath.Join(f.ledgers, fmt.Sprintf("%x.json", sha256.Sum256([]byte(incidentID))))
	if err := os.WriteFile(name, data, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) mirror() (*Mirror, *Dir) {
	f.t.Helper()
	dir, err := OpenDir(f.sinkRoot, "run-1", f.clock)
	if err != nil {
		f.t.Fatal(err)
	}
	return &Mirror{RuntimeRoot: f.runtime, LedgerRoot: f.ledgers, Sink: dir, Now: func() time.Time { return f.clock }}, dir
}

func pass(t *testing.T, m *Mirror) {
	t.Helper()
	if err := m.Pass(); err != nil {
		t.Fatal(err)
	}
}

func readSink(t *testing.T, dir *Dir, relative string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir.Root(), filepath.FromSlash(relative)))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func records(t *testing.T, dir *Dir) []Record {
	t.Helper()
	got, err := dir.Records()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func sinkFiles(t *testing.T, dir *Dir) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(dir.Root(), func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(dir.Root(), name)
		files[filepath.ToSlash(relative)] = string(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

const incidentID = "incident-synthetic-1"

func seedRun(f *fixture) string {
	session := "sessions/" + strings.TrimPrefix(IncidentPath(incidentID), "incidents/")
	f.write(session+"/"+IncidentIdentityFile, `{"incident_id":"`+incidentID+`"}`)
	f.write(session+"/responder-usage.jsonl", "{\"turn\":1}\n")
	f.write(session+"/codex/sessions/2026/01/02/rollout-a.jsonl", "{\"type\":\"session_meta\"}\n")
	f.write(session+"/scratch/sdo-reports/r1/0001-act.json", `{"act":true}`)
	f.write(session+"/checkpoints/c1/request.json", `{}`)
	f.write("tasks/designer-0123456789ab/task.json", `{"schema":"sdo.task-evidence/v1","task_id":"designer/1","agent_id":"designer","task_type":"health"}`)
	f.write("tasks/designer-0123456789ab/request.json", `{}`)
	f.write("tasks/designer-0123456789ab/result.json", `{}`)
	f.write("tasks/designer-0123456789ab/codex/sessions/2026/rollout-d.jsonl", "{}\n")
	f.write("usage/controller-turns.jsonl", "{}\n")
	f.write("telemetry/lifecycle-events.jsonl", "{}\n")
	return session
}

func TestSyntheticIncidentWritesLayoutAndIndex(t *testing.T) {
	f := newFixture(t)
	seedRun(f)
	f.ledger(incidentID, true)
	m, dir := f.mirror()
	pass(t, m)

	incident := IncidentPath(incidentID)
	for _, want := range []string{
		LayoutFile, IndexFile,
		incident + "/" + IncidentIdentityFile, incident + "/responder-usage.jsonl", incident + "/" + LedgerFile, incident + "/" + SealFile,
		incident + "/codex/sessions/2026/01/02/rollout-a.jsonl", incident + "/scratch/sdo-reports/r1/0001-act.json", incident + "/checkpoints/c1/request.json",
		"lifecycle/designer-0123456789ab/task.json", "lifecycle/designer-0123456789ab/result.json", "lifecycle/designer-0123456789ab/codex/sessions/2026/rollout-d.jsonl",
		"usage/controller-turns.jsonl", "telemetry/lifecycle-events.jsonl",
	} {
		if _, ok := sinkFiles(t, dir)[want]; !ok {
			t.Errorf("sink lacks %s", want)
		}
	}
	var layout layoutDocument
	if err := json.Unmarshal([]byte(readSink(t, dir, LayoutFile)), &layout); err != nil || layout.Schema != LayoutSchema || layout.RunID != "run-1" {
		t.Fatalf("layout = %+v, %v", layout, err)
	}
	got := records(t, dir)
	if len(got) != 2 {
		t.Fatalf("index records = %+v, want one incident and one task", got)
	}
	byKind := map[string]Record{}
	for _, record := range got {
		byKind[record.Kind] = record
	}
	incidentRec := byKind[KindIncident]
	if incidentRec.ID != incidentID || incidentRec.Path != incident || incidentRec.Outcome != "success" || len(incidentRec.CauseClasses) != 1 || incidentRec.CauseClasses[0] != "config-value-wrong" {
		t.Fatalf("incident record = %+v", incidentRec)
	}
	if incidentRec.DetectedAt == nil || !incidentRec.DetectedAt.Equal(epoch) || incidentRec.AcknowledgedAt == nil || !incidentRec.SealedAt.Equal(epoch) {
		t.Fatalf("incident record times = %+v", incidentRec)
	}
	seal := readSink(t, dir, incident+"/"+SealFile)
	if digest := sha256.Sum256([]byte(seal)); hex.EncodeToString(digest[:]) != incidentRec.SHA256 {
		t.Fatal("index digest does not match seal.json")
	}
	var document sealDocument
	if err := json.Unmarshal([]byte(seal), &document); err != nil || len(document.Files) != incidentRec.Files || incidentRec.Files != 6 {
		t.Fatalf("seal = %+v (%v), record files %d", document, err, incidentRec.Files)
	}
	task := byKind[KindTask]
	if task.ID != "designer/1" || task.AgentID != "designer" || task.TaskType != "health" || task.Status != "succeeded" || task.Path != "lifecycle/designer-0123456789ab" {
		t.Fatalf("task record = %+v", task)
	}
}

func TestCredentialLikeFilesAreNeverWritten(t *testing.T) {
	f := newFixture(t)
	session := seedRun(f)
	for _, secret := range []string{
		"codex/auth.json", "claude/.credentials.json", "tasks/designer-0123456789ab/codex/auth.json",
		"tasks/designer-0123456789ab/claude/.credentials.json", session + "/token.json", session + "/auth.jsonl",
		session + "/codex/sessions/2026/rollout-token.jsonl", session + "/api-history/0001.json",
		"build/go-cache/aa/bb", "toolchains/x/bin", "backups/store.tar", "requests/r.json",
		session + "/scratch/clone/secret.jsonl", "claude/plugins/catalog.jsonl",
	} {
		f.write(secret, "secret-value")
	}
	outside := filepath.Join(t.TempDir(), "outside.jsonl")
	if err := os.WriteFile(outside, []byte("secret-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(f.runtime, filepath.FromSlash(session), "linked.jsonl")); err != nil {
		t.Fatal(err)
	}
	m, dir := f.mirror()
	pass(t, m)
	for name, content := range sinkFiles(t, dir) {
		lower := strings.ToLower(filepath.Base(name))
		if content == "secret-value" || strings.Contains(lower, "auth") || strings.Contains(lower, "credential") || strings.Contains(name, "linked") || (strings.Contains(lower, "token") && lower != "token_usage.json") {
			t.Errorf("sink wrote %s", name)
		}
	}
}

func TestSourceTeardownLeavesSinkIntact(t *testing.T) {
	f := newFixture(t)
	seedRun(f)
	f.ledger(incidentID, true)
	m, dir := f.mirror()
	pass(t, m)
	before := sinkFiles(t, dir)

	// Deleting the control namespace deletes the repository PVC: the runtime root and the ledgers disappear.
	if err := os.RemoveAll(filepath.Dir(f.runtime)); err != nil {
		t.Fatal(err)
	}
	f.clock = f.clock.Add(time.Minute)
	pass(t, m)
	restarted, _ := f.mirror()
	pass(t, restarted)

	after := sinkFiles(t, dir)
	if len(after) != len(before) {
		t.Fatalf("sink changed after teardown: %d files before, %d after", len(before), len(after))
	}
	for name, content := range before {
		if after[name] != content {
			t.Errorf("%s changed after teardown", name)
		}
	}
	if got := records(t, dir); len(got) != 2 {
		t.Fatalf("index has %d records after teardown, want the original 2", len(got))
	}
}

func TestOpenIncidentIsMirroredLiveAndSealedAtClosure(t *testing.T) {
	f := newFixture(t)
	session := seedRun(f)
	f.ledger(incidentID, false)
	m, dir := f.mirror()
	pass(t, m)
	incident := IncidentPath(incidentID)
	rollout := "/codex/sessions/2026/01/02/rollout-a.jsonl"
	if got := readSink(t, dir, incident+rollout); got != "{\"type\":\"session_meta\"}\n" {
		t.Fatalf("live rollout prefix = %q", got)
	}
	for _, record := range records(t, dir) {
		if record.Kind == KindIncident {
			t.Fatalf("open incident was sealed: %+v", record)
		}
	}

	f.appendTo(session+rollout, "{\"type\":\"turn\"}\n")
	f.ledger(incidentID, true)
	f.clock = f.clock.Add(time.Minute)
	pass(t, m)
	if got := readSink(t, dir, incident+rollout); !strings.HasSuffix(got, "{\"type\":\"turn\"}\n") {
		t.Fatalf("rollout after closure = %q", got)
	}
	countIncident := func() int {
		n := 0
		for _, record := range records(t, dir) {
			if record.Kind == KindIncident {
				n++
			}
		}
		return n
	}
	if countIncident() != 1 {
		t.Fatal("closure did not seal the incident once")
	}
	pass(t, m)
	if countIncident() != 1 {
		t.Fatal("an unchanged incident was resealed")
	}

	// Reflection resumes the session after closure: the unit is resealed with a new record.
	f.appendTo(session+rollout, "{\"type\":\"reflection\"}\n")
	f.clock = f.clock.Add(time.Minute)
	pass(t, m)
	if countIncident() != 2 {
		t.Fatal("later evidence did not reseal the incident")
	}
}

func TestRestartDoesNotReseal(t *testing.T) {
	f := newFixture(t)
	seedRun(f)
	f.ledger(incidentID, true)
	m, dir := f.mirror()
	pass(t, m)
	restarted, _ := f.mirror()
	f.clock = f.clock.Add(time.Hour)
	pass(t, restarted)
	if got := records(t, dir); len(got) != 2 {
		t.Fatalf("restart appended records: %+v", got)
	}
}

func TestIndexIgnoresTornLastLine(t *testing.T) {
	f := newFixture(t)
	_, dir := f.mirror()
	if err := dir.Append(Record{Kind: KindTask, ID: "a", Path: "lifecycle/a"}); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(dir.Root(), IndexFile), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"schema":"sdo.evidence-index/v1","kind":"ta`); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if got := records(t, dir); len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("records = %+v", got)
	}
}

func TestPlaceRejectsUnsafePaths(t *testing.T) {
	for _, name := range []string{"../sessions/a/x.jsonl", "/sessions/a/x.jsonl", "sessions/a/../b/x.jsonl", "worktrees/x.jsonl", "sessions/a/api-history/x.json"} {
		if target, ok := Place(name); ok {
			t.Errorf("Place(%q) = %q", name, target)
		}
	}
	if target, ok := Place("setup/designer/log.jsonl"); !ok || target != "controller/setup/designer/log.jsonl" {
		t.Fatalf("Place(setup) = %q, %v", target, ok)
	}
}

func TestOpenDirRejectsInvalidRunID(t *testing.T) {
	for _, id := range []string{"", "../x", "a/b", ".hidden"} {
		if _, err := OpenDir(t.TempDir(), id, epoch); err == nil {
			t.Errorf("OpenDir accepted run ID %q", id)
		}
	}
}
