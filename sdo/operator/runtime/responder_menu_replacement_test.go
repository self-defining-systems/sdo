package runtime

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

func TestResponderMenuHermeticReplacementWorkerResumesReportsAndAllowance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	template, cluster := menuThirdRound(t)
	first := NewResponderLoop(template.Request, template.Programs, cluster, nil, template.Executor, 0)
	first.MenuConfig = template.MenuConfig
	store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "control", "synthetic-menu-tools-state")
	first.selectionStore = store
	directory := t.TempDir()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(directory, "worker.py")
	if err := os.WriteFile(script, []byte(menuReplacementPython), 0600); err != nil {
		t.Fatal(err)
	}
	uv, err := exec.LookPath("uv")
	if err != nil {
		t.Fatal(err)
	}
	run := func(phase string) map[string]any {
		t.Helper()
		cmd := exec.CommandContext(ctx, uv, "run", "python", script, phase, first.Request.IncidentID)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "PYTHONPATH="+root, "SDO_TOOLS_DIR="+filepath.Join(directory, "tools"), "CODEX_HOME="+filepath.Join(directory, "codex"), "SDO_SESSION_SCRATCH="+filepath.Join(directory, "scratch"), "SDO_MITIGATION_SESSION=1")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("replacement worker %s: %v\n%s", phase, err, output)
		}
		var result map[string]any
		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatalf("worker %s result: %v %s", phase, err, output)
		}
		return result
	}
	serve := func(loop *ResponderLoop) func() {
		server := &SessionToolServer{Directory: filepath.Join(directory, "tools"), Handle: loop.Handle, PollInterval: time.Millisecond}
		if err := server.Prepare(); err != nil {
			t.Fatal(err)
		}
		serving, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- server.Serve(serving) }()
		return func() {
			stop()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("old controller API calls did not quiesce")
			}
		}
	}
	stopFirst := serve(first)
	original := run("first")
	stopFirst() // Worker exited and its transport drained; no old calls can start.
	select {
	case <-first.mutationOwner.gate:
		first.mutationOwner.gate <- struct{}{}
	default:
		t.Fatal("old mutation owner remains busy")
	}
	oldReport := cloneSelectionReport(first.selection.snapshot().Attempts[0].Report)
	state, _, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replacement := NewResponderLoop(first.Request, first.Programs, cluster, nil, first.Executor, 99)
	replacement.MenuConfig = first.MenuConfig
	if err := replacement.attachSelectionJournal(ctx, first.Request, store, state, true); err != nil {
		t.Fatal(err)
	}
	stopSecond := serve(replacement)
	defer stopSecond()
	resumed := run("second")
	if original["session_id"] != resumed["session_id"] || resumed["status"] != "STATUS_SUCCESS" || resumed["rounds"] != float64(3) || replacement.LoopsUsed() != 4 || replacement.MaxBatches != 5 {
		t.Fatalf("replacement reset session/allowance or failed alternate option: first=%v second=%v", original, resumed)
	}
	ledger := replacement.selection.snapshot()
	if len(ledger.Attempts) != 2 || !proto.Equal(oldReport, ledger.Attempts[0].Report) || !ledger.Attempts[0].DeliveryAcknowledged || ledger.UsedBatches != 4 || ledger.ReservedBatches != 0 {
		t.Fatal("redelivery changed report or spent/reset incident allowance")
	}
}

const menuReplacementPython = `import io, json, os, shlex, sys
from pathlib import Path
from benchmarks.sregym.assurance.scripted_codex.turn import Turn
from benchmarks.sregym.assurance.scripted_codex.wire import EventStream, Rollout
from sdo.agents.responder.session_recovery import open_recovery
phase, incident = sys.argv[1:]
recovery = open_recovery(incident, "codex")
session = recovery.resume_session_id()
home = Path(os.environ["CODEX_HOME"])
if phase == "first":
    assert session is None
    rollout = Rollout.create(home, cwd=str(Path.cwd()))
    recovery.checkpoint(rollout.session_id)
    option, rounds = "restart-only", 1
else:
    assert session and len(recovery.session.pending_reports) == 1
    assert recovery.session.pending_reports[0].status == 4
    assert "STATUS_UNRESOLVED" in recovery.recovered_prompt()
    rollout = Rollout.resume(home, session)
    recovery.acknowledge_reports()
    option, rounds = "routing-then-restart", 3
turn = Turn("responder", 175, Path.cwd(), rollout, EventStream(io.StringIO()))
code, output = turn.run(shlex.join([sys.executable, "-m", "sdo.agents.responder.tools_cli", "mitigate", option, "--rounds", str(rounds)]))
assert code == 0, output
report = json.loads(output.splitlines()[0])["report"]
turn.finish("read immutable reports and continue the original session")
print(json.dumps(dict(session_id=rollout.session_id,status=report["status"],rounds=len(report["rounds"]))))
`
