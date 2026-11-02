package runtime

import (
	"context"

	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type productionMenuProgram struct {
	id   string
	port int
}

func (p productionMenuProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "port", ValueKind: program.IntValue, Default: p.port}}}
}

func (p productionMenuProgram) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	if len(view.Services()) != 1 {
		return program.Verdict{Result: program.Unknown}, nil
	}
	if view.Services()[0].Spec.Ports[0].TargetPort.IntVal != 8080 {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (p productionMenuProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: program.FromParam("port"), Reason: "restore synthetic route"}}}, nil
}

type productionMenuHealth struct{}

func (productionMenuHealth) Spec() sdk.DetectorSpec {
	spec := (fastloopDetector{}).Spec()
	spec.ID = "production-menu-health"
	spec.Interval = 20 * time.Millisecond
	spec.Watches = []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}}
	return spec
}

func (productionMenuHealth) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if len(view.Services()) == 1 && view.Services()[0].Spec.Ports[0].TargetPort.IntVal == 8080 {
		return nil, nil
	}
	return []sdk.Finding{{DetectorID: "production-menu-health", RuleID: "routing", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: menuService, Summary: "synthetic route is unhealthy", Evidence: "independent route port check", Playbooks: []string{"base"}, ParameterBindings: map[string]sdk.ObjectRef{"service": menuService}}}, nil
}

const productionMenuScript = `import json,os,shlex,subprocess,sys,uuid
from pathlib import Path
from sdo.agents.responder.session_recovery import open_recovery
incident,directory=sys.argv[1],Path(sys.argv[2])
recovery=open_recovery(incident,"codex")
assert recovery is not None
class Rollout:
 session_id=str(uuid.uuid4())
rollout=Rollout()
transcripts=Path(os.environ["CODEX_HOME"])/"sessions"/"2026"/"01"/"01"
transcripts.mkdir(parents=True)
(transcripts/f"rollout-2026-01-01T00-00-00-{rollout.session_id}.jsonl").write_text(json.dumps(dict(type="session_meta",payload=dict(id=rollout.session_id)))+"\n")
recovery.checkpoint(rollout.session_id)
class Turn:
 def run(self,command,**_):
  done=subprocess.run(["/bin/sh","-c",command],cwd=Path.cwd(),capture_output=True,text=True,check=False)
  return done.returncode,done.stdout+done.stderr
 def finish(self,text):
  pass
turn=Turn()
def select(program):
 code,text=turn.run(shlex.join([sys.executable,"-m","sdo.agents.responder.tools_cli","--json","menu"]),)
 assert code==0,text
 response=json.loads(text.splitlines()[0])
 if response.get("summarized"):
  response=json.loads(Path(response["details_file"]).read_text())
 menu=response["menu"]
 option=next(o["option_id"] for o in menu["options"] if o.get("program_id")==program)
 code,text=turn.run(shlex.join([sys.executable,"-m","sdo.agents.responder.tools_cli","--json","mitigate",option,"--rounds","1"]))
 assert code==0,text
 envelope=json.loads(text.splitlines()[0])
 full=json.loads(Path(envelope["details_file"]).read_text())
 assert len(full["actions"])==1,full
 return full["actions"][0]["report"]
first=select("try-wrong-port")
assert first["status"]=="STATUS_UNRESOLVED",first
second=select("restore-route")
assert second["status"]=="STATUS_SUCCESS",second
assert first["session_id"]==second["session_id"]
assert first["attempt_id"]!=second["attempt_id"]
turn.finish("read failed mitigation report and selected successful alternate in the same session")
(directory/"factory-report.json").write_text(json.dumps(dict(first=first,second=second)))
print(json.dumps(dict(session_id=rollout.session_id)))
`
