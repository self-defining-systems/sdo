"""Exercise the standalone onboarding launcher with a stream-JSON CLI stand-in."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from sdo.agents.onboarding import run_session

LAUNCHER = Path(run_session.__file__)
GOAL_PATH = ".sdo/goal.md"


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


@pytest.fixture
def checkout(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@example.invalid")
    (repo / ".sdo").mkdir()
    (repo / GOAL_PATH).write_text("original goal\n")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "bootstrap")
    return repo, git(repo, "rev-parse", "HEAD")


def executable(path: Path, code: str) -> None:
    path.write_text(f"#!{sys.executable}\n{code}")
    path.chmod(0o700)


@pytest.mark.parametrize(
    ("defect", "diagnostic", "accepted"),
    [
        ("goal", ".sdo/goal.md", True),
        ("scope", "ownership.denied", True),
        ("memory", "schema.version", True),
        ("dirty", "uncommitted", True),
        ("provider_error", "", False),
        ("infra", "toolchain unavailable", False),
        ("truncated", "", False),
        ("clean", "", True),
        ("blocked", "", True),
    ],
)
def test_onboarding_checks_completion_inside_existing_conversation(
    tmp_path: Path,
    checkout: tuple[Path, str],
    defect: str,
    diagnostic: str,
    accepted: bool,
) -> None:
    repo, base = checkout
    prompt = tmp_path / "prompt"
    prompt.write_text("Set up monitoring.")
    transcript = tmp_path / "transcript.jsonl"
    feedback = tmp_path / "feedback"
    executable(
        tmp_path / "claude",
        """
import json
import os
import subprocess
import sys
from pathlib import Path

def event(kind, **fields):
    print(json.dumps(dict(type=kind, **fields)), flush=True)

assert json.loads(sys.stdin.readline())['message']['content'][0]['text'] == 'Set up monitoring.'
mode = os.environ['TEST_DEFECT']
if mode == 'truncated':
    sys.exit(0)
if mode == 'provider_error':
    event('result', subtype='error', is_error=True)
    sys.exit(0)
def commit():
    subprocess.run(['git', 'add', '-A'], check=True)
    subprocess.run(['git', 'commit', '-qm', 'agent changes'], check=True)

reply = 'blocked: the app exports no request metrics' if mode == 'blocked' else None
event('system', subtype='task_started', task_id='setup')
if mode == 'scope':
    event('system', subtype='session_state_changed', state='running')
event('result', subtype='success', is_error=False, result=reply)
if mode == 'goal':
    Path('.sdo/goal.md').write_text('changed goal')
elif mode == 'scope':
    Path('outside').touch()
elif mode == 'dirty':
    Path('scratch').touch()
Path('.sdo/detector').write_text('bad' if mode == 'memory' else 'good')
if mode != 'dirty':
    commit()
event('system', subtype='task_notification', task_id='setup')
event('result', subtype='success', is_error=False, result=reply)
if mode == 'scope':
    event('system', subtype='session_state_changed', state='idle')
correction = sys.stdin.readline()
if not correction:
    sys.exit(0)
Path(os.environ['TEST_FEEDBACK']).write_text(correction)
Path('.sdo/goal.md').write_text('original goal\\n')
Path('.sdo/detector').write_text('good')
for path in ('outside', 'scratch'):
    Path(path).unlink(missing_ok=True)
commit()
event('result', subtype='success', is_error=False, result=reply)
assert sys.stdin.read() == ''
""",
    )
    executable(
        tmp_path / "operator",
        """
import os
import subprocess
import sys
from pathlib import Path
assert sys.argv[1:3] == ['memory', 'check']
assert os.environ['SDO_ACTOR'] == 'health_judge'
assert os.environ['SDO_PHASE'] == 'initial'
assert os.environ['SDO_BASE_COMMIT'] == os.environ['TEST_BASE']
root = Path(sys.argv[sys.argv.index('--root') + 1])
assert root != Path(os.environ['TEST_REPO'])
assert not subprocess.check_output(['git', '-C', str(root), 'status', '--porcelain'])
assert (root / '.sdo/goal.md').read_text() == 'original goal\\n'
assert (root / '.sdo/detector').is_file(), 'validated before background work completed'
if os.environ['TEST_DEFECT'] == 'infra':
    print('toolchain unavailable', file=sys.stderr)
    sys.exit(2)
if (root / 'outside').exists():
    print('outside: ownership.denied')
    sys.exit(1)
if (root / '.sdo/detector').read_text() == 'bad':
    print('.sdo/detector: schema.version')
    sys.exit(1)
(root / '.sdo/derived').touch()
""",
    )
    run = subprocess.run(
        [
            sys.executable,
            str(LAUNCHER),
            "--base",
            base,
            "--operator",
            str(tmp_path / "operator"),
            str(prompt),
            str(transcript),
            str(repo),
        ],
        env={
            **os.environ,
            "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
            "TEST_DEFECT": defect,
            "TEST_BASE": base,
            "TEST_FEEDBACK": str(feedback),
            "TEST_REPO": str(repo),
        },
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    if accepted:
        assert run.returncode == 0, run.stderr
        assert f"Onboarding export validated: {git(repo, 'rev-parse', 'HEAD')}" in run.stdout
        if diagnostic:
            assert diagnostic in json.loads(feedback.read_text())["message"]["content"][0]["text"]
        else:
            assert not feedback.exists()
        assert not git(repo, "status", "--porcelain")
        if defect == "blocked":
            assert "Onboarding reply: blocked: the app exports no request metrics" in run.stderr
    else:
        assert run.returncode != 0
        assert "Onboarding export validated:" not in run.stdout
        assert diagnostic in run.stderr
        assert not feedback.exists()
