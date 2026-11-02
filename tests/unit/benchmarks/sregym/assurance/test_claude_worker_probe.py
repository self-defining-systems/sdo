"""The live rehearsal inherits confinement from a real health worker."""

import copy

from benchmarks.sregym.fastloop.claude_worker_probe import probe_job


def test_probe_preserves_worker_security_and_uses_production_image() -> None:
    spec = {
        "securityContext": {"runAsNonRoot": True, "runAsUser": 65532, "seccompProfile": {"type": "Unconfined"}},
        "containers": [
            {
                "name": "agent",
                "image": "scripted:old",
                "command": ["scripted"],
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "readOnlyRootFilesystem": True,
                    "capabilities": {"drop": ["ALL"]},
                },
                "volumeMounts": [{"name": "credentials", "mountPath": "/sdo/credentials", "readOnly": True}],
                "env": [
                    {"name": "SDO_SESSION_DIR", "value": "/shared/session"},
                    {"name": "SDO_OPUS_MAX_SESSIONS", "value": "8"},
                ],
            }
        ],
        "volumes": [{"name": "credentials", "secret": {"secretName": "empty-scripted-secret"}}],
    }
    template = {"spec": {"template": {"spec": spec}}}
    before = copy.deepcopy(template)
    job = probe_job(template, "worker:frozen", "live-secret")
    actual = job["spec"]["template"]["spec"]
    assert actual["securityContext"] == spec["securityContext"]
    assert actual["containers"][0]["securityContext"] == spec["containers"][0]["securityContext"]
    assert actual["containers"][0]["volumeMounts"] == spec["containers"][0]["volumeMounts"]
    assert actual["containers"][0]["image"] == "worker:frozen"
    assert actual["containers"][0]["command"][-1] == "/usr/local/lib/sdo/check_claude_worker.py"
    assert {"name": "SDO_OPUS_MAX_SESSIONS", "value": "8"} in actual["containers"][0]["env"]
    assert all(item["name"] != "SDO_SESSION_DIR" for item in actual["containers"][0]["env"])
    assert actual["volumes"][0]["secret"]["secretName"] == "live-secret"
    assert template == before

    pooled = probe_job(
        template,
        "worker:frozen",
        "live-secret",
        pool_host_path="/opt/sdo-dev/.fastloop/shared-pool",
        pool_capacity=8,
        host_id="test-host",
    )
    pod = pooled["spec"]["template"]["spec"]
    pool = next(volume for volume in pod["volumes"] if volume["name"] == "opus-pool")
    assert pool["hostPath"]["path"] == "/opt/sdo-dev/.fastloop/shared-pool"
    env = {item["name"]: item["value"] for item in pod["containers"][0]["env"]}
    assert env["SDO_OPUS_POOL_DIR"] == "/sdo/opus-pool"
    assert env["SDO_OPUS_POOL_MODE"] == "per_host"
    assert env["SDO_OPUS_MAX_SESSIONS"] == "8"
    assert env["SDO_OPUS_POOL_HOST_ID"] == "test-host"
    assert pod["containers"][0]["securityContext"] == spec["containers"][0]["securityContext"]
