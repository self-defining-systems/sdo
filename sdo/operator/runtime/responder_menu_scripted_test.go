package runtime

const menuStrictPython = `import json, sys
from pathlib import Path
from benchmarks.sregym.protocol.native_receipt import native_production_receipt
from benchmarks.sregym.protocol.receipt import validate_production_receipt
ledger=json.loads(Path(sys.argv[1]).read_text())
receipt=native_production_receipt(ledger, namespace="app", control_namespace="demo", controller_image="synthetic/controller:hermetic", responder_image="synthetic/responder:scripted", validator_image="synthetic/validator:hermetic", controller_pod_uid="synthetic-controller-uid", responder_job_evidence=[dict(incident_id=ledger["incident_id"], job_name=sys.argv[3], job_uid=sys.argv[4], succeeded=True)])
validate_production_receipt(receipt)
Path(sys.argv[2]).write_text(json.dumps(receipt,indent=2))
print("strict receipt PASS: "+receipt["resolution"])
`
