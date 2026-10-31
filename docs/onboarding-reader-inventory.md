# Onboarding reader inventory

The former architecture assessment is replaced with an inventory rendered by Go
from the accepted source commit. It lists manifest resources, kinds, namespaces,
source paths, selectors, pod labels, images and declared dependency references.
Application semantics remain in source; no model-authored description is persisted.

| Reader | Former input | Decision |
|---|---|---|
| Health judge worker and prompt | DeployerAssessment summary, resources, source_commit, topology_fingerprint; covered_resources echo | Pass Go inventory and source resources; read source for application semantics. Remove all echo fields and deployer session identity. |
| Responder prompt (`codex.py`) | Memory snapshot architecture, inlined with a size cap | Keep bounded inventory text as orientation. Read source/manifests directly for meaning; live evidence still establishes incidents. |
| Responder memory tools / Go catalog | Architecture text in accepted-commit-bound snapshot | Keep the same logical text field, now Go inventory. Goal text may be absent or plain text. |
| Reflection | Accepted memory snapshot and protected architecture | Keep inventory for context; source/architecture/goal remain protected. No deployer metadata is needed. |
| Lifecycle coordinator | ArchitectureRequest/Result, summary coverage and commit/fingerprint echoes | Delete worker dispatch and request/result messages. Go writes inventory as Upkeep after trusted source acceptance. |
| Shared validator | Architecture-only deployer proposal and accepted-source echo validation | Delete these special rules. Ownership keeps architecture Go/Upkeep-only; ordinary structure and snapshot budgets still apply. |
| SREGym adapter/driver | Goal sentence and lifecycle commit receipts | Keep optional generated goal as useful benchmark context. Keep architecture commit receipt; it now identifies the deterministic inventory publication. |
| SREGym recording/scripted workers and hermetic fixtures | Architecture capability and objective_digest | Remove architecture dispatch/capability and digest. Scripted judge writes an ordinary detector and default playbook. |
| Experiment analysis / provenance interpretation | Legacy objective digest, coverage and architecture topology fingerprint | Document current fields and distinguish historical records. No current analysis reader requires removed model echoes. |

The source-deployment plan worker remains necessary for source preparation. It
moves from `agents/deployer` to `agents/source_deployment`; it no longer has an
architecture capability or a read-only architecture model session. The broker's
existing `deployer` actor still identifies trusted source publication, while the
architecture is published by Go using Upkeep.

The health-objective playbook remains the default used by Go's generic health
detectors. Ordinary authored health detectors may reference it. There is no
mandatory health-objective detector. One fresh authoring session uses memory check
as its gate; bounded typed validation corrections and transport retries remain.
