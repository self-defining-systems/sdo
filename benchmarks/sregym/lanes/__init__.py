"""SREGym on the lane platform: the workload adapter and a thin subject shim (``benchmarks/lanes/docs/contract.md``).

The workload (``workload.py``) maps the lane platform's workload verbs onto fastloop: ``app-up up`` is ``fastloop
up`` (the kind cluster, the SREGym app and SDO with its agent registrations), ``run`` is one ``fastloop run`` of one
SREGym problem (row-start reset to the planned memory commit, no-fault rehearsal, injection, SDO's response, the
problem's oracle, recovery and cleanup), and the run's accepted memory is exported beside the run so a plan edge can
seed the next arm with it. The subject (``subject.py``) is a shim: fastloop installs and drives SDO inside the
workload, so the hooks only report a version and pass the plan's seed through the operator file.

The agent is the spec's ``operator.settings`` (``agent.py``): without a model it is the scripted, model-free agent (the
scripted registrations and the host's placeholder credentials hook); with ``provider``, ``model``, ``effort``,
``auth_file`` and ``config`` (the same keys the fuzzops hooks take) ``run`` drives SDO's own responder and closing turn
(``fastloop run --provider --model --reasoning-effort --config``, ``credentials_hook.sh`` putting ``auth_file`` into the
``sdo-agent-credentials`` Secret) and the subject describes itself as needing credentials. ``run`` returns after the
closing turn (fastloop drains it), so the exported memory carries what the closing turn learned. Each run's grade has
the columns of a per-problem report: ``resolution_path``, ``warm_path``, ``match_reasons``, ``dispatched_at``,
``page_source``, ``page_to_healthy_seconds``, ``responder_usd``, ``reflection_usd``, ``closing_finished``.

``run.sh workload|subject <verb>`` is the command a lanes inventory names.
"""
