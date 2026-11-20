"""SREGym on the lane platform: the workload adapter and a thin subject shim (``benchmarks/lanes/docs/contract.md``).

The workload (``workload.py``) maps the lane platform's workload verbs onto fastloop: ``app-up up`` is ``fastloop
up`` (the kind cluster, the SREGym app and SDO with its agent registrations), ``run`` is one ``fastloop run`` of one
SREGym problem (row-start reset to the planned memory commit, no-fault rehearsal, injection, SDO's response, the
problem's oracle, recovery and cleanup), and the run's accepted memory is exported beside the run so a plan edge can
seed the next arm with it. The subject (``subject.py``) is a shim: fastloop installs and drives SDO inside the
workload, so the hooks only report a version and pass the plan's seed through the operator file.

``run.sh workload|subject <verb>`` is the command a lanes inventory names.
"""
