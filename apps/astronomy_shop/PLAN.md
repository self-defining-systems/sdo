# AS1 execution plan

1. Pin OpenTelemetry Demo 2.2.0 and SREGym's chart revision 7d7b0747.
2. Render and test the exact astronomy-shop topology, flags, services and images;
   pass repository format/static checks and push the application inputs.
3. Add the workspace/source-build adapter to vic-lsh/SREGym sdo3, with failing-first
   contracts. Preserve chart topology and build application images from source.
   Open a separate harness PR; leave fault capabilities and fastloop to their owners.
4. After PB1 releases lane52.lock, run one fully detached, recorded cold seed on
   luna-pilot-52. Publish real deployment evidence through SDO's broker, reconcile
   the rendered lock, and require zero object gaps and healthy workloads/traffic.
5. Push validated changes, open an SDO PR against vic-lsh/dev/sdo-improve without
   merging, and write AS1_REPORT.md outside the repository with exact heads,
   seed/lock evidence and any blockers. No live LLM calls or credential reads.
