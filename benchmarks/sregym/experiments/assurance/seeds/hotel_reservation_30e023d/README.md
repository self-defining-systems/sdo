# Hotel Reservation health seed (lifecycle commit 30e023d)

`operational_memory/` is the `.sdo` tree an SDO lifecycle round produced at commit 30e023d. It is pre-fairness: see the analyze-experiment failure-patterns reference. Later hand edits: gofmt, generic health packages regenerated from the current installer (`memory.PrepareHealth`), and `DependsOn` lists completed with the rate service and the memcached caches the search and profile paths use. `../overlays/links` adds the hotel link probe.

## Overlap with the benchmark grader's probe

The composite grader (`ENTRY_POINTS["Hotel Reservation"]`) requests `/hotels`, `/recommendations?require=rate` and `/recommendations?require=dis`. `hotel-search` (`/hotels`) and `hotel-recommendations` (`/recommendations?require=dis`) overlap with these paths; `user-login-check` (`/user`) does not.

The generic `pod-admission` detector requires successful server-side dry-run Pod creation in the application namespace, even while existing Pods are Ready. API rejection is unhealthy; transport failure is an observation error.
