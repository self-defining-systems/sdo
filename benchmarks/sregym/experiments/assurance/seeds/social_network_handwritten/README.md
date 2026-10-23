# Social Network health seed (hand-authored)

`operational_memory/` is a complete `.sdo` tree for the DeathStarBench socialNetwork Helm chart that SREGym deploys (namespace `social-network`, entry `nginx-thrift:8080`). It was written by hand from the application's chart, nginx configuration, Lua handlers, Thrift configuration, C++ sources and its own wrk2 scripts, not by an SDO lifecycle round, and contains no fault, verdict or injection knowledge.

- `health-objective`: required Deployments, Services, ConfigMaps (all 27 chart components), scale-to-zero, unavailable replicas, selector mismatch, missing ready endpoints, and NetworkPolicies that deny all ingress or egress to the application pods.
- Traffic (`health`, `verify`): `landing-page` (`GET /`), `home-timeline-read` and `user-timeline-read` (`GET /wrk2-api/{home,user}-timeline/read`, random `user_id` in 0..961, `start=0&stop=10`). All reads. Compose, login, register and follow are writes or need a registered user, so their services are covered only by the objective detector and the link probe.
- `links`: 19 Thrift edges (port 9090) from `service-config.json` and the Lua handlers; datastores excluded.
- `service-endpoints`, `traffic-health`, `traffic-links` are the installer's generic packages (`memory.PrepareHealth`), checked in byte for byte. Regenerate after an installer change with `SDO_UPDATE_ASSURANCE_SEEDS=1 go test -count=1 ./memory/validate/ -run TestAssuranceSeedsPassHealthJudgeCheck` from `sdo/operator`.

## Overlap with the benchmark grader's probe

The composite grader (`sregym/conductor/problems/composition/grading.py`, `ENTRY_POINTS["Social Network"]`) requests `/`, `/wrk2-api/home-timeline/read?user_id=1&start=0&stop=10` and `/wrk2-api/user-timeline/read?user_id=1&start=0&stop=10`. All three scenarios overlap with these paths (the seed varies `user_id`). Both were derived independently from the same nginx configuration and wrk2 scripts, and the gateway serves no other unauthenticated read.
