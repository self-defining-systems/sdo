---
schema_version: 1
generated_at_commit: hand-authored
generated_at: 2026-10-23T00:00:00+00:00
application: Social Network
topology_fingerprint: hand-authored
---
# Social Network architecture

Hand-authored from the DeathStarBench socialNetwork Helm chart (`helm-chart/socialnetwork`), its nginx configuration, Lua handlers, Thrift service configuration and C++ service sources; not produced by an SDO deployer round.

## Workloads and Services

The chart deploys 27 subcharts into one namespace, each rendering a Deployment, a ClusterIP Service and a ConfigMap of the same name. Pods carry the labels `service=<name>` and `app=<name>`; each Service selects `service=<name>`. Every Deployment mounts its same-named ConfigMap (service-config.json and jaeger-config.yml for the C++ services, mongod.conf, redis.conf or memcached settings for the datastores, nginx.conf for the gateways).

- Gateway: `nginx-thrift` (OpenResty, port 8080) serves the static pages and maps `/api/...` and `/wrk2-api/...` locations to Lua handlers that call the Thrift services. Its init container fetches the Lua scripts and pages from the upstream DeathStarBench repository.
- Media gateway: `media-frontend` (port 8081) serves media upload and download.
- Thrift services (port 9090): `compose-post-service`, `home-timeline-service`, `media-service`, `post-storage-service`, `social-graph-service`, `text-service`, `unique-id-service`, `url-shorten-service`, `user-mention-service`, `user-service`, `user-timeline-service`.
- Datastores: MongoDB (`media-mongodb`, `post-storage-mongodb`, `social-graph-mongodb`, `url-shorten-mongodb`, `user-mongodb`, `user-timeline-mongodb`, port 27017), Redis (`home-timeline-redis`, `social-graph-redis`, `user-timeline-redis`, port 6379), memcached (`media-memcached`, `post-storage-memcached`, `url-shorten-memcached`, `user-memcached`, port 11211).
- Tracing: `jaeger` (all-in-one).

## Request paths

- Timeline reads: nginx-thrift calls `home-timeline-service` (reads `home-timeline-redis`) or `user-timeline-service` (reads `user-timeline-redis`, falling back to `user-timeline-mongodb`); both resolve post IDs through `post-storage-service`, which reads `post-storage-memcached` and `post-storage-mongodb` only when the timeline is not empty.
- Compose: nginx-thrift calls `compose-post-service`, which calls `unique-id-service`, `text-service` (which calls `url-shorten-service` and `user-mention-service`), `media-service`, `user-service`, `post-storage-service`, `user-timeline-service` and `home-timeline-service` (which asks `social-graph-service` for followers).
- Accounts and graph: nginx-thrift calls `user-service` for register and login, and `social-graph-service` for follow, unfollow and follower lists; `user-service` and `social-graph-service` call each other.
