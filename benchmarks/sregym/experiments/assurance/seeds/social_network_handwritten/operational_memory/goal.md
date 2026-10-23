---
schema_version: 1
owner: human
application: Social Network
---
# Health objective

The deployed Deployments named compose-post-service, home-timeline-redis, home-timeline-service, jaeger, media-frontend, media-memcached, media-mongodb, media-service, nginx-thrift, post-storage-memcached, post-storage-mongodb, post-storage-service, social-graph-mongodb, social-graph-redis, social-graph-service, text-service, unique-id-service, url-shorten-memcached, url-shorten-mongodb, url-shorten-service, user-memcached, user-mention-service, user-mongodb, user-service, user-timeline-mongodb, user-timeline-redis, user-timeline-service remain available; the deployed Services with the same names expose ready endpoints; the per-service configuration ConfigMaps with the same names and every non-optional ConfigMap volume reference remain present; no NetworkPolicy denies all traffic to or from these workloads; and representative timeline reads through nginx-thrift succeed.
