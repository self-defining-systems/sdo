"""Probe recipes for benchmark observations, with no harness/oracle dependency.

Field probes compare only the injected configuration surface to the live,
pre-injection reference. They deliberately abstain on other valid configurations;
this is conservative timing evidence, not a repair prescription for SDO.
"""

from __future__ import annotations

# Paths traverse named container lists with '*'. References are captured before
# injection by probes_for; they never come from injector backups or an oracle.
_POD = "spec.template.spec."
_CONTAINER = _POD + "containers.*."
BASELINE_FIELDS: dict[str, tuple[str, ...]] = {
    "assign_to_non_existent_node": (_POD + "nodeSelector",),
    "wrong_dns_policy": (_POD + "dnsPolicy", _POD + "dnsConfig"),
    "init_container_dependency_hang": (_POD + "initContainers",),
    "pvc_claim_mismatch": (_POD + "volumes",),
    "duplicate_pvc_mounts": (_POD + "volumes", _CONTAINER + "volumeMounts", _POD + "affinity"),
    "sidecar_port_conflict": (_CONTAINER + "command", _CONTAINER + "args", _CONTAINER + "ports"),
    "service_port_conflict": (_CONTAINER + "ports",),
    "wrong_bin_usage": (_CONTAINER + "command", _CONTAINER + "args"),
    "env_variable_shadowing": (_CONTAINER + "env",),
    "missing_env_variable": (_CONTAINER + "env",),
    "incorrect_port_assignment": (_CONTAINER + "env",),
    "liveness_probe_misconfiguration": (_CONTAINER + "livenessProbe",),
    "liveness_probe_too_aggressive": (_CONTAINER + "livenessProbe", _CONTAINER + "startupProbe"),
    "update_incompatible_correlated": (_CONTAINER + "image",),
    "pod_anti_affinity_deadlock": (_POD + "affinity",),
    "edge_request_filter_cpu_saturation": (
        _CONTAINER + "command",
        _CONTAINER + "args",
        _CONTAINER + "env",
        _CONTAINER + "resources",
    ),
    "rolling_update_misconfigured": ("spec.strategy", _POD + "initContainers"),
}

# These faults remove a replica and block its recreation. A current, fully
# available rollout proves admission on this target recovered, without needing
# webhook TLS Secret contents or any admission request from the observer.
AVAILABILITY_KINDS = frozenset(
    {
        "admission_webhook_tls_mismatch",
        "admission_webhook_outage",
        "psa_restricted_blocks_recreation",
        "namespace_memory_limit",
    }
)

FEATURE_FLAGS = {
    "loadgenerator_flood_homepage": "loadGeneratorFloodHomepage",
    "ad_service_manual_gc": "adManualGc",
    "cart_service_failure": "cartFailure",
    "astronomy_shop_ad_service_image_slow_load": "imageSlowLoad",
    "astronomy_shop_payment_service_failure": "paymentFailure",
    "astronomy_shop_payment_service_unreachable": "paymentUnreachable",
    "astronomy_shop_product_catalog_service_failure": "productCatalogFailure",
}

FLAG_FIELDS = {
    "astronomy_shop_ad_service_high_cpu": ("adHighCpu", (_CONTAINER + "resources.limits.cpu",)),
    "astronomy_shop_failed_readiness_probe": ("failedReadinessProbe", (_CONTAINER + "readinessProbe",)),
}

CLASS_KINDS = {
    "K8STargetPortMisconfig": "target_port",
    "IncorrectImage": "incorrect_image",
    "ReadinessProbeMisconfiguration": "readiness",
    "WrongServiceSelector": "wrong_selector",
    "NetworkPolicyBlock": "network_policy",
    "MissingConfigMap": "configmap",
    "ResourceRequestTooLarge": "resource_request",
    "ResourceRequestTooSmall": "resource_request",
    "AssignNonExistentNode": "assign_to_non_existent_node",
    "WrongDNSPolicy": "wrong_dns_policy",
    "InitContainerDependencyHang": "init_container_dependency_hang",
    "PVCClaimMismatch": "pvc_claim_mismatch",
    "DuplicatePVCMounts": "duplicate_pvc_mounts",
    "SidecarPortConflict": "sidecar_port_conflict",
    "ServicePortConflict": "service_port_conflict",
    "WrongBinUsage": "wrong_bin_usage",
    "EnvVariableShadowing": "env_variable_shadowing",
    "MissingEnvVariable": "missing_env_variable",
    "IncorrectPortAssignment": "incorrect_port_assignment",
    "LivenessProbeMisconfiguration": "liveness_probe_misconfiguration",
    "LivenessProbeTooAggressive": "liveness_probe_too_aggressive",
    "UpdateIncompatibleCorrelated": "update_incompatible_correlated",
    "MisconfigAppHotelRes": "misconfig_app_hotel_res",
    "PodAntiAffinityDeadlock": "pod_anti_affinity_deadlock",
    "RollingUpdateMisconfigured": "rolling_update_misconfigured",
    "AdmissionWebhookTLSMismatch": "admission_webhook_tls_mismatch",
    "AdmissionWebhookOutage": "admission_webhook_outage",
    "PSARestrictedBlocksRecreation": "psa_restricted_blocks_recreation",
    "NamespaceMemoryLimit": "namespace_memory_limit",
    "LoadGeneratorFloodHomepage": "loadgenerator_flood_homepage",
    "AdServiceManualGc": "ad_service_manual_gc",
    "CartServiceFailure": "cart_service_failure",
    "ImageSlowLoad": "astronomy_shop_ad_service_image_slow_load",
    "PaymentServiceFailure": "astronomy_shop_payment_service_failure",
    "PaymentServiceUnreachable": "astronomy_shop_payment_service_unreachable",
    "ProductCatalogServiceFailure": "astronomy_shop_product_catalog_service_failure",
    "AdServiceHighCpu": "astronomy_shop_ad_service_high_cpu",
    "FailedReadinessProbe": "astronomy_shop_failed_readiness_probe",
    "MissingService": "missing_service",
    "InternalTrafficPolicyLocalAstronomyShop": "internal_traffic_policy_local_astronomy_shop",
    "ServiceDNSResolutionFailure": "service_dns_resolution_failure_netpol",
    "IngressMisroute": "ingress_misroute",
    "HPAMissingEffectiveCPURequest": "hpa_missing_effective_cpu_request",
    "DevShmExhaustionHotelReservation": "dev_shm_exhaustion_hotel_reservation",
    "EdgeRequestFilterCPUSaturation": "edge_request_filter_cpu_saturation",
    "FinalizerDeadlockController": "finalizer_deadlock_controller",
    "CronJobSidecarBlocksCompletionHotelReservation": "cronjob_sidecar_blocks_completion_hotel_reservation",
    "MutatingWebhookResourceLimits": "mutating_webhook_resource_limits_social_network",
}
BASELINE_FIELDS["misconfig_app_hotel_res"] = (_CONTAINER + "image",)

# Public problem attributes needed to name observable API objects. Never inspect
# injection backups, diagnosis text, oracle objects or credential values.
PROBE_ATTRIBUTES = {
    "IngressMisroute": {"ingress_name": "ingress_name", "path": "path", "correct_service": "correct_service"},
    "HPAMissingEffectiveCPURequest": {"hpa_name": "HPA_NAME"},
    "DevShmExhaustionHotelReservation": {"scratch_mib": "scratch_mib"},
    "CronJobSidecarBlocksCompletionHotelReservation": {
        "cronjob_name": "cronjob_name",
        "primary_container": "PRIMARY_CONTAINER",
        "sidecar_container": "SIDECAR_CONTAINER",
    },
    "FinalizerDeadlockController": {
        key: key for key in ("namespace", "controller_name", "sa_name", "clusterrole_name", "clusterrolebinding_name")
    },
}

UNSUPPORTED_REASONS = {
    "MongoDBUserUnregistered": (
        "database user catalog is not visible in Kubernetes objects;"
        " add a read-only usersInfo observation without credential output"
    ),
    "MongoDBRevokeAuth": (
        "database roles are not visible in Kubernetes objects;"
        " add a read-only usersInfo roles observation without credential output"
    ),
    "KafkaQueueProblems": (
        "disabled feature flag cannot prove broker backlog drained; add read-only broker/consumer progress observations"
    ),
    "KafkaPoisonPillHOLBlock": (
        "Pod readiness cannot prove consumer head-of-line progress; add read-only consumer offset/lag observations"
    ),
    "ValkeyMemoryDisruption": (
        "runtime memory/eviction settings are not visible in Kubernetes objects;"
        " add read-only CONFIG GET/INFO observations"
    ),
    "ValkeyAuthDisruption": (
        "Pod readiness cannot prove application authentication;"
        " add a private read-only authentication handshake without credential output"
    ),
    "SecretRotationStaleEnvCredentialsAstronomyShop": (
        "Pod/Secret metadata cannot prove resolved credential freshness;"
        " add a private authentication handshake, never read Secret data"
    ),
    "EphemeralPortRangeHotelReservation": (
        "process/network-namespace sysctls are not visible in Kubernetes objects;"
        " add a read-only in-container sysctl/connection observation"
    ),
    "ExpiredTlsHotelReservation": (
        "Ingress metadata cannot prove served certificate validity;"
        " add a public TLS handshake observer, never read Secret data"
    ),
}
