"""Hand-written RH1/RH2 replay plans for the exact first ten mixed-stream slots.

These are instructions, never captured experiment outputs. Astronomy repairs are
candidates reduced from recovery because RH1's composite oracles were blocked.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Step:
    operation: str
    kind: str
    name: str

    def __post_init__(self) -> None:
        if self.operation not in {
            "restore",
            "delete",
            "restart",
            "credentials",
            "kafka_skip",
            "recreate",
            "replace_pending",
            "rolling",
        }:
            raise ValueError(f"unknown replay operation {self.operation}")
        if self.kind not in {
            "Deployment",
            "Service",
            "ConfigMap",
            "NetworkPolicy",
            "Job",
            "MutatingWebhookConfiguration",
            "ValidatingWebhookConfiguration",
        }:
            raise ValueError(f"unsupported replay resource {self.kind}")
        if not self.name or "/" in self.name:
            raise ValueError("replay resource name must be non-empty and unqualified")


@dataclass(frozen=True)
class Slot:
    problem_id: str
    namespace: str
    steps: tuple[Step, ...]


# Preserve the exact draw identities and recorded social repair order. No
# harness recovery/oracle implementation is called from the scripted worker.
SCRIPTED_REPLAY_SLOTS = (
    Slot(
        "mf__astronomy_shop__n3__s1416840650__reliable__v_",
        "astronomy-shop",
        (
            Step("delete", "MutatingWebhookConfiguration", "pod-policy.validation.k8s.io"),
            Step("restore", "ConfigMap", "flagd-config"),
            Step("restore", "Deployment", "shipping"),
        ),
    ),
    Slot(
        "mf__astronomy_shop__n5__s64878673__reliable__v_",
        "astronomy-shop",
        (
            Step("delete", "NetworkPolicy", "deny-dns-egress-checkout"),
            Step("restore", "ConfigMap", "flagd-config"),
            Step("restore", "Deployment", "shipping"),
            Step("restore", "Service", "checkout"),
            Step("restore", "Deployment", "frontend-proxy"),
            Step("restore", "Deployment", "load-generator"),
            Step("credentials", "Deployment", "product-catalog"),
            Step("credentials", "Deployment", "accounting"),
            Step("credentials", "Deployment", "product-reviews"),
            Step("restart", "Deployment", "product-catalog"),
        ),
    ),
    Slot(
        "mf__social_network__n2__s3455414167__reliable__v_",
        "social-network",
        (
            Step("delete", "MutatingWebhookConfiguration", "pod-policy.validation.k8s.io"),
            Step("restore", "Deployment", "user-service"),
        ),
    ),
    Slot(
        "mf__social_network__n2__s2571351228__reliable__v_",
        "social-network",
        (
            Step("restore", "Deployment", "media-service"),
            Step("restore", "Deployment", "user-service"),
        ),
    ),
    Slot(
        "mf__astronomy_shop__n5__s1913342792__reliable__v_",
        "astronomy-shop",
        (
            Step("restore", "ConfigMap", "flagd-config"),
            Step("kafka_skip", "Deployment", "orders-validator"),
            Step("credentials", "Deployment", "product-catalog"),
            Step("credentials", "Deployment", "accounting"),
            Step("credentials", "Deployment", "product-reviews"),
            Step("restart", "Deployment", "product-catalog"),
        ),
    ),
    Slot(
        "mf__astronomy_shop__n3__s2092627323__reliable__v_",
        "astronomy-shop",
        (
            Step("restore", "ConfigMap", "flagd-config"),
            Step("restore", "Deployment", "ad"),
            Step("restore", "Deployment", "frontend"),
        ),
    ),
    Slot(
        "mf__astronomy_shop__n2__s1471960113__reliable__v_",
        "astronomy-shop",
        (
            Step("delete", "Job", "valkey-memory-flood"),
            Step("restore", "Deployment", "shipping"),
            Step("restart", "Deployment", "valkey-cart"),
        ),
    ),
    Slot(
        "mf__social_network__n2__s1969073207__reliable__v_",
        "social-network",
        (
            Step("restore", "ConfigMap", "media-mongodb"),
            Step("restore", "Deployment", "media-mongodb"),
            Step("restore", "Service", "compose-post-service"),
            Step("restore", "Deployment", "compose-post-service"),
        ),
    ),
    Slot(
        "mf__astronomy_shop__n2__s2139970257__reliable__v_",
        "astronomy-shop",
        (
            Step("delete", "Job", "valkey-memory-flood"),
            Step("restore", "Deployment", "currency"),
            Step("restart", "Deployment", "valkey-cart"),
        ),
    ),
    Slot(
        "mf__social_network__n5__s2045972234__reliable__v_",
        "social-network",
        (
            Step("delete", "ValidatingWebhookConfiguration", "pod-policy.validation.k8s.io"),
            Step("restore", "Deployment", "url-shorten-service"),
            Step("restore", "Deployment", "user-service"),
            Step("restore", "Deployment", "text-service"),
            Step("delete", "NetworkPolicy", "deny-dns-egress-media-service"),
            Step("restart", "Deployment", "social-graph-service"),
            Step("recreate", "Deployment", "url-shorten-service"),
            Step("replace_pending", "Deployment", "url-shorten-service"),
            Step("rolling", "Deployment", "url-shorten-service"),
        ),
    ),
)
