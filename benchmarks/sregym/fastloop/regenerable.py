"""Secrets that a fault's own setup creates and the baseline can never archive.

Baselines never hold Secret data, so a captured fault-infrastructure namespace
(a webhook backend left behind by an earlier fault) restores its workloads
without the TLS Secret they mount. Restore regenerates those Secrets here, the
way the fault's backend setup does at injection: a fresh self-signed
certificate covering the namespace's Service names. Secret data is never read.
Restore owns this in one place, ``reconcile_application``, which both the
in-loop fault recovery and ``fastloop reset`` call.
"""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

if TYPE_CHECKING:
    from collections.abc import Iterable

#: (namespace, Secret name) of backend TLS Secrets that fault setups create in their own namespace.
REGENERABLE_TLS_SECRETS = frozenset(
    {
        ("policy-system", "pod-policy-webhook-tls"),  # AdmissionWebhookTLSMismatch
        ("policy-system", "compliance-webhook-tls"),  # CumulativeAdmissionWebhookTimeout
        ("platform-ops", "platform-policy-controller-tls"),  # MutatingWebhookResourceLimits
    }
)
VALIDITY = timedelta(days=365)


def is_regenerable(namespace: str, kind: str, name: str) -> bool:
    return kind == "Secret" and (namespace, name) in REGENERABLE_TLS_SECRETS


def tls_secret_manifest(namespace: str, name: str, services: Iterable[str]) -> dict[str, Any]:
    """A kubernetes.io/tls Secret with a fresh self-signed certificate for the Services' cluster DNS names."""
    names = sorted(
        {f"{service}.{namespace}.{suffix}" for service in services for suffix in ("svc", "svc.cluster.local")}
    )
    if not names:
        names = [f"{name}.{namespace}.svc"]
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, names[0])])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + VALIDITY)
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(n) for n in names]), critical=False)
        .sign(key, hashes.SHA256())
    )
    pem_certificate = certificate.public_bytes(serialization.Encoding.PEM)
    pem_key = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace},
        "type": "kubernetes.io/tls",
        "data": {
            "tls.crt": base64.b64encode(pem_certificate).decode(),
            "tls.key": base64.b64encode(pem_key).decode(),
        },
    }


def referenced_regenerable_secrets(objects: list[dict[str, Any]], namespace: str) -> list[dict[str, Any]]:
    """Fresh manifests for the regenerable Secrets that the baseline's workloads reference."""
    from benchmarks.sregym.fastloop.recovery_closure import prerequisites

    services = [v["metadata"]["name"] for v in objects if v.get("kind") == "Service"]
    wanted = sorted(
        {
            ref.name
            for v in objects
            for ref in prerequisites(v, namespace)
            if is_regenerable(ref.namespace, ref.kind, ref.name)
        }
    )
    return [tls_secret_manifest(namespace, name, services) for name in wanted]
