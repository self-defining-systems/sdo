"""Idempotent app deploy on a reused lane (eval class C10).

SREGym installs every app, and its telemetry, through ``Helm.install``, which runs a plain
``helm install``. On a lane that still holds the release, ``fastloop up --redeploy`` then fails with
"cannot re-use a name that is still in use". The worker bridge replaces that one entry point with
``helm upgrade --install``, so deploy is repeatable for every app without touching each app's code.
"""

from __future__ import annotations

import logging
import subprocess
from typing import Any

logger = logging.getLogger(__name__)


def install_or_upgrade(**args: Any) -> None:
    """``Helm.install`` with the same arguments; installs the release, or upgrades it if it exists."""
    release_name = args.get("release_name")
    chart_path = args.get("chart_path")
    namespace = args.get("namespace")
    if not release_name or not chart_path or not namespace:
        raise ValueError("helm deploy requires release_name, chart_path and namespace")
    logger.info("Helm upgrade --install: %s in namespace %s", release_name, namespace)
    if not args.get("remote_chart", False):
        subprocess.run(
            f"helm dependency update {chart_path}", shell=True, capture_output=True, check=False  # noqa: S602
        )
    command = f"helm upgrade --install {release_name} {chart_path} -n {namespace} --create-namespace"
    if args.get("version"):
        command += f" --version {args['version']}"
    if args.get("extra_args"):
        command += " " + " ".join(args["extra_args"])
    completed = subprocess.run(command, shell=True, capture_output=True, text=True, check=False)  # noqa: S602
    if completed.stderr.strip():
        logger.warning("Helm upgrade --install stderr output:\n%s", completed.stderr.strip())
    if completed.returncode != 0:
        raise RuntimeError(
            f"Helm install failed for release '{release_name}' in namespace '{namespace}'. "
            f"Chart: {chart_path}. Error output:\n{completed.stderr.strip()}\n"
            f"Stdout (for context):\n{completed.stdout.strip()}"
        )
    logger.debug(completed.stdout.strip())


def make_deploy_idempotent(helm: Any) -> None:
    """Route ``helm.install`` (SREGym's ``Helm`` class) through ``install_or_upgrade``."""
    helm.install = staticmethod(install_or_upgrade)
