"""The GitLab series: successive releases of the upstream GitLab Helm chart, one commit each.

Where it comes from: a release list (``apps/gitlab/releases*.json``) of real releases of the upstream chart
(https://gitlab.com/gitlab-org/charts/gitlab), each with its archive digest and what it changes in the rendered output
(images, values keys, workloads). Two lists exist: ``releases-10.5.4.json`` (the base is chart 10.5.4, the release the
fuzzops GitLab pack and its SDO baselines were made at; then 10.5.5, 10.5.7, 10.6.0, 10.6.1: two patches, a minor
release and a patch) and ``releases.json`` (base 10.4.2, for a baseline made there).

Why it is realistic: this is what an operator of a GitLab deployment actually has to absorb over time: patch releases
that move the CNG images, a minor release that moves every image and adds values. Nothing is invented. Each commit's
tree is the fuzzops GitLab pack's release rendered as the pack's SDO repository holds it (``helm template`` of its
umbrella chart with that upstream release and the pack's values, then the same plain-manifest conversion the hooks make
of the live release: no hooks or namespaces, and the Jobs that are not hooks), written as ``k8s/render.yaml``. A commit
therefore replays as the diff that one chart bump makes to that file.

The Jobs are workloads of SDO's deployment plan, and a head push has no route to amend the plan, so the baseline a
series runs on must already name them: ``baseline`` derives it from an existing baseline (``derive_baseline``).

The series is built at run time from cached or downloaded archives (digest checked); nothing is committed.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from apps.gitlab import assemble
from benchmarks.fuzzops.hooks import plain_docs
from benchmarks.sregym.fastloop.gitlab_releases import RELEASES_PATH, Release, load_releases

if TYPE_CHECKING:
    from collections.abc import Callable

_IDENT = ("-c", "user.name=gitlab-chart-release", "-c", "user.email=chart-release@invalid")
NAMESPACE = "fg1"
RENDER_FILE = "k8s/render.yaml"
NEXT_PATH = RELEASES_PATH.with_name("releases-10.5.4.json")


def plain_render(helm_output: str, *, migrations: bool = False) -> str:
    """The pack's SDO repository file for a render: plain documents without helm hooks, as ``yaml.safe_dump_all``.

    ``migrations`` keeps the Jobs that are not helm hooks (the chart's schema migration): a release needs its own, or
    its pods wait in an init container for the schema. A series keeps them in every commit, the base too.

    Pod ``checksum/*`` annotations are left out. They hash the rendered config, which SDO's own instrumentation has
    changed in the lane's file, so every chart bump would conflict on them there; an engineer would resolve that by
    hand and the replayer cannot."""
    docs = [
        doc
        for doc in plain_docs(helm_output, jobs=migrations)
        if "helm.sh/hook" not in ((doc.get("metadata") or {}).get("annotations") or {})
    ]
    for doc in docs:
        pod = (((doc.get("spec") or {}).get("template") or {}).get("metadata") or {}).get("annotations") or {}
        for key in [k for k in pod if k.startswith("checksum/")]:
            del pod[key]
    return yaml.safe_dump_all(docs, sort_keys=False)


def render_release(release: Release, archive: Path, pack: Path, *, migrations: bool = False) -> dict[str, str]:
    """The release rendered through the fuzzops GitLab pack at ``pack`` (its ``chart/`` and ``values-fuzzops.yaml``)."""
    with tempfile.TemporaryDirectory(prefix="gitlab-series-") as scratch:
        build = Path(scratch) / "chart"
        shutil.copytree(pack / "chart", build)
        with tarfile.open(archive) as tar:
            tar.extractall(build / "charts", filter="data")
        chart = build / "Chart.yaml"
        chart.write_text(
            re.sub(r"(- name: gitlab\n\s+version: )\S+", rf"\g<1>{release.version}", chart.read_text(), count=1)
        )
        done = subprocess.run(
            ["helm", "template", "gitlab", str(build), "-n", NAMESPACE, "-f", str(pack / "values-fuzzops.yaml")],
            capture_output=True,
            text=True,
            check=True,
        )
    return {RENDER_FILE: plain_render(done.stdout, migrations=migrations)}


def fetch(release: Release, cache: Path) -> Path:
    """The release's archive, from ``cache`` or downloaded, digest checked."""
    target = cache / f"gitlab-{release.version}.tgz"
    if not target.is_file():
        pins = release.pins(assemble.load_pins())
        cache.mkdir(parents=True, exist_ok=True)
        target = assemble.fetch_chart(pins, cache)
    assemble.verify_archive(target, release.sha256)
    return target


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", *_IDENT, "-C", str(repo), *args], capture_output=True, text=True, check=True)
    return done.stdout.strip()


def _write_tree(repo: Path, files: dict[str, str]) -> None:
    for name, text in files.items():
        target = repo / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)


def build(
    dest: Path,
    cache: Path,
    pack: Path,
    *,
    releases_path: Path = NEXT_PATH,
    limit: int | None = None,
    render: Callable[..., dict[str, str]] = render_release,
    fetch_archive: Callable[[Release, Path], Path] = fetch,
) -> Path:
    """Create ``dest`` (the history) with the base release's tree and one commit per later release; write the spec.

    ``limit`` keeps the first N commits after the base. Returns ``dest/series.json``.
    """
    releases = load_releases(releases_path)
    base, later = releases.releases[0], releases.releases[1:]
    later = later if limit is None else later[:limit]
    repo = dest / "upstream"
    repo.mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "trunk")
    _write_tree(repo, render(base, fetch_archive(base, cache), pack, migrations=True))
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", f"chore(deps): gitlab chart {base.version} (app {base.app_version}), the base")
    base_sha = _git(repo, "rev-parse", "HEAD")
    commits = []
    for release in later:
        _write_tree(repo, render(release, fetch_archive(release, cache), pack, migrations=True))
        _git(repo, "add", "-A")
        _git(
            repo,
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            f"chore(deps): bump gitlab chart to {release.version} (app {release.app_version})\n\n{release.note}",
        )
        commits.append(_git(repo, "rev-parse", "HEAD"))
    spec = {
        "name": "gitlab-chart-" + base.version.replace(".", "-"),
        "description": (
            f"The upstream GitLab chart releases after {base.version}: " + ", ".join(r.version for r in later) + "."
        ),
        "source": f"https://gitlab.com/gitlab-org/charts/gitlab releases; digests in apps/gitlab/{releases_path.name}",
        "upstream": {"repo": str(repo), "base": base_sha, "commits": commits},
        "mode": "diff",
        # a commit that does not apply is skipped and recorded, so one run shows every position that fails
        "on_conflict": "skip",
    }
    path = dest / "series.json"
    path.write_text(json.dumps(spec, indent=1) + "\n")
    return path


# ---- the baseline a series runs on ---------------------------------------------------------------------------------

_GENERATED = re.compile(r"^[a-z0-9]{5,10}$")  # the grammar of a plan's `*` (sdo/operator/rendered/workloads.go)
MEMORY_COMMIT_MESSAGE = (
    "sdo(series-base): the series' Jobs are workloads of the deployment plan\n\n"
    "The base release's migration and bucket Jobs are in the render, and the plan names every Job a release of the\n"
    "series carries (a generated name as Job/<namespace>/<prefix>-*).\n\n"
    "SDO-Incident: commit-series-baseline\nSDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed\n"
)


def stable_job_id(name: str, namespace: str = NAMESPACE) -> str:
    """The plan identity of a Job: a generated name suffix (five to ten lowercase alphanumerics with a digit) is `*`."""
    prefix, _, token = name.rpartition("-")
    if prefix and _GENERATED.match(token) and any(c.isdigit() for c in token):
        return f"Job/{namespace}/{prefix}-*"
    return f"Job/{namespace}/{name}"


def _jobs(render: str) -> list[str]:
    return [d["metadata"]["name"] for d in yaml.safe_load_all(render) if isinstance(d, dict) and d.get("kind") == "Job"]


def series_job_ids(repo: Path, revisions: list[str]) -> list[str]:
    """Every Job any of ``revisions`` renders, as plan workload IDs (sorted, once each)."""
    ids = {stable_job_id(name) for rev in revisions for name in _jobs(_git(repo, "show", f"{rev}:{RENDER_FILE}"))}
    return sorted(ids)


def derive_baseline(source: Path, dest: Path, series: Path) -> str:
    """A copy of the baseline ``source`` for the series ``series`` (its ``series.json``) in ``dest``: the memory's
    render gains the base release's Jobs (merged in where they sit in the base render) and its deployment plan names
    every Job the series carries. Returns the new memory commit. The series' commits then apply onto it as they did
    onto the source."""
    spec = json.loads(series.read_text())["upstream"]
    repo = Path(spec["repo"])
    base_text = _git(repo, "show", f"{spec['base']}:{RENDER_FILE}") + "\n"
    jobless = yaml.safe_dump_all(
        [d for d in yaml.safe_load_all(base_text) if isinstance(d, dict) and d.get("kind") != "Job"], sort_keys=False
    )
    shutil.copytree(source, dest, ignore=shutil.ignore_patterns("memory.bundle"))
    baseline = json.loads((dest / "baseline.json").read_text())
    with tempfile.TemporaryDirectory(prefix="series-baseline-") as scratch:
        work, files = Path(scratch) / "memory", Path(scratch)
        subprocess.run(
            ["git", "clone", "-q", "-b", baseline["branch"], str(source / baseline["bundle"]), str(work)], check=True
        )
        (files / "base").write_text(jobless)
        (files / "other").write_text(base_text)
        merged = subprocess.run(
            ["git", "merge-file", "-p", str(work / RENDER_FILE), str(files / "base"), str(files / "other")],
            capture_output=True,
            text=True,
            check=False,
        )
        if merged.returncode != 0:
            raise ValueError(f"the base release's Jobs do not merge into the baseline's render ({merged.returncode})")
        (work / RENDER_FILE).write_text(merged.stdout)
        plan_path = work / ".sdo" / "deployment" / "plan.yaml"
        plan = json.loads(plan_path.read_text())
        plan["expectedWorkloadIds"] += [
            i for i in series_job_ids(repo, [spec["base"], *spec["commits"]]) if i not in plan["expectedWorkloadIds"]
        ]
        plan_path.write_text(json.dumps(plan, separators=(",", ":")))
        _git(work, "commit", "-q", "-a", "-m", MEMORY_COMMIT_MESSAGE)
        head = _git(work, "rev-parse", "HEAD")
        _git(work, "bundle", "create", str(dest / baseline["bundle"]), baseline["branch"])
    baseline["commit"] = head
    (dest / "baseline.json").write_text(json.dumps(baseline, indent=2) + "\n")
    return head


# ---- measuring a release list -------------------------------------------------------------------------------------


def workload_images(docs: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Workload name -> its containers' image references, for the controllers of a render."""
    out: dict[str, list[str]] = {}
    for doc in docs:
        if doc.get("kind") not in ("Deployment", "StatefulSet", "DaemonSet"):
            continue
        spec = doc["spec"]["template"]["spec"]
        out[doc["metadata"]["name"]] = sorted(
            c["image"] for c in [*spec.get("initContainers", []), *spec.get("containers", [])]
        )
    return out


def flatten_keys(values: Any, prefix: str = "") -> set[str]:
    """Dotted paths of every mapping key of a values file."""
    keys: set[str] = set()
    if isinstance(values, dict):
        for key, value in values.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            keys.add(path)
            keys |= flatten_keys(value, path)
    return keys


def derive_changes(
    before: list[dict[str, Any]], after: list[dict[str, Any]], keys_before: set[str], keys_after: set[str], schema: bool
) -> dict[str, Any]:
    """What a release changes relative to the one before it, in the shape ``releases.json`` records."""
    images_before = {i for refs in workload_images(before).values() for i in refs}
    images_after = {i for refs in workload_images(after).values() for i in refs}
    names_before, names_after = set(workload_images(before)), set(workload_images(after))
    return {
        "images_changed": len(images_after - images_before),
        "values_keys_added": len(keys_after - keys_before),
        "values_keys_removed": len(keys_before - keys_after),
        "values_schema": schema,
        "workloads_added": sorted(names_after - names_before),
        "workloads_removed": sorted(names_before - names_after),
        "objects": len(after),
    }


def archive_values(archive: Path) -> tuple[set[str], bool]:
    """The upstream chart's values keys and whether it ships a ``values.schema.json``."""
    with tarfile.open(archive) as tar:
        names = tar.getnames()
        member = next(n for n in names if n.count("/") == 1 and n.endswith("/values.yaml"))
        values = yaml.safe_load(tar.extractfile(member).read())  # type: ignore[union-attr]
        return flatten_keys(values), any(n.endswith("/values.schema.json") and n.count("/") == 1 for n in names)


def release_kind(previous: str, version: str) -> str:
    """``Major``, ``Minor`` or ``Patch``: the most significant version field that changes from ``previous``."""
    before, after = previous.split("."), version.split(".")
    if before[0] != after[0]:
        return "Major"
    return "Minor" if before[1] != after[1] else "Patch"


def derive_list(index: Path, versions: list[str], base_version: str, pack: Path, cache: Path) -> dict[str, Any]:
    """A release list document for ``base_version`` then ``versions``: digests from the chart repository's ``index``,
    effects measured by rendering each release through the pack and comparing with the one before."""
    entries = {e["version"]: e for e in yaml.safe_load(index.read_text())["entries"]["gitlab"]}
    out: list[dict[str, Any]] = []
    previous: tuple[list[dict[str, Any]], set[str]] | None = None
    for position, version in enumerate([base_version, *versions]):
        entry = entries[version]
        release = Release(
            version=version,
            app_version=entry["appVersion"],
            created=entry["created"],
            url=entry["urls"][0],
            sha256=entry["digest"],
            baseline=position == 0,
            changes=None if position == 0 else _placeholder(),
        )
        archive = fetch(release, cache)
        docs = list(yaml.safe_load_all(render_release(release, archive, pack)[RENDER_FILE]))
        keys, schema = archive_values(archive)
        record: dict[str, Any] = {
            "version": version,
            "app_version": entry["appVersion"],
            "created": entry["created"],
            "url": entry["urls"][0],
            "sha256": entry["digest"],
        }
        if position == 0:
            record["baseline"] = True
            record["note"] = f"Series baseline: {len(docs)} rendered objects through the fuzzops GitLab pack."
        else:
            assert previous is not None
            changes = derive_changes(previous[0], docs, previous[1], keys, schema)
            record["changes"] = changes
            kind = release_kind(out[-1]["version"], version)
            record["note"] = (
                f"{kind} release, app {out[-1]['app_version']} -> {entry['appVersion']}: "
                f"{changes['images_changed']} image references change, "
                f"{changes['values_keys_added']} values keys added, {changes['values_keys_removed']} removed."
            )
        out.append(record)
        previous = (docs, keys)
    return {
        "chart": "gitlab",
        "line": f"{base_version.rpartition('.')[0]}+",
        "repository": "https://charts.gitlab.io",
        "derivation": (
            "Each release was rendered with helm template through the fuzzops GitLab pack (its umbrella chart and "
            "values-fuzzops.yaml) and compared with the release before it: distinct image references of the workloads, "
            "workload names, object count, keys of the upstream chart's values.yaml, presence of values.schema.json. "
            "Digests and dates are the chart repository index's."
        ),
        "releases": out,
    }


def _placeholder() -> Any:
    from benchmarks.sregym.fastloop.gitlab_releases import Changes

    return Changes(0, 0, 0, False, (), (), 0)


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="commit_series.gitlab")
    sub = parser.add_subparsers(dest="command", required=True)
    derive = sub.add_parser("derive", help="measure a release list from the chart repository index")
    derive.add_argument("--index", type=Path, required=True, help="https://charts.gitlab.io/index.yaml, downloaded")
    derive.add_argument("--base", required=True)
    derive.add_argument("--pack", type=Path, required=True)
    derive.add_argument("--cache", type=Path, required=True)
    derive.add_argument("--out", type=Path, required=True)
    derive.add_argument("versions", nargs="+")
    make = sub.add_parser("build", help="build a series into DEST")
    make.add_argument("dest", type=Path)
    make.add_argument("--pack", type=Path, required=True)
    make.add_argument("--cache", type=Path, required=True)
    make.add_argument("--list", type=Path, default=NEXT_PATH)
    make.add_argument("--limit", type=int)
    derived = sub.add_parser("baseline", help="derive the baseline a series runs on from an existing baseline")
    derived.add_argument("source", type=Path)
    derived.add_argument("dest", type=Path)
    derived.add_argument("--series", type=Path, required=True, help="the series.json `build` wrote")
    args = parser.parse_args(argv)
    if args.command == "baseline":
        print(derive_baseline(args.source, args.dest, args.series))
        return 0
    if args.command == "derive":
        document = derive_list(args.index, args.versions, args.base, args.pack, args.cache)
        args.out.write_text(json.dumps(document, indent=2) + "\n")
        return 0
    print(build(args.dest, args.cache, args.pack, releases_path=args.list, limit=args.limit))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
