# Repository tooling map

Use existing gates when extending a contract. New agent implementations and commands must be covered by the registry sweeps; new skill documents are discovered by glob.

| Owner / tool | Contract and extension point |
| --- | --- |
| `benchmarks/sregym/repository_git.py` | Command-local, exact-path Git trust for shared checkout reads and clones (including upload-pack). Real Git ownership regression tests and the AST host-command ratchet live in `tests/unit/benchmarks/sregym/fastloop/test_shared_git_ownership.py`; static negative controls run in `check_errors.sh`. |
| `sdo/agents/prompt_templates.py` | The sole prompt renderer/provider dispatch boundary. `prompt_sections.json` enumerates every template placeholder and names its cap in `prompt_budgets.toml`. A missing cap or overflowing required section fails at runtime. Required parts survive composition unchanged; optional parts explicitly opt into ranked trimming with counts. |
| `tests/unit/sdo/agents/prompts/test_context_registry.py` | Enumerates capability registrations, all production Python source, every prompt template and registered MCP/CLI tool. Add production render fixtures for new registered capabilities. AST negative controls guard bypasses. |
| `tests/unit/sdo/agents/prompts/test_production_caps.py` | Real-scale generated inputs through production prompt/CLI/MCP producers. Every registered tool needs an executable fixture; unknown registrations fail. |
| `tests/unit/sdo/agents/prompts/test_required_content.py` | Always-run required-content ratchet: every agent render, every template placeholder, fresh/retry composition and synthetic profiles at synthetic high-cardinality profiles. Overflow negative controls prove required content cannot be summarized; the responder-stage overflow regression also runs in `check_errors.sh`. |
| `sdo/agents/context_output.py` | Shared lossless spill/summary boundary, including JSON framing, file pointers, Unicode and byte budgets. Full values remain in private session files. |
| `libs/agent_cli/provider_capacity.py` | Shared provider admission owner; resolves pool paths and rejects checkout-contained coordination metadata before creating files or starting a CLI. Negative controls cover the checkout itself, descendants and symlink aliases. |
| `libs/agent_cli/claude_accounts.py`, `scripts/prepare_claude_accounts.py` | Host-local cswap inventory, private mounted account references, per-account admission and crash-safe health; alias-only account evidence and state-change posts. |
| `scripts/prepare_opus_host_pool.py` | Provision static host tokens and hard-link aliases under existing kind extraMount roots; reject different alias inodes or capacity without replacing live files. |
| `scripts/check_skill_sizes.py` | Glob-based skill document size gate in `check_errors.sh`; 100 lines/1,200 words per SKILL.md, 2,000 words/12k characters per reference file. |
| `scripts/gen_config_docs.sh` | Generate `docs/config.md` from `contracts/settings` tags and `Default()` with `sdo/operator/cmd/configdoc`; `--check` fails on drift (run by `check_go_errors.sh`). |
| `benchmarks/fuzzops/sdo-bench.yaml`, `benchmarks/fuzzops/config.py` | The benchmark operator profile (only fields that differ from production defaults) every harness passes first as `--config`, and the helpers that build the `--config` arguments, `SDO_*` launch variables, local worker variables (from `sdo-operator config print`) and the format-1 fuzzops baseline translation. Contracts: `tests/unit/benchmarks/fuzzops/test_config.py`, `tests/unit/benchmarks/sregym/adapter/test_native_launch_args_contract.py`. |
| `scripts/gen_skill_references.sh` | Generate SDK indexes and bounded topic files from `go doc`; `--check` rejects missing, stale and orphan generated files. |
| `scripts/update_prompt_goldens.py` | Regenerate normalized prompt goldens after intentional changes. |
| `scripts/check_prompt_budgets.py` | Every commit's budget raises require an isolated manifest-only commit and `Budget-Reason:` trailer; decreases need no exception. CI runs it on pull requests only: the squash merge turns the PR into one commit, which the per-commit policy cannot judge. |
| `scripts/check_session_context.py` | Read a Codex/Claude provider session file and reject excessive first/continuation messages or accumulated tool text. |
| `sdo/agents/onboarding/run_session.py` | Run standalone Claude onboarding from a frozen Go bootstrap commit. The launcher returns export rejections in the same session and prints the validated Git commit on success. Run with `--help` for the launcher arguments. |
| `benchmarks/sregym/skilleval/runner.py` | Offline reflection evaluation from frozen Codex/Claude scenarios. Native writer ownership is recreated only for private transcript copies; host admission caps offline Opus jobs. Metrics dedupe Claude request IDs against the responder prefix and price token classes rather than cumulative session USD. Focused contracts: `tests/unit/benchmarks/sregym/skilleval/`. |
| `benchmarks/sregym/adapter/context_preflight.py` | Offline real-scale shapes through production producers and the scripted provider's session writer. Fastloop preflight and the hermetic Python shard run the gate. |

## Merge flow (trunk-based)

`main` is the trunk. There is no long-lived integration branch. The `main-required-ci` ruleset (and branch protection) require one check, `Required PR CI`, with `strict=false`, and the ruleset's merge queue (squash, up to 5 entries per group) re-runs the full check on the combined result before landing, so two separately green PRs cannot turn `main` red together. Open the PR, run `gh pr merge --auto --squash`, and it joins the queue when that check is green and merges once the queue's run is green (about 3 minutes each; pull requests run only the tests their change can affect, `scripts/ci_select.py`). Every push to `main` then runs the full suite and the coverage floor in `ci.yml`; a failure there is fixed forward by the next PR, not by blocking the others. Required jobs stay within the 5 minute budgets in `.github/ci_budgets.json`; heavy suites belong in `nightly.yml`, and a flaky test is fixed or quarantined within a day ([flaky tests](testing-guide.md#flaky-tests)). `ci.yml` runs on `merge_group` (full suite, `ci_select.py` plan for the queue event).

Run `scripts/format_code.sh` and `scripts/check_errors.sh` after code edits. Focused validation commands and cost boundaries live in [the testing guide](testing-guide.md).

The local gate runs every static check that pull-request CI runs: `tests/unit/scripts/test_local_gate_parity.py` fails when a `scripts/check_*` entry point, or a script the CI static job names, is not reachable from `check_errors.sh` and is not listed with a reason in its `NOT_LOCAL` table (checks that need a CI log, an image or provider credentials). It also runs every Go `//ci:whole-tree` guard (`scripts/test_go_ci.sh guards`), which pull request CI always runs; Python `CI_WHOLE_TREE` files run locally only where `check_errors.sh` names them.

`scripts/check_pytest_imports.py` maps test files to pytest prepend-mode package names and rejects collisions before collection. `scripts/check_errors.sh` runs it before any Python test; its subprocess tests cover flat modules, distinct packages and matching package names in separate roots. Rename conflicting tests or make their package identity distinct.

The fast repository ratchets in `scripts/check_errors.sh` execute the image-provenance entrypoint with `python -I -S`, no `PYTHONPATH`, and Git different-owner checking. This guards standalone startup and the shared Git policy before image builds, alongside the image-bound dependency check.

`benchmarks/sregym/analysis/stream_accounting.py` owns stream cost states and
role-separated median/ratio denominators. Its synthetic contracts cover missing,
partial and model-free rows, zero denominators and build boundaries. The Claude
reconciler requires positive native no-dispatch evidence; peer billing cannot
prove another role's cost is zero.

`scripts/go_quality_checks.py` owns the mandatory operator quality test selections
and their original 20-second limits. Local static checks execute that inventory;
CI prepares the same race binaries and assigns the tests to its two existing
native jobs. Prepared manifests must contain every assigned quality package,
even when the regression planner selects none of its tests. Missing quality
entries fail closed before execution. Compiler validation remains independent
of vet: the inventory fixture rejects compiler-only directive errors too.

`scripts/check_experiment_outputs.py` scans all tracked text and root report paths
in `check_errors.sh`. Its synthetic temporary-Git negative controls cover run
markers, extensionless and hidden files, binary files, deletions and Git failures.
Keep run reports and measurements in an external experiment directory.

## Onboard a new eval host

`benchmarks/sregym/hosts/hosts.toml` (paths and numbers only, no secrets) holds one `[hosts.<name>]` table per host; `[defaults]` is merged under each.

1. Add a table: `lane_root` (local disk; NFS is refused for lanes, images and evidence), `guard.lane_prefix`, `credentials.profile_dir`. Override `slice.memory_max_gib` or `slice.cpu_cores` only when the derived limits are wrong.
2. `uv run python -m benchmarks.sregym.hosts plan --host NAME` shows every change and diff (nothing is written; `--host` runs over ssh, omit it on the host).
3. Root parts need sudo: `... apply --host NAME --sudo-password-file FILE` (or `SUDO_PASSWORD_FILE`; the password goes only to `sudo -S`).
4. `apply` writes `/etc/systemd/system/sdo-eval.slice` (MemoryMax 50% of RAM, MemoryHigh 94% of that, CPUQuota 44% of cores, CPUWeight 100), `/etc/tmpfiles.d/mglru.conf` (skipped with a warning without `lru_gen`), `/etc/sysctl.d/90-sdo-eval.conf` (kind neighbour table, inotify), the memory-guard env file (`SDO_HOST_*`, lane budget = slice MemoryMax; `fastloop up` reads `~/.config/sdo/host.env` or `SDO_HOST_ENV_FILE`), and the disk-watcher user timer (an installed watcher script is adopted; a new host gets the controller's copy).
5. Credentials are opt-in: `apply --credentials-from chelan5` copies only the ClaudeGCP `settings.json` (key path rewritten), the service-account key and `.claude.json` (dir 700, files 600; never projects or transcripts; no value is printed).
6. `verify --host NAME [--smoke]` prints a pass/fail table (slice limits, MGLRU, sysctls, local lane/evidence/docker roots, guard file, watcher timer, credentials, docker systemd cgroup v2, lanes under the slice; `--smoke` makes one model call). `remove` undoes the root files, timer and guard keys, and leaves the lane root, script and credentials.
7. Plans and applies are idempotent: a second `plan` shows no change. Lanes join the slice through the lane cgroup parent (`sdo-eval.slice`); `verify` reports lanes still outside it as a warning.

## Shared-host memory guard

Agent builds and tests share hosts with eval lanes, so they run under `scripts/capped_run.sh [--memory 16G] -- <command>`: it sets `GOFLAGS=-p=4` and `GOMAXPROCS=8` and, when `systemd-run --user --scope` really applies `MemoryMax` (checked through the scope's own `memory.max`), caps the command at 16G (`SDO_CAPPED_MEMORY_MAX`); otherwise it falls back to `nice`/`ionice`. No root needed. Lane and cell starts are gated by `benchmarks/sregym/adapter/host_memory.py`: `fastloop up` and the eval scheduler refuse or wait (the reason is logged) unless `MemAvailable >= lane cap + reserve` (default 32 GiB) and memory PSI `some avg60` is under 10; `up` also refuses a lane whose cap takes the summed caps of our running lanes (Docker node limits under the host lane prefix) past the lane budget. Configure with `SDO_HOST_MEMORY_RESERVE_GIB`, `SDO_HOST_MEMORY_PSI_MAX`, `SDO_HOST_LANE_BUDGET_GIB`, `SDO_HOST_LANE_PREFIX` (or the `up --host-*` flags; `up` also reads these from `~/.config/sdo/host.env`, see above, and an exported variable wins); in an eval TOML use `host_reserve_gib`, `max_memory_pressure`, `lane_memory_cap_gib`, `lane_budget_gib`. Only our own lanes are counted; other clusters are never touched.

Lane isolation (`benchmarks/sregym/fastloop/lane_cgroup.py`, cgroup v2, no root): every kind node gets a Docker CPU quota (`cpu.max`; `--lane-cpus`/`SDO_LANE_CPUS`, default 6 per lane split across its nodes, `--cpu-limit` overrides per node) and the 16 GiB memory cap, re-applied with `docker update` at every eval claim (`lane_cpus` in the eval `[row]` table) so existing lanes get it too, then read back from the container's own `cpu.max`/`memory.max` (re-applied once if a `daemon-reload` dropped it, an error if it still differs). kind has no cgroup-parent setting, so when the parent slice exists (`--lane-cgroup-parent`/`SDO_LANE_CGROUP_PARENT`/`lane_cgroup_parent`, default `auto` = `sdo-eval.slice` if its unit file exists, `none` disables) a one-file `docker` shim is put first on `PATH` of the `kind create` processes; it adds `--cgroup-parent` only to a `docker run` carrying our kind-cluster label and passes every other docker call through. The slice only takes effect for lanes created after it exists (recreate older lanes to move them in; their per-container limits already apply). The mode is logged on `up` and at eval start; without a slice only per-container limits and the admission budget apply, and with one the slice's `MemoryMax` is the lane budget when `SDO_HOST_LANE_BUDGET_GIB`/`lane_budget_gib` is unset. Note that systemd nests `sdo-eval.slice` under `sdo.slice` in the cgroup tree. Each node's cpuset is `ceil(cpus)` CPUs; a host may reserve a CPU block with `SDO_LANE_PREFERRED_CPUS` (a CPU list inside one NUMA node, e.g. `88-111,200-223`, exported from the host env file): nodes are pinned inside it, free whole cores first then the free siblings of half-used cores, a node moves in at its next claim when the block has room, and placement spills to the rest of the host only when the block cannot hold the whole node.
