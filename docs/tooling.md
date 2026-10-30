# Repository tooling map

Use existing gates when extending a contract. New agent implementations and commands must be covered by the registry sweeps; new skill documents are discovered by glob.

| Owner / tool | Contract and extension point |
| --- | --- |
| `benchmarks/sregym/repository_git.py` | Command-local, exact-path Git trust for shared checkout reads and clones (including upload-pack). Real Git ownership regression tests and the AST host-command ratchet live in `tests/unit/benchmarks/sregym/fastloop/test_shared_git_ownership.py`; static negative controls run in `check_errors.sh`. |
| `sdo/agents/prompt_templates.py` | The sole prompt renderer/provider dispatch boundary. `prompt_sections.json` enumerates every template placeholder and names its cap in `prompt_budgets.toml`. A missing cap or overflowing required section fails at runtime. Required parts survive composition unchanged; optional parts explicitly opt into ranked trimming with counts. |
| `tests/unit/sdo/agents/prompts/test_context_registry.py` | Enumerates capability registrations, all production Python source, every prompt template and registered MCP/CLI tool. Add production render fixtures for new registered capabilities. AST negative controls guard bypasses. |
| `tests/unit/sdo/agents/prompts/test_production_caps.py` | Real-scale generated inputs through production prompt/CLI/MCP producers. Every registered tool needs an executable fixture; unknown registrations fail. |
| `tests/unit/sdo/agents/prompts/test_required_content.py` | Always-run required-content ratchet: every agent render, every template placeholder, fresh/retry composition and synthetic profiles at recorded run19 cardinalities (91 questions, 24 repairs). Overflow negative controls prove required content cannot be summarized; the responder-stage overflow regression also runs in `check_errors.sh`. |
| `sdo/agents/context_output.py` | Shared lossless spill/summary boundary, including JSON framing, file pointers, Unicode and byte budgets. Full values remain in private session files. |
| `libs/agent_cli/provider_capacity.py` | Shared provider admission owner; resolves pool paths and rejects checkout-contained coordination metadata before creating files or starting a CLI. Negative controls cover the checkout itself, descendants and symlink aliases. |
| `libs/agent_cli/claude_accounts.py`, `scripts/prepare_claude_accounts.py` | Host-local cswap inventory, private mounted account references, per-account admission and crash-safe health; alias-only account evidence and state-change posts. |
| `scripts/prepare_opus_host_pool.py` | Provision static host tokens and hard-link aliases under existing kind extraMount roots; reject different alias inodes or capacity without replacing live files. |
| `scripts/check_skill_sizes.py` | Glob-based skill document size gate in `check_errors.sh`; 100 lines/1,200 words per SKILL.md, 2,000 words/12k characters per reference file. |
| `scripts/gen_skill_references.sh` | Generate SDK indexes and bounded topic files from `go doc`; `--check` rejects missing, stale and orphan generated files. |
| `scripts/update_prompt_goldens.py` | Regenerate normalized prompt goldens after intentional changes. |
| `scripts/check_prompt_budgets.py` | Every commit's budget raises require an isolated manifest-only commit and `Budget-Reason:` trailer; decreases need no exception. |
| `scripts/check_session_context.py` | Read a Codex/Claude provider session file and reject excessive first/continuation messages or accumulated tool text. |
| `benchmarks/sregym/skilleval/runner.py` | Offline reflection evaluation from frozen Codex/Claude scenarios. Native writer ownership is recreated only for private transcript copies; host admission caps offline Opus jobs. Metrics dedupe Claude request IDs against the responder prefix and price token classes rather than cumulative session USD. Focused contracts: `tests/unit/benchmarks/sregym/skilleval/`. |
| `benchmarks/sregym/adapter/context_preflight.py` | Offline real-scale shapes through production producers and the scripted provider's session writer. Fastloop preflight and the hermetic Python shard run the gate. |

Run `scripts/format_code.sh` and `scripts/check_errors.sh` after code edits. Focused validation commands and cost boundaries live in [the testing guide](testing-guide.md).

`scripts/check_pytest_imports.py` maps test files to pytest prepend-mode package names and rejects collisions before collection. `scripts/check_errors.sh` runs it before any Python test; its subprocess tests cover flat modules, distinct packages and matching package names in separate roots. Rename conflicting tests or make their package identity distinct.

The fast repository ratchets in `scripts/check_errors.sh` execute the image-provenance entrypoint with `python -I -S`, no `PYTHONPATH`, and Git different-owner checking. This guards standalone startup and the shared Git policy before image builds, alongside the image-bound dependency check.

`benchmarks/sregym/analysis/stream_accounting.py` owns stream cost states and
role-separated median/ratio denominators. Its synthetic contracts cover missing,
partial and model-free rows, zero denominators and build boundaries. The Claude
reconciler requires positive native no-dispatch evidence; peer billing cannot
prove another role's cost is zero.

Controller recovery integration fixtures use `sdo/operator/runtime/recovery_fixture_test.go`: bind the native cached-proof consumer and drive real detector observations before, just before and at the stability boundary. The PR planner protects Controller, Recovery, Closure, SourceInventory, Latent and Continuation families; synthetic planner displacement controls in `tests/unit/scripts/test_pr_test_plan.py` prevent cheaper filler from hiding them. Standalone detector/report units do not assert controller closure.

The two native CI producers are independent named jobs, with the same seven
executed jobs in total. Python depends only on normal preparation; contracts
depend on both. Preparation publishes one exact run/revision-scoped normal Go cache
for nested workspace builds, plus per-bin archives of selected race executables
and their plan. Contract shards never transfer the race compiler cache or link
those test binaries. `scripts/prepared_contracts.py` binds binaries to the source,
plan, selected names and content hashes, then executes them through test2json.
Its real producer/consumer fixture runs with compilation forbidden; workflow
negative controls reject misses, fallback keys and extra compiler transfers.
