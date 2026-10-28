# Repository tooling map

Use existing gates when extending a contract. New agent implementations and commands must be covered by the registry sweeps; new skill documents are discovered by glob.

| Owner / tool | Contract and extension point |
| --- | --- |
| `sdo/agents/prompt_templates.py` | The sole prompt renderer/provider dispatch boundary. `prompt_sections.json` enumerates every template placeholder and names its cap in `prompt_budgets.toml`. A missing cap fails at runtime. |
| `tests/unit/sdo/agents/prompts/test_context_registry.py` | Enumerates capability registrations, all production Python source, every prompt template and registered MCP/CLI tool. Add production render fixtures for new registered capabilities. AST negative controls guard bypasses. |
| `tests/unit/sdo/agents/prompts/test_production_caps.py` | Real-scale generated inputs through production prompt/CLI/MCP producers. Every registered tool needs an executable fixture; unknown registrations fail. |
| `sdo/agents/context_output.py` | Shared lossless spill/summary boundary, including JSON framing, file pointers, Unicode and byte budgets. Full values remain in private session files. |
| `scripts/check_skill_sizes.py` | Glob-based skill document size gate in `check_errors.sh`; 100 lines/1,200 words per SKILL.md, 2,000 words per other Markdown file. |
| `scripts/gen_skill_references.sh` | Generate SDK indexes and bounded topic files from `go doc`; `--check` rejects missing, stale and orphan generated files. |
| `scripts/update_prompt_goldens.py` | Regenerate normalized prompt goldens after intentional changes. |
| `scripts/check_prompt_budgets.py` | Every commit's budget raises require an isolated manifest-only commit and `Budget-Reason:` trailer; decreases need no exception. |
| `scripts/check_session_context.py` | Read a Codex/Claude provider session file and reject excessive first/continuation messages or accumulated tool text. |
| `benchmarks/sregym/assurance/context_preflight.py` | Offline real-scale shapes through production producers and the scripted provider's session writer. Fastloop preflight and the hermetic Python shard run the gate. |

Run `scripts/format_code.sh` and `scripts/check_errors.sh` after code edits. Focused validation commands and cost boundaries live in [the testing guide](testing-guide.md).
