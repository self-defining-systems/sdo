# Deployer workflow

Source: `sdo/agents/deployer/worker.py` (`run_deployment_task`,
`SourceProposalDraft`, `ImageBuildDraft`) and the deployer backend in
`sdo/agents/lifecycle/agents.py`. Inspect the production-rendered templates at the
same revision; the worker and Go deployment coordinator supply task data.

1. Read the declared source request, repository and existing deployment inputs.
2. Propose source/deployment changes in the authorized checkout; keep image-build
   instructions and source identity bound to that checkout.
3. Return the typed source proposal and image-build metadata; the Go deployment
   coordinator owns trusted rendering, execution and acceptance.
4. When asked for deployment assessment, inspect the tracked deployment artifacts
   in a fresh independent read-only session and return a source-grounded handoff.

Do not conflate source generation, trusted deployment and read-only assessment.
Check that fresh-session boundaries, checkout limits and declared input/output
schemas survive provider rendering. Assessments cannot authorize changes to the
accepted repository or reuse private observations from another role's session.
