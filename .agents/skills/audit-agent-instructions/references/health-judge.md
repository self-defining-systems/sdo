# Health-judge workflow

Source: `sdo/agents/health_judge/worker.py`, `run_health_judge` in
`sdo/agents/lifecycle/agents.py`, `memory.PrepareHealth` and the shared validator.
The rendered task chooses assessment or the direct-edit authoring capability.

1. Read the human goal, accepted architecture/source and evidence supplied for
   the current independent judge round.
2. Assess how to measure application health; ground objectives and traffic in
   the source rather than assuming absent metrics or endpoints exist.
3. For an authorized authoring round, use memory and detector skills to edit the
   health detector/traffic artifacts and health-objective playbook in the proposal
   checkout; preserve controller-owned generic policy detectors and provenance.
4. Validate final memory with the shared check and return typed acceptance or
   actionable correction metadata with logical detector IDs.
5. Evaluate the controller's observed healthy baseline in a fresh independent
   round; do not turn missing observations into health or self-author exclusions.

Check that read-only assessments do not receive contradictory file-edit recipes,
round feedback is bounded and independent, and accepted source identity follows
all artifacts. Go prepares generic policy detectors and controls acceptance.
