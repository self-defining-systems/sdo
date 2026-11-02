# Architecture workflow

Source: `run_architecture_task` in `sdo/agents/deployer/worker.py`, architecture
backend methods in `sdo/agents/lifecycle/agents.py`, and the Go lifecycle's
accepted source/deployment artifacts.

1. Inspect the accepted application source, deployment handoff and human goal.
2. Describe actual components, dependencies, deployment topology and the health
   surfaces relevant to operations, grounded in tracked source.
3. Return the typed architecture artifact bound to its generated-at commit;
   the trusted artifact writer/broker owns accepted publication.

Check that source/deployment evidence is attributed, topology is current and the
architecture stage does not author incident memory or change the human goal.
Keep topology data in the request/artifact and general workflow in a skill;
avoid duplicating application-specific detail in role instructions.
