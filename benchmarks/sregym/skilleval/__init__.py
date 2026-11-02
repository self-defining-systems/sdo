"""Offline reflection scenarios: replay the state at reflection start and measure what reflection writes.

A scenario is the exact state a live reflection Job started from: the responder's
Codex rollout cut before the reflection turn, the incident session directory, the
recorded reflection task and a checkout of the memory repository at the outcome
commit. The runner resumes that session inside the recorded responder image with a
chosen skills directory and records cost, validity and replay results.

Scenarios are run outputs and stay outside the repository.
"""
