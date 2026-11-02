# Harness rehearsal streams

Hand-written streams for `benchmarks.sregym.assurance.rehearse_harness`, the scripted
gate every new harness passes before live rows. They hold no run output.

- `baseline.json`: learn a program on the first incident, reuse it on the second.
- `single_image_fault.json`: one incident whose responder repairs source; used for the
  restart-inside-the-injection-pause scenario and the source-repair/judge-refresh scenario.
