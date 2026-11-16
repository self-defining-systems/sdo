# What "instrumented and detected" means for a journey

A journey is a user-facing operation. Background work is a step of the journey that triggers it, not a journey
of its own. Each declared journey is done when every item holds; the check reports one row per journey.

| | The journey has |
|---|---|
| a. mapping | Its request route values appear in the application's own metrics; every background job class it triggers (`triggers`) appears in the job metrics. A job class no journey triggers is unattributed background work: attach it to the journey whose data it affects, or exclude it with a reason (periodic maintenance). |
| b. errors | An error series per journey that counts server errors and leaves client errors out. |
| c. latency | A request-duration series per journey. |
| d. probe | A probe that runs the journey end to end and checks the result: written data reads back intact, the triggered background work finishes and its effect is visible to a user, existing records are still right. It cleans up after itself, keeps a small traffic share and leaves no failing side effects. |
| e. detection | When any component on the path breaks, a detector names this journey from a failure signal (failing requests, a failing probe step) within its horizon, and nothing raises while healthy. A broken worker makes the triggering journey go red through its probe. |
| f. map | The path lists every component the journey really depends on (breaking a component fails exactly the journeys that list it), and each component has metrics and logs pointers (`observe`). |
| g. traces | The journey's spans name each component's trace service, where the runtime allows. A note, never red. |

Write the standard as outcomes: you may change application code (middleware, metrics, tracing), the deployment,
config and memory, whichever makes each row true most simply; do not steer to overlays only. Reuse what the
application already exposes, and let the check show which cell is red. The `observe` pointers are where a
reader looks next: the metrics selector or job, the container or label the logs come from, the trace service.
