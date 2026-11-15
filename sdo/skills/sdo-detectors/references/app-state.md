# App-state readers

To expose state the application itself defines (an admin setting, a database setting, process state), the detection
onboarding agent or a human writes an app-state reader: a **health** detector package that also implements `appstate.Reader`
([SDK reference](sdk/appstate-01.md)); rule `detector.reader-class` rejects one in an incident detector. It gets a
narrow read-only view (store rows, credential-free probe GETs, Kubernetes GET/HEAD, at most 8 reads per call) and returns
`appstate.Values` (prototype: no secret filtering; readers must not select secret columns). A detector reads the result with `appstate.Require` and
`DiffersFromHealthy`, `ChangedWithin` or `HeldAbove`; `ok=false` means no baseline, so return an error, never NoMatch.
App-state findings are clues and never open an incident.

The controller runs each reader on its detector manifest `interval` (at least 10 s), off the detection path, with a 5 s
deadline and at most 4 reads at once; a detector sees the last finished result. A restarted controller has no
last-healthy state until health has held for the settle window, so `DiffersFromHealthy` reports `ok=false` until then.
A reader whose read fails produces an info clue; declare keys that move on their own with `NoisyKeys() []string`.

A reader may also declare pod reads (`PodReads()`, read back with `podread.Require` in `ReadState`). The controller
collects them for that reader alone, just before each read, on their own budget: they never share the detection cycle's
`podread.MaxQueries` and are capped at that many per reader. A reader with pod reads runs at most every 30 s. When they
cannot run, the reader's error says why (`skipped: ...`, or the failing Pod's own reading error). The controller log
shows `app_state_readers_configured` at load and one `app_state_reader` line per reader on its first result and on each
change from ok to an error (or to another error), so every reader's state is visible outside an incident.
Each read runs against the controller's current cluster snapshot, published to the host first, so a reader never waits for a detection cycle. A failed reader is retried after 10 s, doubling up to its interval. A reader with no result 90 s after load is logged as `app_state_reader_stalled` with the reason.
