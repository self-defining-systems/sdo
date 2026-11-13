# App-state readers

To expose state the application itself defines (an admin setting, a database setting, process state), the detection
designer or a human writes an app-state reader: a **health** detector package that also implements `appstate.Reader`
([SDK reference](sdk/appstate-01.md)); rule `detector.reader-class` rejects one in an incident detector. It gets a
narrow read-only view (store rows, credential-free probe GETs, Kubernetes GET/HEAD, at most 8 reads per call) and returns
`appstate.Values`, which refuse credential-named keys. A detector reads the result with `appstate.Require` and
`DiffersFromHealthy`, `ChangedWithin` or `HeldAbove`; `ok=false` means no baseline, so return an error, never NoMatch.
App-state findings are clues and never open an incident.

The controller runs each reader on its detector manifest `interval` (at least 10 s), off the detection path, with a 5 s
deadline and at most 4 reads at once; a detector sees the last finished result. A restarted controller has no
last-healthy state until health has held for the settle window, so `DiffersFromHealthy` reports `ok=false` until then.
A reader whose read fails produces an info clue; declare keys that move on their own with `NoisyKeys() []string`.
