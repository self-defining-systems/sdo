# App-state readers

To expose state the application itself defines (an admin setting, a database setting, process state), the detection
designer or a human writes an app-state reader: a **health** detector package that also implements `appstate.Reader`
([SDK reference](sdk/appstate-01.md)); rule `detector.reader-class` rejects one in an incident detector. It gets a
narrow read-only view (store rows, credential-free probe GETs, Kubernetes GET/HEAD, at most 8 reads per call) and returns
`appstate.Values` (prototype: no secret filtering; readers must not select secret columns). A detector reads the result with `appstate.Require` and
`DiffersFromHealthy`, `ChangedWithin` or `HeldAbove`; `ok=false` means no baseline, so return an error, never NoMatch.
App-state findings are clues and never open an incident.
