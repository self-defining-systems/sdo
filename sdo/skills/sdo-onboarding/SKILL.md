---
name: sdo-onboarding
description: Turn a deployed application's goal journeys into probes, an application map and a health-objective playbook.
---

# Onboarding an application

Work alone. Preserve `.sdo/goal.md`. Edit the application-map region of `.sdo/arch.md`, traffic probes and the
health-objective playbook. Go owns the inventory, generic detectors, outcomes and lifecycle provenance.

1. Read the manifests, README and API. Find how users enter, get credentials and tell a result is correct.
2. Run `sdo-operator onboarding init --repo DIR --namespace NS` in a fresh checkout. Check the selected entry,
   then fill in routes, dependencies and exact assertions. Use the [worked example](references/example.md) for
   fixture Setup, token identity and cleanup. Open the [map reference](../sdo-operations-map/references/map.md)
   for fields you need.
3. Schedule one scenario and check it with `onboarding journeys --only ID`, then work in batches.
   Once memory validates, add `--quick` for the edit loop. Follow the
   [probe design and cadence](references/probe-design.md) for coverage and request budget.
4. If the app already has request counters by route, method and status, map them and confirm with
   `onboarding check --metrics` (it supplies Prometheus by default). Record why unavailable metrics are absent.

`onboarding journeys --plan` sends nothing. Fix `missing-map`, `missing-probe`, then `failing` and `unchecked`.
Keep every journey ID. Use the budget line and estimated detection times to prioritize earlier, more used journeys.

<!-- probe-sdk:begin -->
## Probe SDK

```go
// Scenarios
type Catalog []Scenario
type FaultClass string
const (FaultUnreachable FaultClass = "unreachable"; FaultErrorStatus FaultClass = "error-status"; FaultWrongBody FaultClass = "wrong-body"; FaultSlow FaultClass = "slow")
type ProbeUser struct{Name string; As string; Token string; SSHKey string}
type Scenario struct{ID string; Description string; Target Target; DependsOn []string; SideEffect SideEffect; Marker string; Setup []Step; Steps []Step; Cleanup []Step; Detects []FaultClass; Users []ProbeUser}
type SideEffect string
const (SideEffectRead SideEffect = "read"; SideEffectIdempotentWrite SideEffect = "idempotent-write"; SideEffectWriteWithCleanup SideEffect = "write-with-cleanup")
type Step struct{Name string; Endpoint Endpoint; Target *Target}
type Target struct{Service string; Port int; Scheme string; User string}
// HTTP
type HTTPEndpoint struct{Method string; Path string; Query Params; Form Params; JSON string; JSONParams Params; Headers map[string]string; ExpectStatus []int; ExpectBody []string; Saves map[string]string} // SDK's declarative Endpoint
func DELETE(path string, query Params) *HTTPEndpoint // sends generated query parameters
func GET(path string, query Params) *HTTPEndpoint // read endpoint with generated query parameters
func POST(path string, query Params) *HTTPEndpoint // sends generated query parameters
func PUT(path string, query Params) *HTTPEndpoint // sends generated query parameters
func (e *HTTPEndpoint) Build(_ context.Context, rng *rand.Rand, state State) (Request, error) // implements Endpoint
func (e *HTTPEndpoint) Check(response Response, state State) Verdict // implements Endpoint
func (e *HTTPEndpoint) ChecksContent() bool // reports whether the endpoint checks anything beyond the status
func (e *HTTPEndpoint) Contains(text string) *HTTPEndpoint // requires the response body to contain text
func (e *HTTPEndpoint) Each(path string, field string, pattern string) *HTTPEndpoint // requires the JSON array at dotted path ("" for the whole body) to be non-empty and every member's value at field ("" for the element itself) to match the RE2 pattern, so one malformed or dangling member fails the step
func (e *HTTPEndpoint) ExactSet(path string, field string, want ...string) *HTTPEndpoint // requires the JSON array at dotted path ("" for the whole body) to hold exactly the members want, in any order
func (e *HTTPEndpoint) JSONEquals(path string, want string) *HTTPEndpoint // requires the JSON value at dotted path (object keys and array indexes, for example "data.items.0.id") to equal want exactly
func (e *HTTPEndpoint) MarksExistingRecords() bool // reports whether the step was built with ReadsExistingRecords
func (e *HTTPEndpoint) OrExisting(status int, text string) *HTTPEndpoint // makes the step an ensure step
func (e *HTTPEndpoint) Reads() *HTTPEndpoint // marks a POST, PUT or PATCH that changes nothing (a search, a sign-in)
func (e *HTTPEndpoint) ReadsExistingRecords(path string, field string, pattern string) *HTTPEndpoint // marks the step as one that reads records users made or the deployment seeded, which the probe never wrote
func (e *HTTPEndpoint) SaveCookie(name string, key string) *HTTPEndpoint // stores the value of the response's Set-Cookie named name in State under key
func (e *HTTPEndpoint) SaveJSON(path string, key string) *HTTPEndpoint // stores the JSON field at dotted path (for example "id" or "data.id") of the response in State under key
func (e *HTTPEndpoint) Status(codes ...int) *HTTPEndpoint // replaces the accepted statuses
func (e *HTTPEndpoint) WithBasicAuth(user string, password string) *HTTPEndpoint // sends HTTP basic authentication
func (e *HTTPEndpoint) WithForm(form Params) *HTTPEndpoint // adds a URL-encoded form body
func (e *HTTPEndpoint) WithHeader(name string, value string) *HTTPEndpoint // sets a request header
func (e *HTTPEndpoint) WithJSON(template string, params Params) *HTTPEndpoint // adds a JSON body, for the many write APIs that take one
// Values
const DateLayout = "2006-01-02" // layout DateRange and DaysAfter read and write
const StateNow = "sdo.now" // State key holding the start time of the iteration, RFC 3339 in UTC
const UniqueWidth = 16 // number of hex digits Unique appends
func FromSeedPair(key string, count int, first func(index int) string, second func(index int) string) (Param, Param) // returns two parameters rendered from the same seeded record, such as a user name and its password
type Param interface{Value(rng *rand.Rand, state State, generated map[string]string) (string, error)} // generates one request parameter from the iteration's random source, its shared State, and the parameters of the same request generated so far
func Const(value string) Param // fixed value
func DateRange(from string, to string) Param // picks a calendar date (YYYY-MM-DD) in [from, to]
func DaysAfter(param string, min int, max int) Param // date between min and max days after another date parameter of the same request, such as a check-out date after a check-in date
func FloatBetween(low float64, high float64, decimals int) Param // picks a decimal in [low, high] rounded to decimals places
func FromPrevious(key string) Param // value an earlier step of the same iteration saved in State, for example with SaveJSON
func FromSeed(count int, render func(index int) string) Param // picks one of the count records the application seeds at startup and renders it, for example a seeded user name
func IntBetween(low int, high int) Param // picks an integer in [low, high]
func OneOf(values ...string) Param // picks one of values uniformly
func Unique(prefix string) Param // prefix plus UniqueWidth lowercase hex digits drawn from the iteration's random source
type Params map[string]Param // maps parameter names to generators
func (p Params) Generate(rng *rand.Rand, state State) (map[string]string, error) // renders every parameter of one request to its string value
type State map[string]string // carries values between the steps of one iteration, for example an ID a create step saved for the read and cleanup steps that follow
// Custom checks and SSH
const MethodExec = "EXEC"
type Endpoint interface{Build(ctx context.Context, rng *rand.Rand, state State) (Request, error); Check(response Response, state State) Verdict}
type Request struct{Method string; Path string; Query map[string]string; Headers map[string]string; Body string}
type Response struct{Status int; Header http.Header; Body []byte}
type Verdict struct{OK bool; Reason string; Pending bool}
func Fail(format string, args ...any) Verdict
func NotYet(format string, args ...any) Verdict
func Pass() Verdict
```
Generator imports: the standard library and the SDK, except what reaches the network, files or processes or draws its own randomness: `net`, `crypto/tls`, `crypto/rand`, `math/rand`, `hash/maphash`, `mime/multipart`, `os`, `syscall`, `log`, `flag`, `io/ioutil`, `path/filepath`, `plugin`, `runtime`, `unsafe`, `embed`, `debug`, `go`, `testing`, `expvar`, `C` and below (`net/url`, `net/textproto`, `math/rand/v2` are fine; Build's rng is a math/rand/v2 `*rand.Rand`). Read the iteration's time from `state[traffic.StateNow]`, not `time.Now`.
Map evidence: `config`, `manifest`, `observed`. Use `config` for application source.
<!-- probe-sdk:end -->

## Finish line

Run the [checks](references/probe-design.md#checks). Do not soak: after you finish, SDO installs the
export and runs its probes on schedule for 5 minutes; if one fails, this session gets one short follow-up. As an ordinary user,
check that default lists show no probe records. With ten minutes left, remove edits still red and report any
uncovered journey. Commit the export and run `onboarding journeys --plan`. Fix rejections and commit again.
