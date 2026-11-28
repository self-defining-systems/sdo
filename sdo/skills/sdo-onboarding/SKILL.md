---
name: sdo-onboarding
description: Turn a deployed application's goal journeys into probes, an application map and a health-objective playbook.
---

# Onboarding an application

Work alone. Preserve `.sdo/goal.md`. Edit the map region of `.sdo/arch.md`, the traffic probes and the
health-objective playbook; Go owns the rest. This page and the card below are enough for the first probe; open
the [worked example](references/example.md), [probe design](references/probe-design.md) or
[map reference](../sdo-operations-map/references/map.md) only to look something up.

1. Read the manifests, README and API: how users enter, sign in and tell a result is correct. Try requests with curl.
2. Run `sdo-operator onboarding init --repo DIR --namespace NS`; fill in routes, paths and exact assertions.
3. Write one scenario and run `onboarding journeys --only ID --quick`; then the rest; then the full run.
4. If the app counts requests by route, method and status, map that metric (`onboarding check --metrics`).

`onboarding journeys --plan` sends nothing. Fix `missing-map`, `missing-probe`, then `failing` and `unchecked`.
Keep every journey ID. Dotted JSON paths name keys and array indexes (`data.items.0.id`); `""` is the whole body.

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
type HTTPEndpoint struct{...} // SDK's declarative Endpoint
func DELETE(path string, query Params) *HTTPEndpoint // sends generated query parameters
func GET(path string, query Params) *HTTPEndpoint // read endpoint with generated query parameters
func POST(path string, query Params) *HTTPEndpoint // sends generated query parameters
func PUT(path string, query Params) *HTTPEndpoint // sends generated query parameters
func (e *HTTPEndpoint) Contains(text string) *HTTPEndpoint // requires the response body to contain text
func (e *HTTPEndpoint) Each(path string, field string, pattern string) *HTTPEndpoint // requires the JSON array at dotted path
func (e *HTTPEndpoint) ExactSet(path string, field string, want ...string) *HTTPEndpoint // requires the JSON array at dotted path
func (e *HTTPEndpoint) JSONEquals(path string, want string) *HTTPEndpoint // requires the JSON value at dotted path
func (e *HTTPEndpoint) OrExisting(status int, text string) *HTTPEndpoint // makes the step an ensure step
func (e *HTTPEndpoint) Reads() *HTTPEndpoint // marks a POST, PUT or PATCH that changes nothing
func (e *HTTPEndpoint) ReadsExistingRecords(path string, field string, pattern string) *HTTPEndpoint // marks the step as one that reads records users made or the deployment seeded
func (e *HTTPEndpoint) SaveCookie(name string, key string) *HTTPEndpoint // stores the value of the response's Set-Cookie named name in State under key
func (e *HTTPEndpoint) SaveJSON(path string, key string) *HTTPEndpoint // stores the JSON field at dotted path
func (e *HTTPEndpoint) Status(codes ...int) *HTTPEndpoint // replaces the accepted statuses
func (e *HTTPEndpoint) WithBasicAuth(user string, password string) *HTTPEndpoint // sends HTTP basic authentication
func (e *HTTPEndpoint) WithForm(form Params) *HTTPEndpoint // adds a URL-encoded form body
func (e *HTTPEndpoint) WithHeader(name string, value string) *HTTPEndpoint // sets a request header
func (e *HTTPEndpoint) WithJSON(template string, params Params) *HTTPEndpoint // adds a JSON body
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

Do not soak: SDO runs the installed probes for 5 minutes after you finish and sends one follow-up if one fails.
As an ordinary user, check default lists show no probe records. With ten minutes left, remove edits still red and
report uncovered journeys. Commit, run `onboarding journeys --plan`, fix rejections and commit again.
