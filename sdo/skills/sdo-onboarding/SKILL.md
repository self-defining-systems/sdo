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
type HTTPEndpoint struct{Method string; Path string; Query Params; Form Params; JSON string; JSONParams Params; Headers map[string]string; ExpectStatus []int; ExpectBody []string; Saves map[string]string}
func DELETE(path string, query Params) *HTTPEndpoint
func GET(path string, query Params) *HTTPEndpoint
func POST(path string, query Params) *HTTPEndpoint
func PUT(path string, query Params) *HTTPEndpoint
func (e *HTTPEndpoint) Build(_ context.Context, rng *rand.Rand, state State) (Request, error)
func (e *HTTPEndpoint) Check(response Response, state State) Verdict
func (e *HTTPEndpoint) ChecksContent() bool
func (e *HTTPEndpoint) Contains(text string) *HTTPEndpoint
func (e *HTTPEndpoint) Each(path string, field string, pattern string) *HTTPEndpoint
func (e *HTTPEndpoint) ExactSet(path string, field string, want ...string) *HTTPEndpoint
func (e *HTTPEndpoint) JSONEquals(path string, want string) *HTTPEndpoint
func (e *HTTPEndpoint) MarksExistingRecords() bool
func (e *HTTPEndpoint) OrExisting(status int, text string) *HTTPEndpoint
func (e *HTTPEndpoint) ReadsExistingRecords(path string, field string, pattern string) *HTTPEndpoint
func (e *HTTPEndpoint) SaveCookie(name string, key string) *HTTPEndpoint
func (e *HTTPEndpoint) SaveJSON(path string, key string) *HTTPEndpoint
func (e *HTTPEndpoint) Status(codes ...int) *HTTPEndpoint
func (e *HTTPEndpoint) WithBasicAuth(user string, password string) *HTTPEndpoint
func (e *HTTPEndpoint) WithForm(form Params) *HTTPEndpoint
func (e *HTTPEndpoint) WithHeader(name string, value string) *HTTPEndpoint
func (e *HTTPEndpoint) WithJSON(template string, params Params) *HTTPEndpoint
// Values
const DateLayout = "2006-01-02"
const StateNow = "sdo.now"
const UniqueWidth = 16
func FromSeedPair(key string, count int, first func(index int) string, second func(index int) string) (Param, Param)
type Param interface{Value(rng *rand.Rand, state State, generated map[string]string) (string, error)}
func Const(value string) Param
func DateRange(from string, to string) Param
func DaysAfter(param string, min int, max int) Param
func FloatBetween(low float64, high float64, decimals int) Param
func FromPrevious(key string) Param
func FromSeed(count int, render func(index int) string) Param
func IntBetween(low int, high int) Param
func OneOf(values ...string) Param
func Unique(prefix string) Param
type Params map[string]Param
func (p Params) Generate(rng *rand.Rand, state State) (map[string]string, error)
type State map[string]string
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
Generator imports (+ the local generator package): `archive/zip`, `bufio`, `bytes`, `compress/gzip`, `compress/zlib`, `context`, `crypto/md5`, `crypto/sha1`, `crypto/sha256`, `encoding/base64`, `encoding/binary`, `encoding/hex`, `encoding/json`, `encoding/xml`, `errors`, `fmt`, `hash/crc32`, `io`, `maps`, `math`, `math/rand/v2`, `net/url`, `regexp`, `sdo.dev/operator/sdk/traffic`, `slices`, `sort`, `strconv`, `strings`, `time`, `unicode`, `unicode/utf8`.
Map evidence: `config`, `manifest`, `observed`. Use `config` for application source.
<!-- probe-sdk:end -->

## Finish line

Run the [checks](references/probe-design.md#checks); no healthy run is needed. After you finish, SDO installs the
export and runs its probes on schedule for 5 minutes; if one fails, this session gets one short follow-up. As an ordinary user,
check that default lists show no probe records. With ten minutes left, remove edits still red and report any
uncovered journey. Commit the export and run `onboarding journeys --plan`. Fix rejections and commit again.
