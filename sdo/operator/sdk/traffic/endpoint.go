package traffic

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Param generates one request parameter from the iteration's random source,
// its shared State, and the parameters of the same request generated so far.
type Param interface {
	Value(rng *rand.Rand, state State, generated map[string]string) (string, error)
}

// Params maps parameter names to generators. Parameters are generated in
// sorted name order, except that parameters derived from another parameter of
// the same request (DaysAfter) come after the one they derive from.
type Params map[string]Param

type dependentParam interface {
	dependsOn() string
}

type paramFunc func(rng *rand.Rand, state State, generated map[string]string) (string, error)

func (f paramFunc) Value(rng *rand.Rand, state State, generated map[string]string) (string, error) {
	return f(rng, state, generated)
}

// Const is a fixed value.
func Const(value string) Param {
	return paramFunc(func(*rand.Rand, State, map[string]string) (string, error) { return value, nil })
}

// OneOf picks one of values uniformly.
func OneOf(values ...string) Param {
	return paramFunc(func(rng *rand.Rand, _ State, _ map[string]string) (string, error) {
		if len(values) == 0 {
			return "", fmt.Errorf("OneOf needs at least one value")
		}
		return values[rng.IntN(len(values))], nil
	})
}

// IntBetween picks an integer in [low, high].
func IntBetween(low int, high int) Param {
	return paramFunc(func(rng *rand.Rand, _ State, _ map[string]string) (string, error) {
		if high < low {
			return "", fmt.Errorf("IntBetween(%d, %d) is empty", low, high)
		}
		return strconv.Itoa(low + rng.IntN(high-low+1)), nil
	})
}

// FloatBetween picks a decimal in [low, high] rounded to decimals places.
func FloatBetween(low float64, high float64, decimals int) Param {
	return paramFunc(func(rng *rand.Rand, _ State, _ map[string]string) (string, error) {
		if high < low || decimals < 0 || decimals > 9 {
			return "", fmt.Errorf("FloatBetween(%g, %g, %d) is invalid", low, high, decimals)
		}
		return strconv.FormatFloat(low+rng.Float64()*(high-low), 'f', decimals, 64), nil
	})
}

// DateLayout is the layout DateRange and DaysAfter read and write.
const DateLayout = "2006-01-02"

// DateRange picks a calendar date (YYYY-MM-DD) in [from, to].
func DateRange(from string, to string) Param {
	return paramFunc(func(rng *rand.Rand, _ State, _ map[string]string) (string, error) {
		start, err := time.Parse(DateLayout, from)
		if err != nil {
			return "", fmt.Errorf("DateRange start: %w", err)
		}
		end, err := time.Parse(DateLayout, to)
		if err != nil {
			return "", fmt.Errorf("DateRange end: %w", err)
		}
		days := int(end.Sub(start).Hours() / 24)
		if days < 0 {
			return "", fmt.Errorf("DateRange(%s, %s) is empty", from, to)
		}
		return start.AddDate(0, 0, rng.IntN(days+1)).Format(DateLayout), nil
	})
}

type daysAfter struct {
	param    string
	min, max int
}

// DaysAfter is a date between min and max days after another date parameter
// of the same request, such as a check-out date after a check-in date.
func DaysAfter(param string, min int, max int) Param {
	return daysAfter{param: param, min: min, max: max}
}

func (d daysAfter) dependsOn() string { return d.param }

func (d daysAfter) Value(rng *rand.Rand, _ State, generated map[string]string) (string, error) {
	base, ok := generated[d.param]
	if !ok {
		return "", fmt.Errorf("DaysAfter: parameter %q is not generated in this request", d.param)
	}
	start, err := time.Parse(DateLayout, base)
	if err != nil {
		return "", fmt.Errorf("DaysAfter(%s): %w", d.param, err)
	}
	if d.max < d.min {
		return "", fmt.Errorf("DaysAfter(%s, %d, %d) is empty", d.param, d.min, d.max)
	}
	return start.AddDate(0, 0, d.min+rng.IntN(d.max-d.min+1)).Format(DateLayout), nil
}

// FromSeed picks one of the count records the application seeds at startup
// and renders it, for example a seeded user name. Use FromSeedPair when two
// parameters must describe the same record.
func FromSeed(count int, render func(index int) string) Param {
	return paramFunc(func(rng *rand.Rand, _ State, _ map[string]string) (string, error) {
		if count < 1 || render == nil {
			return "", fmt.Errorf("FromSeed needs a positive count and a renderer")
		}
		return render(rng.IntN(count)), nil
	})
}

// FromSeedPair returns two parameters rendered from the same seeded record,
// such as a user name and its password. The record index is kept in State
// under key, so later steps can reuse it with FromPrevious.
func FromSeedPair(key string, count int, first func(index int) string, second func(index int) string) (Param, Param) {
	pick := func(rng *rand.Rand, state State) (int, error) {
		if count < 1 || first == nil || second == nil {
			return 0, fmt.Errorf("FromSeedPair needs a positive count and two renderers")
		}
		if saved, ok := state[key]; ok {
			return strconv.Atoi(saved)
		}
		index := rng.IntN(count)
		state[key] = strconv.Itoa(index)
		return index, nil
	}
	return paramFunc(func(rng *rand.Rand, state State, _ map[string]string) (string, error) {
			index, err := pick(rng, state)
			if err != nil {
				return "", err
			}
			return first(index), nil
		}), paramFunc(func(rng *rand.Rand, state State, _ map[string]string) (string, error) {
			index, err := pick(rng, state)
			if err != nil {
				return "", err
			}
			return second(index), nil
		})
}

// FromPrevious is a value an earlier step of the same iteration saved in
// State, for example with SaveJSON.
func FromPrevious(key string) Param {
	return paramFunc(func(_ *rand.Rand, state State, _ map[string]string) (string, error) {
		value, ok := state[key]
		if !ok {
			return "", fmt.Errorf("FromPrevious: no earlier step saved %q", key)
		}
		return value, nil
	})
}

// Generate renders params deterministically.
func (p Params) Generate(rng *rand.Rand, state State) (map[string]string, error) {
	if len(p) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(p))
	for name := range p {
		names = append(names, name)
	}
	sort.Strings(names)
	generated := make(map[string]string, len(p))
	pending := names
	for len(pending) > 0 {
		progressed := false
		deferred := pending[:0:0]
		for _, name := range pending {
			if dependent, ok := p[name].(dependentParam); ok {
				if _, ready := generated[dependent.dependsOn()]; !ready {
					if _, exists := p[dependent.dependsOn()]; !exists {
						return nil, fmt.Errorf("parameter %q depends on missing parameter %q", name, dependent.dependsOn())
					}
					deferred = append(deferred, name)
					continue
				}
			}
			if p[name] == nil {
				return nil, fmt.Errorf("parameter %q has no generator", name)
			}
			value, err := p[name].Value(rng, state, generated)
			if err != nil {
				return nil, fmt.Errorf("parameter %q: %w", name, err)
			}
			generated[name] = value
			progressed = true
		}
		if !progressed {
			return nil, fmt.Errorf("parameters %v depend on each other", deferred)
		}
		pending = deferred
	}
	return generated, nil
}

var placeholderPattern = regexp.MustCompile(`\{([A-Za-z0-9_.-]+)\}`)

// HTTPEndpoint is the SDK's declarative Endpoint: a method and path, generated
// query and form parameters or a JSON body, and response expectations. Build
// it with GET, POST, PUT, or DELETE and refine it with the chainable methods.
// Path segments written {key} are filled from State.
type HTTPEndpoint struct {
	Method       string
	Path         string
	Query        Params
	Form         Params
	JSON         string
	JSONParams   Params
	Headers      map[string]string
	ExpectStatus []int
	ExpectBody   []string
	Saves        map[string]string
	// existing lists the answers (status, body text) meaning the data an
	// ensure step creates is already there; see OrExisting.
	existing []existingAnswer
	// jsonChecks are the exact-content checks of JSONEquals, ExactSet and Each.
	jsonChecks []func(document any, state State) error
	// cookies maps State keys to the response cookies SaveCookie stores.
	cookies map[string]string
	// basicUser and basicPassword are the WithBasicAuth templates.
	basicUser, basicPassword string
}

type existingAnswer struct {
	status int
	text   string
}

// GET is a read endpoint with generated query parameters.
func GET(path string, query Params) *HTTPEndpoint {
	return &HTTPEndpoint{Method: http.MethodGet, Path: path, Query: query}
}

// POST sends generated query parameters; add a form body with WithForm.
func POST(path string, query Params) *HTTPEndpoint {
	return &HTTPEndpoint{Method: http.MethodPost, Path: path, Query: query}
}

// PUT sends generated query parameters; add a form body with WithForm.
func PUT(path string, query Params) *HTTPEndpoint {
	return &HTTPEndpoint{Method: http.MethodPut, Path: path, Query: query}
}

// DELETE sends generated query parameters.
func DELETE(path string, query Params) *HTTPEndpoint {
	return &HTTPEndpoint{Method: http.MethodDelete, Path: path, Query: query}
}

// WithForm adds a URL-encoded form body.
func (e *HTTPEndpoint) WithForm(form Params) *HTTPEndpoint { e.Form = form; return e }

// WithJSON adds a JSON body, for the many write APIs that take one. template
// is the JSON document; each {key} in it is replaced by the generated value of
// params[key] or, failing that, the State value an earlier step saved, escaped
// for a JSON string. Write "{key}" for a string and {key} for a number. The
// filled body must be valid JSON.
func (e *HTTPEndpoint) WithJSON(template string, params Params) *HTTPEndpoint {
	e.JSON, e.JSONParams = template, params
	return e
}

// WithHeader sets a request header. Each {key} in value is replaced by the
// State value an earlier step saved, for example a session cookie
// ("session={session}") a login step stored with SaveCookie.
func (e *HTTPEndpoint) WithHeader(name string, value string) *HTTPEndpoint {
	if e.Headers == nil {
		e.Headers = map[string]string{}
	}
	e.Headers[name] = value
	return e
}

// WithBasicAuth sends HTTP basic authentication. Each {key} in user and
// password is filled from State, for example a probe user's password put
// there ("{password}").
func (e *HTTPEndpoint) WithBasicAuth(user string, password string) *HTTPEndpoint {
	e.basicUser, e.basicPassword = user, password
	return e
}

// Status replaces the accepted statuses; by default any 2xx is accepted.
func (e *HTTPEndpoint) Status(codes ...int) *HTTPEndpoint { e.ExpectStatus = codes; return e }

// OrExisting makes the step an ensure step: a response with status whose body
// contains text means the data the step creates already exists, and the step
// passes without its Contains and SaveJSON checks. Use it for setup steps that
// create a test identity or fixture, which must pass on a fresh application
// and on one that already has the data.
func (e *HTTPEndpoint) OrExisting(status int, text string) *HTTPEndpoint {
	e.existing = append(e.existing, existingAnswer{status: status, text: text})
	return e
}

// Contains requires the response body to contain text.
func (e *HTTPEndpoint) Contains(text string) *HTTPEndpoint {
	e.ExpectBody = append(e.ExpectBody, text)
	return e
}

// JSONEquals requires the JSON value at dotted path (object keys and array
// indexes, for example "data.items.0.id") to equal want exactly. Each {key}
// in want is replaced by the State value an earlier step saved, so a read-back
// can require exactly the value the write chose.
func (e *HTTPEndpoint) JSONEquals(path string, want string) *HTTPEndpoint {
	e.jsonChecks = append(e.jsonChecks, func(document any, state State) error {
		expected, err := fillState(want, state)
		if err != nil {
			return err
		}
		value, found := jsonLookup(document, path)
		got, scalar := jsonScalar(value)
		if !found || !scalar {
			return fmt.Errorf("body has no value at %q", path)
		}
		if got != expected {
			return fmt.Errorf("%q is %q, want exactly %q", path, bounded(got), bounded(expected))
		}
		return nil
	})
	return e
}

// ExactSet requires the JSON array at dotted path ("" for the whole body) to
// hold exactly the members want, in any order: none missing, none extra, no
// duplicates. field is the dotted path of each member's value inside an
// array element ("" when the elements are the values). Each {key} in want is
// filled from State. Use it when the probe owns the data, so the healthy set
// is known exactly.
func (e *HTTPEndpoint) ExactSet(path string, field string, want ...string) *HTTPEndpoint {
	e.jsonChecks = append(e.jsonChecks, func(document any, state State) error {
		members, err := jsonMembers(document, path, field)
		if err != nil {
			return err
		}
		counts := map[string]int{}
		for _, member := range members {
			counts[member]++
		}
		var missing []string
		for _, template := range want {
			expected, err := fillState(template, state)
			if err != nil {
				return err
			}
			if counts[expected] == 0 {
				missing = append(missing, expected)
				continue
			}
			counts[expected]--
		}
		var extra []string
		for member, count := range counts {
			for ; count > 0; count-- {
				extra = append(extra, member)
			}
		}
		sort.Strings(extra)
		if len(missing) > 0 || len(extra) > 0 {
			return fmt.Errorf("%q members differ from the expected set: missing %s, unexpected %s", path, bounded(fmt.Sprint(missing)), bounded(fmt.Sprint(extra)))
		}
		return nil
	})
	return e
}

// Each requires the JSON array at dotted path ("" for the whole body) to be
// non-empty and every member's value at field ("" for the element itself) to
// match the RE2 pattern, so one malformed or dangling member fails the step.
func (e *HTTPEndpoint) Each(path string, field string, pattern string) *HTTPEndpoint {
	compiled, compileErr := regexp.Compile(pattern)
	e.jsonChecks = append(e.jsonChecks, func(document any, _ State) error {
		if compileErr != nil {
			return fmt.Errorf("Each pattern %q: %w", pattern, compileErr)
		}
		members, err := jsonMembers(document, path, field)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return fmt.Errorf("%q has no members", path)
		}
		for index, member := range members {
			if !compiled.MatchString(member) {
				return fmt.Errorf("%q member %d is %q, which does not match %q", path, index, bounded(member), pattern)
			}
		}
		return nil
	})
	return e
}

// ChecksContent reports whether the endpoint checks anything beyond the
// status: body text, saved JSON fields or cookies, or exact JSON content.
func (e *HTTPEndpoint) ChecksContent() bool {
	return len(e.ExpectBody) > 0 || len(e.Saves) > 0 || len(e.jsonChecks) > 0 || len(e.cookies) > 0
}

// SaveCookie stores the value of the response's Set-Cookie named name in
// State under key; a response without it fails. Use it on a login step of a
// test identity the scenario created, and send the session with WithHeader.
func (e *HTTPEndpoint) SaveCookie(name string, key string) *HTTPEndpoint {
	if e.cookies == nil {
		e.cookies = map[string]string{}
	}
	e.cookies[key] = name
	return e
}

// SaveJSON stores the JSON field at dotted path (for example "id" or
// "data.id") of the response in State under key; a missing field fails.
func (e *HTTPEndpoint) SaveJSON(path string, key string) *HTTPEndpoint {
	if e.Saves == nil {
		e.Saves = map[string]string{}
	}
	e.Saves[key] = path
	return e
}

// Build implements Endpoint.
func (e *HTTPEndpoint) Build(_ context.Context, rng *rand.Rand, state State) (Request, error) {
	path, err := fillPath(e.Path, state)
	if err != nil {
		return Request{}, err
	}
	query, err := e.Query.Generate(rng, state)
	if err != nil {
		return Request{}, err
	}
	request := Request{Method: e.Method, Path: path, Query: query}
	if len(e.Headers) > 0 {
		request.Headers = make(map[string]string, len(e.Headers))
		for name, value := range e.Headers {
			filled, err := fillState(value, state)
			if err != nil {
				return Request{}, fmt.Errorf("%s %s: header %s: %w", e.Method, e.Path, name, err)
			}
			request.Headers[name] = filled
		}
	}
	if e.basicUser != "" || e.basicPassword != "" {
		user, err := fillState(e.basicUser, state)
		if err != nil {
			return Request{}, fmt.Errorf("%s %s: basic auth user: %w", e.Method, e.Path, err)
		}
		password, err := fillState(e.basicPassword, state)
		if err != nil {
			return Request{}, fmt.Errorf("%s %s: basic auth password: %w", e.Method, e.Path, err)
		}
		if request.Headers == nil {
			request.Headers = map[string]string{}
		}
		request.Headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
	}
	if e.JSON != "" {
		if len(e.Form) > 0 {
			return Request{}, fmt.Errorf("%s %s: an endpoint sends a form or a JSON body, not both", e.Method, e.Path)
		}
		body, err := e.jsonBody(rng, state)
		if err != nil {
			return Request{}, err
		}
		request.Body = body
		if request.Headers == nil {
			request.Headers = map[string]string{}
		}
		request.Headers["Content-Type"] = "application/json"
	}
	if len(e.Form) > 0 {
		form, err := e.Form.Generate(rng, state)
		if err != nil {
			return Request{}, err
		}
		values := url.Values{}
		for key, value := range form {
			values.Set(key, value)
		}
		request.Body = values.Encode()
		if request.Headers == nil {
			request.Headers = map[string]string{}
		}
		request.Headers["Content-Type"] = "application/x-www-form-urlencoded"
	}
	return request, nil
}

func (e *HTTPEndpoint) jsonBody(rng *rand.Rand, state State) (string, error) {
	generated, err := e.JSONParams.Generate(rng, state)
	if err != nil {
		return "", err
	}
	var missing string
	body := placeholderPattern.ReplaceAllStringFunc(e.JSON, func(match string) string {
		key := match[1 : len(match)-1]
		value, ok := generated[key]
		if !ok {
			value, ok = state[key]
		}
		if !ok && missing == "" {
			missing = key
		}
		encoded, _ := json.Marshal(value)
		return string(encoded[1 : len(encoded)-1])
	})
	if missing != "" {
		return "", fmt.Errorf("%s %s: JSON body placeholder %q is neither a parameter nor saved by an earlier step", e.Method, e.Path, missing)
	}
	if !json.Valid([]byte(body)) {
		return "", fmt.Errorf("%s %s: filled JSON body is not valid JSON: %s", e.Method, e.Path, bounded(body))
	}
	return body, nil
}

// Check implements Endpoint.
func (e *HTTPEndpoint) Check(response Response, state State) Verdict {
	for _, existing := range e.existing {
		if response.Status == existing.status && existing.text != "" && strings.Contains(string(response.Body), existing.text) {
			return Pass()
		}
	}
	if !statusAccepted(e.ExpectStatus, response.Status) {
		return Fail("HTTP %d: observed %s", response.Status, observedExcerpt(response.Body, response.Status, ""))
	}
	body := string(response.Body)
	for _, text := range e.ExpectBody {
		if !strings.Contains(body, text) {
			return Fail("HTTP %d body lacks %q: observed %s", response.Status, text, observedExcerpt(response.Body, response.Status, text))
		}
	}
	if len(e.cookies) > 0 {
		keys := make([]string, 0, len(e.cookies))
		for key := range e.cookies {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value, ok := responseCookie(response.Header, e.cookies[key])
			if !ok {
				return Fail("HTTP %d sets no cookie %q", response.Status, e.cookies[key])
			}
			state[key] = value
		}
	}
	if len(e.Saves) == 0 && len(e.jsonChecks) == 0 {
		return Pass()
	}
	var document any
	if err := json.Unmarshal(response.Body, &document); err != nil {
		return Fail("HTTP %d body is not JSON: %s", response.Status, bodyExcerpt(response.Body, response.Status))
	}
	for _, check := range e.jsonChecks {
		if err := check(document, state); err != nil {
			return Fail("HTTP %d body: %v", response.Status, err)
		}
	}
	keys := make([]string, 0, len(e.Saves))
	for key := range e.Saves {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, ok := jsonField(document, e.Saves[key])
		if !ok {
			return Fail("HTTP %d body has no field %q", response.Status, e.Saves[key])
		}
		state[key] = value
	}
	return Pass()
}

func fillPath(path string, state State) (string, error) {
	var missing string
	filled := placeholderPattern.ReplaceAllStringFunc(path, func(match string) string {
		key := match[1 : len(match)-1]
		value, ok := state[key]
		if !ok && missing == "" {
			missing = key
		}
		return url.PathEscape(value)
	})
	if missing != "" {
		return "", fmt.Errorf("path %s: no earlier step saved %q", path, missing)
	}
	return filled, nil
}

func statusAccepted(accepted []int, status int) bool {
	if len(accepted) == 0 {
		return status >= 200 && status < 300
	}
	for _, candidate := range accepted {
		if candidate == status {
			return true
		}
	}
	return false
}

func jsonField(document any, path string) (string, bool) {
	value, found := jsonLookup(document, path)
	if !found {
		return "", false
	}
	return jsonScalar(value)
}

// jsonLookup walks a dotted path of object keys and array indexes; the empty
// path is the document itself.
func jsonLookup(document any, path string) (any, bool) {
	current := document
	if path == "" {
		return current, true
	}
	for _, part := range strings.Split(path, ".") {
		switch node := current.(type) {
		case map[string]any:
			next, ok := node[part]
			if !ok {
				return nil, false
			}
			current = next
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(node) {
				return nil, false
			}
			current = node[index]
		default:
			return nil, false
		}
	}
	return current, true
}

// jsonScalar renders a JSON value as text; null has no text.
func jsonScalar(value any) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64), true
	case bool:
		return strconv.FormatBool(value), true
	case nil:
		return "", false
	default:
		encoded, err := json.Marshal(value)
		return string(encoded), err == nil
	}
}

// jsonMembers renders the value at field of every element of the array at
// path.
func jsonMembers(document any, path string, field string) ([]string, error) {
	value, found := jsonLookup(document, path)
	array, isArray := value.([]any)
	if !found || !isArray {
		return nil, fmt.Errorf("%q is not an array", path)
	}
	members := make([]string, 0, len(array))
	for index, element := range array {
		value, found := jsonLookup(element, field)
		member, scalar := jsonScalar(value)
		if !found || !scalar {
			return nil, fmt.Errorf("%q member %d has no value at %q", path, index, field)
		}
		members = append(members, member)
	}
	return members, nil
}

func responseCookie(header http.Header, name string) (string, bool) {
	for _, line := range header.Values("Set-Cookie") {
		cookie, err := http.ParseSetCookie(line)
		if err == nil && cookie.Name == name && cookie.Value != "" {
			return cookie.Value, true
		}
	}
	return "", false
}

// fillState replaces each {key} in template with the State value an earlier
// step saved.
func fillState(template string, state State) (string, error) {
	var missing string
	filled := placeholderPattern.ReplaceAllStringFunc(template, func(match string) string {
		key := match[1 : len(match)-1]
		value, ok := state[key]
		if !ok && missing == "" {
			missing = key
		}
		return value
	})
	if missing != "" {
		return "", fmt.Errorf("no earlier step saved %q", missing)
	}
	return filled, nil
}

func bodyExcerpt(body []byte, status int) string {
	text := strings.TrimSpace(string(body))
	if text == "" {
		text = "(empty body) " + http.StatusText(status)
	}
	return bounded(text)
}
