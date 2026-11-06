package policy

import (
	"fmt"
	"strings"
	"text/template"
)

// ReviewedHelmVersion is the Helm release whose function map (HelmFunctions
// plus the sprig map it embeds) the classification below was reviewed
// against. TestClassificationMatchesThePinnedHelm fails when go.mod moves it.
const ReviewedHelmVersion = "v3.15.4"

// The policy is a classification, not a denylist: every name a template may
// call is either allowed (a pure function of its arguments) or forbidden with
// a reason, and a name in neither set is forbidden. Restrict applies it at
// execution: each forbidden or unclassified function of the engine's map is
// replaced by a stub that fails the render, so the policy holds however a
// call is reached (include, tpl, helpers, branches the values select).
// TestEveryTemplateFunctionIsClassified fails when the pinned engine map holds
// a name that is in neither set, so a new function must be classified before
// the chart policy accepts it.

// builtinFunctions are text/template's predefined functions.
var builtinFunctions = []string{
	"and", "call", "html", "index", "slice", "js", "len", "not", "or", "print", "printf", "println", "urlquery",
	"eq", "ge", "gt", "le", "lt", "ne",
}

// allowedFunctions are pure functions of their arguments.
var allowedFunctions = splitNames(`
abbrev abbrevboth add add1 add1f addf adler32sum all any append atoi b32dec b32enc b64dec b64enc base biggest
buildCustomCert camelcase cat ceil chunk clean coalesce compact concat contains decryptAES deepCopy deepEqual default
derivePassword dict dig dir div divf empty ext fail first float64 floor get has hasKey hasPrefix hasSuffix hello
indent initial initials int int64 isAbs join kebabcase keys kindIs kindOf last list lower max maxf merge
mergeOverwrite min minf mod mul mulf mustAppend mustChunk mustCompact mustDeepCopy mustFirst mustFromJson mustHas
mustInitial mustLast mustMerge mustMergeOverwrite mustPrepend mustPush mustRegexFind mustRegexFindAll
mustRegexMatch mustRegexReplaceAll mustRegexReplaceAllLiteral mustRegexSplit mustRest mustReverse mustSlice
mustToJson mustToPrettyJson mustToRawJson mustUniq mustWithout nindent nospace omit osBase osClean osDir osExt
osIsAbs pick pluck plural prepend push quote regexFind regexFindAll regexMatch regexQuoteMeta regexReplaceAll
regexReplaceAllLiteral regexSplit repeat replace rest reverse round semver semverCompare seq set sha1sum
sha256sum slice snakecase sortAlpha split splitList splitn squote sub subf substr swapcase ternary title
toDecimal toJson toPrettyJson toRawJson toString toStrings trim trimAll trimPrefix trimSuffix trimall trunc tuple
typeIs typeIsLike typeOf uniq unset until untilStep untitle upper urlJoin urlParse values without wrap wrapWith
toToml toYaml fromYaml fromYamlArray toJson fromJson fromJsonArray include tpl required
duration durationRound
`)

// forbiddenFunctions are the functions whose result is not a pure function of
// the chart, values and release, or that read the cluster or environment.
// Everything that produces or formats a point in time is forbidden, not only
// the clock readers: date and its siblings fall back to the current time for
// an argument that is not a time or an integer. duration and durationRound
// stay allowed: they format a number or string, and with every time producer
// forbidden no time.Time reaches durationRound's clock-reading branch.
var forbiddenFunctions = map[string]string{
	"lookup": "reads the live cluster", "getHostByName": "resolves names over the network",
	"env": "reads the process environment", "expandenv": "reads the process environment",

	"now": "reads the clock", "ago": "reads the clock",
	"date": "may read the clock", "dateInZone": "may read the clock", "date_in_zone": "may read the clock",
	"htmlDate": "may read the clock", "htmlDateInZone": "may read the clock",
	"dateModify": "belongs to the date family", "date_modify": "belongs to the date family",
	"mustDateModify": "belongs to the date family", "must_date_modify": "belongs to the date family",
	"toDate": "belongs to the date family", "mustToDate": "belongs to the date family",
	"unixEpoch": "belongs to the date family",

	"randAlpha": "is random", "randAlphaNum": "is random", "randNumeric": "is random", "randAscii": "is random",
	"randInt": "is random", "randBytes": "is random", "uuidv4": "is random", "shuffle": "is random",
	"genCA": "is random", "genCAWithKey": "is random", "genPrivateKey": "is random",
	"genSelfSignedCert": "is random", "genSelfSignedCertWithKey": "is random",
	"genSignedCert": "is random", "genSignedCertWithKey": "is random",
	"htpasswd": "is salted randomly", "bcrypt": "is salted randomly", "encryptAES": "may use a random IV",
}

var allowed = func() map[string]bool {
	m := map[string]bool{}
	for _, n := range allowedFunctions {
		m[n] = true
	}
	for _, n := range builtinFunctions {
		m[n] = true
	}
	return m
}()

func splitNames(s string) []string { return strings.Fields(s) }

// Forbidden reports whether a template may not call name, and why.
func Forbidden(name string) (string, bool) {
	if why, ok := forbiddenFunctions[name]; ok {
		return why, true
	}
	if allowed[name] {
		return "", false
	}
	return "is not classified by the helm policy (add it to allowedFunctions or forbiddenFunctions)", true
}

// KnownFunctions is every classified name, allowed or forbidden.
func KnownFunctions() map[string]bool {
	m := map[string]bool{}
	for n := range allowed {
		m[n] = true
	}
	for n := range forbiddenFunctions {
		m[n] = true
	}
	return m
}

// BuiltinFunctions lists text/template's predefined functions.
func BuiltinFunctions() []string { return append([]string(nil), builtinFunctions...) }

// Restrict applies the classification to an engine function map: every
// forbidden or unclassified function in funcs, and every forbidden function
// the engine does not define (env and expandenv, which Helm deletes), becomes
// a stub whose call fails with a *Error naming the function.
func Restrict(funcs template.FuncMap) {
	for name := range funcs {
		if why, bad := Forbidden(name); bad {
			funcs[name] = stub(name, why)
		}
	}
	for name, why := range forbiddenFunctions {
		funcs[name] = stub(name, why)
	}
}

func stub(name, why string) func(...any) (any, error) {
	return func(...any) (any, error) {
		return nil, &Error{Rule: RuleForbiddenFunction, Detail: fmt.Sprintf("template function %q %s", name, why)}
	}
}
