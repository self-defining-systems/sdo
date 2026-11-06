package policy

import (
	"fmt"
	"strings"
)

// ReviewedHelmVersion is the Helm release whose function map (HelmFunctions
// plus the sprig map it embeds) the classification below was reviewed
// against. TestClassificationMatchesThePinnedHelm fails when go.mod moves it.
const ReviewedHelmVersion = "v3.15.4"

// The policy is a classification, not a denylist: every name a template may
// call is either allowed (a pure function of its arguments) or forbidden with
// a reason, and a name in neither set is forbidden. TestEveryTemplateFunctionIsClassified
// fails when the pinned sprig or Helm map holds a name that is in neither set,
// so a new function must be classified before the chart policy accepts it.

// builtinFunctions are text/template's predefined functions.
var builtinFunctions = []string{
	"and", "call", "html", "index", "slice", "js", "len", "not", "or", "print", "printf", "println", "urlquery",
	"eq", "ge", "gt", "le", "lt", "ne",
}

// helmFunctions are the functions pkg/engine adds to sprig's map (funcMap in
// engine/funcs.go), minus env and expandenv, which Helm deletes.
var helmFunctions = []string{
	"toToml", "toYaml", "toYamlPretty", "fromYaml", "fromYamlArray", "toJson", "fromJson", "fromJsonArray",
	"include", "tpl", "required", "lookup",
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
toToml toYaml toYamlPretty fromYaml fromYamlArray toJson fromJson fromJsonArray include required
`)

// forbiddenFunctions are the functions whose result is not a pure function of
// the chart, values and release, or that evaluate text the static pass cannot
// see. Everything touching time is forbidden, not only the clock readers:
// date and its siblings fall back to the current time for an argument that is
// not a time or an integer, and a static check cannot rule that out.
var forbiddenFunctions = map[string]string{
	"lookup": "reads the live cluster", "getHostByName": "resolves names over the network",
	"env": "reads the process environment", "expandenv": "reads the process environment",
	"tpl": "renders text computed at render time, which the static check cannot see",

	"now": "reads the clock", "ago": "reads the clock",
	"date": "may read the clock", "dateInZone": "may read the clock", "date_in_zone": "may read the clock",
	"htmlDate": "may read the clock", "htmlDateInZone": "may read the clock",
	"dateModify": "belongs to the date family", "date_modify": "belongs to the date family",
	"mustDateModify": "belongs to the date family", "must_date_modify": "belongs to the date family",
	"toDate": "belongs to the date family", "mustToDate": "belongs to the date family",
	"unixEpoch": "belongs to the date family", "duration": "belongs to the date family",
	"durationRound": "belongs to the date family",

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

// HelmFunctions lists the names Helm's engine adds to sprig's function map.
func HelmFunctions() []string { return append([]string(nil), helmFunctions...) }

// BuiltinFunctions lists text/template's predefined functions.
func BuiltinFunctions() []string { return append([]string(nil), builtinFunctions...) }

// dynamicInclude explains why an include or template call is rejected.
func dynamicInclude(fn string) string {
	return fmt.Sprintf("%s must name a template with a string literal that is not an output file: a computed or file-path name can run a template whose forbidden functions the static check treats as conditional", fn)
}

