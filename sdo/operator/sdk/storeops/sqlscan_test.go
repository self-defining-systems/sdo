package storeops

import (
	"math/rand"
	"slices"
	"strings"
	"testing"
)

func TestScanSQLJudgesTextOutsideQuotes(t *testing.T) {
	for name, test := range map[string]struct {
		engine, text string
		ok           bool
	}{
		"yaml document":            {"postgres", "scopes = '---\n- :api\n'", true},
		"postgres plain backslash": {"postgres", `a = 'C:\'`, true},
		"postgres E escape":        {"postgres", `a = E'it\'s; -- fine'`, true},
		"postgres E closes":        {"postgres", `a = E'x\\'; DROP TABLE t`, false},
		"postgres E continues":     {"postgres", "a = E'x'\n'\\'; DROP TABLE t --'", true},
		"postgres plain closes":    {"postgres", `a = 'x\'; DROP TABLE t --'`, false},
		"not an E prefix":          {"postgres", `a = xe'x\'; DROP TABLE t --'`, false},
		"dollar":                   {"postgres", "a = $$it's; -- /* \\ $$", true},
		"tagged dollar":            {"postgres", "a = $x$ $$ ; $x$", true},
		"open dollar":              {"postgres", "a = $x$ $$", false},
		"dollar in identifier":     {"postgres", "a = b$$; DROP TABLE t --$$", false},
		"number before dollar":     {"postgres", "a = 0x1F$$; DROP TABLE t --$$", false},
		"quoted identifier":        {"postgres", `"a;--""b" = 1`, true},
		"json operator":            {"postgres", `a = jsonb_set(a, '{k}', '"x\\y"') #- '{z}'`, true},
		"nested comment":           {"postgres", "a = 1 /* x */", false},
		"line comment":             {"postgres", "a = 1 -- x", false},
		"meta command":             {"postgres", "a = 1\n\\! sh", false},
		"open quote":               {"postgres", "a = 'x", false},
		"odd quotes":               {"postgres", "a = 'it's'", false},
		"closes the scope":         {"postgres", "TRUE) OR (TRUE", false},
		"open parenthesis":         {"postgres", "(TRUE", false},
		"parenthesis in literal":   {"postgres", "a = ')'", true},
		"NUL in literal":           {"postgres", "a = '\x00'", false},
		"line break":               {"postgres", "a = 1,\n b = 2", true},
		"mysql escape":             {"mysql", `a = 'it\'s; -- # fine'`, true},
		"mysql escape closes":      {"mysql", `a = 'x\\'; DROP TABLE t`, false},
		"mysql double quote":       {"mysql", `a = "{""k"": ""#;--""}"`, true},
		"mysql ANSI_QUOTES":        {"mysql", `id IN (SELECT 1 AS "\")); DROP TABLE victim; -- ")`, false},
		"mysql double escape":      {"mysql", `a = "x\\"`, false},
		"mysql backtick":           {"mysql", "`a;``#` = 1", true},
		"mysql backtick backslash": {"mysql", "`a\\` = 1", false},
		"mysql hash":               {"mysql", "a = 1 # x", false},
		"mysql dollar is no quote": {"mysql", "a = $$ ; $$", false},
		"postgres hash operator":   {"postgres", "a = b # 1", true},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := scanSQL("text", test.text, dialects[test.engine])
			if (err == nil) != test.ok {
				t.Fatalf("scanSQL(%q) = %v, want ok=%v", test.text, err, test.ok)
			}
		})
	}
}

var dialects = map[string]sqlDialect{"postgres": postgresDialect, "mysql": mysqlDialect}

// hazards are the pieces a value or a clause is built from.
var hazards = []string{"'", `"`, "`", "\\", "--", "/*", "*/", "#", ";", "\n", "\r", "$$", "$a$", "(", ")", "E", "e", "x", "1", " ", ":", "where", "é"}

func randomText(r *rand.Rand) string {
	var b strings.Builder
	for range r.Intn(14) {
		b.WriteString(hazards[r.Intn(len(hazards))])
	}
	return b.String()
}

// Any value, escaped as its dialect says, is accepted in a statement and a
// selector, and its content is never judged.
func TestAnyEscapedValueIsAccepted(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	double := func(s string) string { return strings.ReplaceAll(s, "'", "''") }
	escape := func(s string) string { return double(strings.ReplaceAll(s, `\`, `\\`)) }
	for range 20000 {
		value := randomText(r)
		for engine, literals := range map[string][]string{
			"postgres": {"'" + double(value) + "'", "E'" + escape(value) + "'", "$sdoq$" + value + "$sdoq$", `'` + double(value) + "'\n'" + double(value) + "'"},
			"mysql":    {"'" + escape(value) + "'", `"` + strings.ReplaceAll(strings.ReplaceAll(value, `\`, ""), `"`, `""`) + `"`},
		} {
			for _, literal := range literals {
				e, _ := For(engine)
				if err := e.ValidateStatement("note = " + literal + ", n = 2"); err != nil {
					t.Fatalf("%s %q: %v", engine, literal, err)
				}
				if err := e.ValidateScope(Scope{Collection: "t", Selector: "note = " + literal}); err != nil {
					t.Fatalf("%s %q: %v", engine, literal, err)
				}
			}
		}
	}
}

// Accepted text followed by SDO's own text leaves the scan where it was: what
// follows is still read outside quotes, parentheses and comments.
func checkClosed(t *testing.T, text string) {
	for name, d := range dialects {
		scanned, err := scanSQL("text", text, d)
		if err != nil {
			continue
		}
		if len(scanned.top) != len(text) {
			t.Fatalf("%s %q: top is %d bytes, want %d", name, text, len(scanned.top), len(text))
		}
		rest, err := scanSQL("text", text+" MARK", d)
		if err != nil || !strings.HasSuffix(rest.top, " MARK") {
			t.Fatalf("%s %q leaves its clause: %q, %v", name, text, rest.top, err)
		}
		if _, err := scanSQL("text", text+" ;", d); err == nil {
			t.Fatalf("%s %q hides the separator after it", name, text)
		}
	}
}

func TestAcceptedTextStaysInItsClause(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for range 100000 {
		checkClosed(t, randomText(r))
	}
}

func FuzzScanSQL(f *testing.F) {
	for _, seed := range []string{"a = '---\n'", `E'\''`, "$a$ $$ $a$", "`a`", "1e5$$", "'a'\n'b'"} {
		f.Add(seed)
	}
	f.Fuzz(checkClosed)
}

func TestMySQLWireKeepsCarriageReturns(t *testing.T) {
	for text, want := range map[string]string{
		"a = 'x\r\ny'":      "a = 'x\\r\ny'",
		"a = 'x\\\\\r\n'":   "a = 'x\\\\\\r\n'",
		"a = 'x\\\r\n'":     "a = 'x\\r\n'",
		"a = 1,\r\nb = 'x'": "a = 1,\r\nb = 'x'",
		"`a\r` = 1":         "`a\r` = 1",
	} {
		if got := mysqlWire(text); got != want {
			t.Errorf("mysqlWire(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestRedisWordsQuoteLikeRedisCLI(t *testing.T) {
	for statement, want := range map[string][]string{
		`HSET a b`:             {"HSET", "a", "b"},
		"SET \"a b\n\"":        {"SET", "a b\n"},
		`SET "a\n\x41\"\\"`:    {"SET", "a\nA\"\\"},
		`SET 'a "b" \'c\' \n'`: {"SET", `a "b" 'c' \n`},
		`SET ""`:               {"SET", ""},
		`SREM {"a":1}`:         {"SREM", `{"a":1}`},
		`SET "a`:               nil,
		`SET "a"b`:             nil,
	} {
		got, err := redisWords(statement)
		if (err == nil) != (want != nil) || strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("redisWords(%q) = %q, %v, want %q", statement, got, err, want)
		}
	}
}

func TestJSStringIsJavaScript(t *testing.T) {
	if got := jsString("\a\U000e0001\""); got != `"\u0007`+"\U000e0001"+`\""` {
		t.Fatalf("jsString = %q", got)
	}
}

// mysqlEnds lists where each quoted part of text ends, "..." read as a string
// (escapes) or, under ANSI_QUOTES, as an identifier.
func mysqlEnds(text string, ansi bool) []int {
	var ends []int
	for i := 0; i < len(text); i++ {
		if strings.IndexByte("'\"`", text[i]) < 0 {
			continue
		}
		end := stringEnd(text, i, text[i] == '\'' || text[i] == '"' && !ansi, false)
		ends = append(ends, end)
		if end < 0 {
			break
		}
		i = end - 1
	}
	return ends
}

func TestAcceptedMySQLTextReadsTheSameUnderAnsiQuotes(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	for range 100000 {
		text := randomText(r)
		if _, err := scanSQL("text", text, mysqlDialect); err != nil {
			continue
		}
		if plain, ansi := mysqlEnds(text, false), mysqlEnds(text, true); !slices.Equal(plain, ansi) {
			t.Fatalf("%q reads %v, under ANSI_QUOTES %v", text, plain, ansi)
		}
	}
}
