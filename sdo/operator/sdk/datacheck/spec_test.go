package datacheck_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"slices"
	"strings"
	"testing"
	"testing/quick"

	"sdo.dev/operator/sdk/datacheck"
	"sdo.dev/operator/sdk/podread"
)

const rowsShape = `component: order-db
engine: postgres
target: {labels: {app: order-db}}
read: {table: public.orders, key: id, where: "status <> 'void' AND total >= 0", first: 8}
assert:
  - shape: {id: integer, total: number}
`

func validate(t *testing.T, id, source string) error {
	t.Helper()
	check, err := datacheck.Parse([]byte(source))
	if err != nil {
		return err
	}
	return check.Validate(id)
}

func TestValidChecksOfEveryEngine(t *testing.T) {
	for id, source := range map[string]string{"items": itemShape, "groups": groupReference, "members": memberEquivalence, "orders": rowsShape} {
		if err := validate(t, id, source); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
}

func TestValidationRefusesWritesUnboundedReadsAndDanglingAssertions(t *testing.T) {
	for name, tc := range map[string]struct{ source, want string }{
		"unknown field":         {itemShape + "command: [rm]\n", "field command not found"},
		"code in a filter":      {strings.Replace(itemShape, "key_type: number,", `key_type: number, filter: {$where: "sleep(1)"},`, 1), "operator $where is not allowed"},
		"expr in a filter":      {strings.Replace(itemShape, "key_type: number,", `key_type: number, filter: {$expr: {$function: {}}},`, 1), "operator $expr is not allowed"},
		"no selection":          {strings.Replace(itemShape, ", first: 4", "", 1), "exactly one of ids, range or first"},
		"two selections":        {strings.Replace(itemShape, "first: 4", "first: 4, ids: ['1']", 1), "exactly one of ids, range or first"},
		"unbounded sample":      {strings.Replace(itemShape, "first: 4", "first: 33", 1), "first must be 1 to 32"},
		"wide range":            {strings.Replace(itemShape, "first: 4", "range: {from: 1, to: 40}", 1), "range must be"},
		"unsafe id":             {strings.Replace(itemShape, "first: 4", `ids: ["1'; drop"]`, 1), "is not a key"},
		"no assertion":          {strings.Split(itemShape, "assert:")[0] + "assert: []\n", "assert lists 1 to"},
		"two kinds in one":      {itemShape + "  - {shape: {a: string}, bounds: {field: a, min: 1}}\n", "exactly one of shape"},
		"unknown type":          {strings.Replace(itemShape, "title: string", "title: text", 1), `type "text" of title`},
		"bad path":              {strings.Replace(itemShape, "title: string", `"title..x": string`, 1), "is not a field path"},
		"reference, no lookup":  {itemShape + "  - reference: {field: owner.user_id}\n", "declare lookup"},
		"lookup, no assertion":  {strings.Replace(groupReference, "  - reference: {field: \"members[]\"}\n", "", 1), "lookup is read only by"},
		"lookup selects":        {strings.Replace(groupReference, "key_type: number}", "key_type: number, first: 3}", 1), "takes its keys from the checked records"},
		"set against value":     {strings.Replace(memberEquivalence, `source: "members[].user_id"`, `source: "owner_id"`, 1), "compare a set with a single value"},
		"redis field":           {strings.Replace(groupReference, "{\"members[]\": digits}", "{title: string}", 1), "members, members[] and size only"},
		"pattern without id":    {strings.Replace(groupReference, `"group:{id}"`, `"group:*"`, 1), "must hold {id} once"},
		"memcached listing":     {"component: c\nengine: memcached\ntarget: {image: memcached}\nread: {pattern: \"{id}\", first: 2}\nassert: [{shape: {value: string}}]\n", "memcached cannot list its keys"},
		"memcached reference":   {"component: c\nengine: memcached\ntarget: {image: memcached}\nread: {pattern: \"{id}\", decode: json, ids: [\"1\"]}\nlookup: {component: d, engine: redis, target: {image: redis}, read: {pattern: \"{id}\", type: string}}\nassert: [{reference: {field: a}}]\n", "cannot be decoded in its container"},
		"no target":             {strings.Replace(itemShape, "target: {labels: {app: item-store}}", "target: {}", 1), "target needs labels or an image"},
		"unknown engine":        {strings.Replace(itemShape, "engine: mongodb", "engine: cassandra", 1), `engine "cassandra"`},
		"short interval":        {itemShape + "interval: 10s\n", "interval"},
		"function in where":     {strings.Replace(rowsShape, "total >= 0", "pg_sleep(10) IS NULL", 1), "function calls"},
		"subquery in where":     {strings.Replace(rowsShape, "total >= 0", "total IN (SELECT 1)", 1), "expected a column or a literal"},
		"statement in where":    {strings.Replace(rowsShape, "total >= 0", "total >= 0; DELETE FROM orders", 1), "semicolons"},
		"union in where":        {strings.Replace(rowsShape, "total >= 0", "total >= 0 UNION SELECT 1", 1), "unexpected"},
		"comment in where":      {strings.Replace(rowsShape, "total >= 0", "total >= 0 -- x", 1), "comments"},
		"cast in where":         {strings.Replace(rowsShape, "total >= 0", "total::text = '1'", 1), "casts"},
		"escape string":         {strings.Replace(rowsShape, "total >= 0", `status = E'\\x'`, 1), "backslashes"},
		"nested column":         {strings.Replace(rowsShape, "total: number", "total.cents: number", 1), "columns, not total.cents"},
		"lookup reads itself":   {strings.Replace(groupReference, "component: item-store\n  engine", "component: group-cache\n  engine", 1), "reads the checked component"},
		"bad id":                {itemShape, "id"},
		"text value field path": {"component: c\nengine: memcached\ntarget: {image: memcached}\nread: {pattern: \"{id}\", ids: [\"1\"]}\nassert: [{shape: {a: string}}]\n", "decode: json reads fields"},
	} {
		t.Run(name, func(t *testing.T) {
			id := "check"
			if name == "bad id" {
				id = "Bad_ID"
			}
			err := validate(t, id, tc.source)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v, want %q", err, tc.want)
			}
		})
	}
}

// readCommands lists the store commands a generated read can issue. The
// reads are built from fixed templates, so these are the only ones.
var (
	redisCall   = regexp.MustCompile(`redis\.call\("([A-Z]+)"`)
	redisRead   = []string{"GET", "HMGET", "LLEN", "LRANGE", "SCAN", "SCARD", "SMEMBERS", "TYPE", "ZCARD", "ZREVRANGE"}
	mongoWrites = regexp.MustCompile(`(?i)\b(insert|update|delete|remove|drop|replace|findAndModify|aggregate|mapReduce|runCommand|eval|createIndex|rename)\w*\(`)
)

func readsOf(t *testing.T, id, source string) []podread.Query {
	t.Helper()
	check, err := datacheck.Parse([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	reads := check.Reads(id)
	if err := podread.ValidateQueries(reads); err != nil {
		t.Fatalf("reads of %s are not valid pod reads: %v", id, err)
	}
	return reads
}

func TestReadsUseOnlyReadOnlyStoreCommands(t *testing.T) {
	for id, source := range map[string]string{"items": itemShape, "groups": groupReference, "members": memberEquivalence, "orders": rowsShape} {
		for _, q := range readsOf(t, id, source) {
			text := strings.Join(q.Args, "")
			switch {
			case strings.Contains(text, "redis-cli"):
				for _, m := range redisCall.FindAllStringSubmatch(text, -1) {
					if !slices.Contains(redisRead, m[1]) {
						t.Fatalf("%s issues redis %s", q.ID, m[1])
					}
				}
				if strings.Contains(text, "redis.pcall") {
					t.Fatalf("%s uses pcall", q.ID)
				}
			case strings.Contains(text, "mongosh"):
				if m := mongoWrites.FindString(text); m != "" {
					t.Fatalf("%s calls %s", q.ID, m)
				}
				if !strings.Contains(text, ".find(F,P)") || !strings.Contains(text, "maxTimeMS(") {
					t.Fatalf("%s is not a bounded find", q.ID)
				}
			case strings.Contains(text, "psql"):
				if !strings.Contains(text, "default_transaction_read_only=on") || !strings.Contains(text, "statement_timeout=") {
					t.Fatalf("%s does not run read-only with a timeout", q.ID)
				}
			default:
				t.Fatalf("%s runs an unknown client", q.ID)
			}
			if !strings.Contains(text, "LIMIT") && !strings.Contains(text, "limit(") && !strings.Contains(text, "while #ids>n") && !strings.Contains(text, "ARGV[1]") {
				t.Fatalf("%s reads without a record limit", q.ID)
			}
		}
	}
}

func TestLookupFollowsTheCheckedRead(t *testing.T) {
	reads := readsOf(t, "groups", groupReference)
	if len(reads) != 2 || reads[0].ID != datacheck.QueryID("groups") || reads[1].Keys == nil || reads[1].Keys.From != reads[0].ID {
		t.Fatalf("reads = %+v", reads)
	}
	if reads[0].Interval != datacheck.MinInterval || !slices.Contains(reads[1].Args, podread.KeysPlaceholder) {
		t.Fatalf("lookup does not take the placeholder as its own argument: %q", reads[1].Args)
	}
}

// Property: a sample whose records all carry the shape clears, and removing
// one required field from k distinct records yields exactly those k records.
func TestShapeViolationsAreExactlyTheDamagedRecords(t *testing.T) {
	d := detector(t, "items", itemShape)
	property := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		n := 1 + rng.Intn(datacheck.MaxRecords)
		damaged := map[string]bool{}
		var b strings.Builder
		for i := range n {
			doc := map[string]any{"item_id": i, "owner": map[string]any{"user_id": rng.Intn(1000)}, "title": fmt.Sprint("t", rng.Int()), "tags": []any{"a"}, "stock": rng.Intn(5)}
			if rng.Intn(3) == 0 {
				field := []string{"title", "owner", "item_id"}[rng.Intn(3)]
				delete(doc, field)
				damaged[fmt.Sprint(i)] = true
			}
			data, _ := json.Marshal(doc)
			fmt.Fprintf(&b, "R\t%d\t%s\n", i, data)
		}
		b.WriteString("E\n")
		findings, err := d.Detect(context.Background(), view(t, d, b.String()))
		if err != nil {
			return false
		}
		if len(damaged) == 0 {
			return len(findings) == 0
		}
		if len(findings) != 1 || findings[0].RuleID != datacheck.RuleShape {
			return false
		}
		got, _ := findings[0].Metadata["records"].([]string)
		if len(got) != len(damaged) {
			return false
		}
		for _, id := range got {
			if !damaged[id] {
				return false
			}
		}
		return true
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 300}); err != nil {
		t.Fatal(err)
	}
}
