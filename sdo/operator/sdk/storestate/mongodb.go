package storestate

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// mongoSections are the sections of the mongodb read, in order. Each header
// must be present for the read to count; users and catalog may be empty.
var mongoSections = []string{"server", "params", "locks", "users", "catalog"}

// mongoSectionKinds are the item kinds each section shows, so a refused or
// truncated section leaves them unread.
var mongoSectionKinds = map[string][]ItemKind{
	"server":  {KindRole, KindSetting, KindWriteBlock},
	"params":  {KindSetting},
	"locks":   {KindLock, KindWriteBlock},
	"users":   {KindPrincipal},
	"catalog": {KindNamespace, KindIndex, KindValidator, KindSetting},
}

// mongoScript runs the shell the image carries (the legacy mongo shell, which
// starts faster, or mongosh) against the store's own port. When the
// credential prelude found a password ($P), it authenticates against admin as
// the paired user ($U, else root); the password is passed to the shell and
// never printed. The JavaScript arrives split across the positional arguments.
const mongoScript = `set -- --quiet --port %d --eval "$1$2$3$4$5$6$7$8$9"
[ -n "$P" ]&&set -- "$@" -u "${U:-root}" -p "$P" --authenticationDatabase admin
m=mongosh;command -v mongo >/dev/null 2>&1&&m=mongo
$m "$@" 2>&1
`

// mongoRead is the read itself, ES5 so both shells run it. It prints one
// "== <section>" header per section and tab-separated facts; a section whose
// command fails prints one "ERR <codeName> <message>" line instead. It reads
// command results and catalog metadata only: counts come from collection
// metadata, users from usersInfo without credentials, validators as their
// level and action, never their text. Users stop at 16 and the catalog at the
// output budget, each marked "more".
const mongoRead = `var o=[],A=db.getSiblingDB("admin"),T="\t";function N(x){return String(Number(x))}
function c(r){if(!r.ok){var e=Error(r.errmsg);e.codeName=r.codeName;throw e}return r}
function s(n,f){var l=[];o.push("== "+n);try{f(l);o=o.concat(l)}catch(e){o.push("ERR "+(e.codeName||"-")+" "+String(e.message||e).split("\n")[0])}}
function B(l,b){var u=0;for(var j=0;j<l.length;j++){u+=l[j].length+1;if(u>b){l.length=j;l.push("more");return}}}
s("server",function(l){var h=c(A.runCommand({isMaster:1}));l.push("role"+T+(h.ismaster?h.setName?"primary":"standalone":h.secondary?"secondary":h.arbiterOnly?"arbiter":"other"));var x=c(A.runCommand({serverStatus:1})).connections;l.push("conn"+T+N(x.current)+T+N(x.current+x.available));
var p=c(A.runCommand({getCmdLineOpts:1})).parsed,z=p.security||{},q=p.replication||{};l.push("set"+T+"authorization"+T+(z.authorization||"disabled"),"set"+T+"replSetName"+T+(q.replSetName||"-"),"set"+T+"storageReadOnly"+T+!!(p.storage||{}).readOnly)})
s("params",function(l){var g=c(A.runCommand({getParameter:1,notablescan:1,cursorTimeoutMillis:1,transactionLifetimeLimitSeconds:1,maxTransactionLockRequestTimeoutMillis:1}));for(var k in g)if(k!="ok")l.push("set"+T+k+T+g[k])})
s("locks",function(l){var r=c(A.runCommand({currentOp:1,waitingForLock:true}));l.push("fsync"+T+(r.fsyncLock?1:0),"waiting"+T+r.inprog.length)})
s("users",function(l){var u=c(A.runCommand({usersInfo:{forAllDBs:true}})).users;u.slice(0,16).forEach(function(u){l.push("user"+T+u.db+T+u.user+T+u.roles.map(function(r){return r.role+"@"+r.db}).sort().join(","))});if(u.length>16)l.push("more")})
s("catalog",function(l){c(A.runCommand({listDatabases:1,nameOnly:true})).databases.map(function(x){return x.name}).sort().forEach(function(n){if(n=="local"||n=="config")return;var d=db.getSiblingDB(n);l.push("db"+T+n+T+c(d.runCommand({profile:-1})).was);d.getCollectionInfos({type:"collection"}).sort(function(a,b){return a.name<b.name?-1:1}).forEach(function(i){if(i.name.indexOf("system.")==0)return;var v=i.options.validator,y=v&&Object.keys(v).length?(i.options.validationLevel||"strict")+"/"+(i.options.validationAction||"error"):"-";
l.push("coll"+T+n+T+i.name+T+N(c(d.runCommand({count:i.name})).n)+T+y);c(d.runCommand({listIndexes:i.name})).cursor.firstBatch.forEach(function(x){if(x.name=="_id_")return;var k=[];for(var f in x.key)k.push(f+":"+(typeof x.key[f]=="string"?x.key[f]:N(x.key[f])));l.push("index"+T+n+T+i.name+T+x.name+T+[k.join(",")].concat(x.unique?["unique"]:[],x.sparse?["sparse"]:[],x.partialFilterExpression?["partial"]:[],x.expireAfterSeconds!=null?["ttl="+N(x.expireAfterSeconds)]:[]).join(" "))})})});B(l,3800-o.join("\n").length)})
print(o.join("\n"))
`

type mongoAdapter struct{}

func (mongoAdapter) Engine() Engine   { return EngineMongoDB }
func (mongoAdapter) DefaultPort() int { return 27017 }

func (mongoAdapter) ReadCommand(e Endpoint) []string {
	return readCommand([]string{"MONGO", "MONGODB"}, fmt.Sprintf(mongoScript, e.Port), chunk(mongoRead, readChunkBytes)...)
}

// readChunkBytes keeps each argument of a split read within the pod-read
// argument bound.
const readChunkBytes = 1000

// chunk splits s into pieces of at most n bytes, which the read script
// concatenates back.
func chunk(s string, n int) []string {
	var out []string
	for len(s) > n {
		out = append(out, s[:n])
		s = s[n:]
	}
	return append(out, s)
}

var mongoUnreachable = regexp.MustCompile(`(?i)ECONNREFUSED|couldn't connect|connect failed|MongoNetworkError|MongoServerSelectionError|not found|timed out`)

// mongoRefusal classifies an "ERR <codeName> <message>" line or, for a read
// that never reached its script, the shell's own output.
func mongoRefusal(line string) string {
	lower := strings.ToLower(line)
	switch {
	case mongoUnreachable.MatchString(line):
		return "unreachable"
	case strings.Contains(lower, "authentication failed"), strings.Contains(line, "AuthenticationFailed"):
		return "auth-failed"
	case strings.Contains(lower, "requires authentication"):
		return "auth-required"
	case strings.Contains(line, "Unauthorized"), strings.Contains(lower, "not authorized"):
		return "no-permission"
	case strings.Contains(line, "TooManyConnections"), strings.Contains(lower, "too many open connections"):
		return "max-clients"
	case strings.Contains(line, "CommandNotFound"):
		return "unsupported"
	}
	return "refused"
}

func (mongoAdapter) Parse(stdout string) (Facts, error) {
	sections, err := splitSections("mongodb", stdout)
	if err != nil {
		// The shell refuses a wrong password before the script runs: that
		// is the store's answer to every section.
		if line := strings.TrimSpace(stdout); line != "" && mongoRefusal(line) == "auth-failed" {
			sections = map[string][]string{}
			for _, name := range mongoSections {
				sections[name] = []string{"ERR AuthenticationFailed"}
			}
		} else {
			return Facts{}, err
		}
	}
	for _, name := range mongoSections {
		if _, ok := sections[name]; !ok {
			return Facts{}, fmt.Errorf("mongodb read lacks section %q", name)
		}
	}
	var items []Item
	answered := map[string]bool{}
	unread := map[ItemKind]bool{}
	for _, name := range mongoSections {
		lines := sections[name]
		if len(lines) > 0 && strings.HasPrefix(lines[0], "ERR ") {
			class := mongoRefusal(lines[0])
			if class == "unreachable" {
				return Facts{}, fmt.Errorf("mongodb read section %s: %s", name, lines[0])
			}
			items = append(items, Item{Kind: KindRefusal, Name: name, Value: class})
			for _, k := range mongoSectionKinds[name] {
				unread[k] = true
			}
			continue
		}
		if slices.Contains(lines, "more") {
			for _, k := range mongoSectionKinds[name] {
				unread[k] = true
			}
		}
		answered[name] = true
	}
	if answered["server"] {
		for _, line := range sections["server"] {
			if !strings.HasPrefix(line, "conn\t") && len(strings.Split(line, "\t")) < 2 {
				return Facts{}, fmt.Errorf("mongodb server line is malformed: %q", line)
			}
		}
	}
	f := Facts{Engine: EngineMongoDB}
	role, readOnly := "", false
	for _, name := range mongoSections {
		if !answered[name] {
			continue
		}
		for _, line := range sections[name] {
			if line == "more" {
				continue
			}
			fields := strings.Split(line, "\t")
			switch {
			case fields[0] == "role" && len(fields) == 2:
				role = fields[1]
				items = append(items, Item{Kind: KindRole, Name: "role", Value: role})
			case fields[0] == "conn" && len(fields) == 3:
				f.Connections = gauge(fields[1], fields[2])
			case fields[0] == "set" && len(fields) == 3:
				items = append(items, Item{Kind: KindSetting, Name: fields[1], Value: fields[2]})
				readOnly = readOnly || fields[1] == "storageReadOnly" && fields[2] == "true"
			case fields[0] == "fsync" && len(fields) == 2:
				if fields[1] != "0" {
					items = append(items, Item{Kind: KindLock, Name: "fsync", Value: "held"})
				}
			case fields[0] == "waiting" && len(fields) == 2:
				if n, err := strconv.Atoi(fields[1]); err != nil {
					return Facts{}, fmt.Errorf("mongodb lock line is malformed: %q", line)
				} else if n > 0 {
					items = append(items, Item{Kind: KindLock, Name: "waiting-for-lock", Value: "active"})
				}
			case fields[0] == "user" && len(fields) == 4:
				items = append(items, Item{Kind: KindPrincipal, Scope: fields[1], Name: fields[2], Value: fields[3]})
			case fields[0] == "db" && len(fields) == 3:
				items = append(items, Item{Kind: KindSetting, Scope: fields[1], Name: "profile", Value: fields[2]})
			case fields[0] == "coll" && len(fields) == 5:
				items = append(items, Item{Kind: KindNamespace, Scope: fields[1], Name: fields[2], Value: fields[3]})
				if fields[4] != "-" {
					items = append(items, Item{Kind: KindValidator, Scope: fields[1], Name: fields[2], Value: fields[4]})
				}
			case fields[0] == "index" && len(fields) == 5:
				items = append(items, Item{Kind: KindIndex, Scope: fields[1] + "." + fields[2], Name: fields[3], Value: fields[4]})
			default:
				return Facts{}, fmt.Errorf("mongodb %s line is malformed: %q", name, line)
			}
		}
	}
	var blocks []string
	if _, locked := NewFacts(EngineMongoDB, items).Lookup(Key{Kind: KindLock, Name: "fsync"}); locked {
		blocks = append(blocks, "fsync-lock")
	}
	if role != "" && role != "standalone" && role != "primary" {
		blocks = append(blocks, "not-primary")
	}
	if readOnly {
		blocks = append(blocks, "read-only")
	}
	for _, b := range blocks {
		items = append(items, Item{Kind: KindWriteBlock, Name: b, Value: "active"})
	}
	if answered["server"] && answered["locks"] || len(blocks) > 0 {
		writable := len(blocks) == 0
		f.Writable = &writable
	}
	f.Items = NewFacts(EngineMongoDB, items).Items
	f.Unread = sortedKinds(unread)
	if err := f.Validate(); err != nil {
		return Facts{}, err
	}
	return f, nil
}

func sortedKinds(set map[ItemKind]bool) []ItemKind {
	var out []ItemKind
	for k := range set {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
