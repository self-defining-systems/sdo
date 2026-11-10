package datacheck

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"sdo.dev/operator/sdk/podread"
)

// Every read prints one line per fact and ends with "E" when it completed:
//
//	R <tab> id <tab> record JSON   a record (strings over the cap become {"$long":n},
//	                                arrays over MaxMembers {"$size":n})
//	W <tab> id <tab> type          a redis key that holds another type
//	K <tab> key                    a key for the lookup (first read only)
//	Q <tab> key                    a key the lookup was asked for
//	E                              the read completed
//	ERR <message>                  the store refused the read
//
// The reads are built here from validated names only; the designer's own
// text reaches a store solely as a validated mongodb filter (JSON) or
// postgres condition (closed grammar).
const (
	keyPrefix  = "K\t"
	chunkBytes = 900
	// valueBytes caps one scalar value in a record; a string value of a
	// string-typed key (a cached document) may be longer.
	valueBytes   = 256
	documentByte = 2048
)

// command returns the argv of one read of s. For the first read keys is the
// selected ids (or nil for first); for a lookup it is the placeholder.
// keyPaths names the field whose values the first read prints as lookup keys;
// keyIDs prints the record ids instead.
func (s Store) command(fields []string, keys []string, first int, keyPath string, keyIDs bool, lookup bool) []string {
	switch s.Engine {
	case EngineMongoDB:
		return mongoCommand(s, fields, keys, first, keyPath, keyIDs, lookup)
	case EnginePostgres:
		return postgresCommand(s, fields, keys, first, keyPath, keyIDs, lookup)
	case EngineRedis:
		return redisCommand(s, fields, keys, first, keyPath, keyIDs, lookup)
	case EngineMemcached:
		return memcachedCommand(s, keys, keyIDs, lookup)
	}
	return nil
}

func (s Store) port() int {
	if s.Port != 0 {
		return s.Port
	}
	switch s.Engine {
	case EngineMongoDB:
		return 27017
	case EnginePostgres:
		return 5432
	case EngineRedis:
		return 6379
	}
	return 11211
}

// chunk splits s into argv pieces within the pod-read argument bound; the
// read script joins them back with an empty IFS.
func chunk(s string) []string {
	var out []string
	for len(s) > chunkBytes {
		out = append(out, s[:chunkBytes])
		s = s[chunkBytes:]
	}
	return append(out, s)
}

// script splits text at the keys placeholder so the placeholder is a whole
// argument of its own.
func script(text string) []string {
	before, after, found := strings.Cut(text, podread.KeysPlaceholder)
	if !found {
		return chunk(text)
	}
	out := append(chunk(before), podread.KeysPlaceholder)
	return append(out, chunk(after)...)
}

func jsonString(s string) string {
	data, _ := json.Marshal(s)
	return string(data)
}

// mongoShell runs the shell the image carries (the legacy mongo shell or
// mongosh) against the store's own port, as the image's root user when its
// environment names one; the password never reaches the output.
const mongoShell = `IFS=;s="$*";u=${MONGO_INITDB_ROOT_USERNAME:-};p=${MONGO_INITDB_ROOT_PASSWORD:-}
[ -n "${MONGODB_ROOT_PASSWORD:-}" ]&&u=${MONGODB_ROOT_USER:-root}&&p=$MONGODB_ROOT_PASSWORD
set -- --quiet --port %d --eval "$s"
[ -n "$u" ]&&set -- "$@" -u "$u" -p "$p" --authenticationDatabase admin
m=mongosh;command -v mongo >/dev/null 2>&1&&m=mongo
$m "$@" 2>&1
`

// mongoRead is ES5 so both shells run it. S is a value's key text, J its
// capped JSON, G the values at a path ("a.b", "list[]" for each element).
// It calls find only, with a projection, a sort, a limit and maxTimeMS.
const mongoRead = `var T="\t",o=[];try{var D=db.getSiblingDB(%s),C=D.getCollection(%s),KP=%s,L=%d,M=%d;
function S(v){if(v===null||v===undefined)return null;var t=typeof v;if(t=="string")return v;if(t=="number"||t=="boolean")return String(v);var n=(v.constructor&&v.constructor.name)||"";if(/Long|Int|Decimal|Double/.test(n)){var m=String(v.toString()).match(/-?[0-9][0-9.eE+-]*/);return m?m[0]:null}if(/ObjectId/.test(n))return v.str||v.toHexString();if(v instanceof Date)return v.toISOString();return null}
function J(v,d){if(v===null||v===undefined)return"null";var t=typeof v;if(t=="string")return v.length>L?'{"$long":'+v.length+'}':JSON.stringify(v);if(t=="number")return isFinite(v)?String(v):"null";if(t=="boolean")return String(v);var n=(v.constructor&&v.constructor.name)||"";
if(/Long|Int|Decimal|Double/.test(n)||/ObjectId/.test(n)){var x=S(v);return x===null?'{"$type":"other"}':/ObjectId/.test(n)?JSON.stringify(x):x}if(v instanceof Date)return JSON.stringify(v.toISOString());
if(v instanceof Array){if(v.length>M)return'{"$size":'+v.length+'}';var a=[];for(var i=0;i<v.length;i++)a.push(J(v[i],d+1));return"["+a.join(",")+"]"}if(d>4)return'{"$deep":1}';if(t!="object"||/Binary|BinData|Timestamp|Code|Regex/.test(n))return'{"$type":"other"}';var b=[];for(var k in v)if(Object.prototype.hasOwnProperty.call(v,k)&&typeof v[k]!="function")b.push(JSON.stringify(k)+":"+J(v[k],d+1));return"{"+b.join(",")+"}"}
function G(v,p){var out=[v],s=p.split(".");for(var i=0;i<s.length;i++){var e=/\[\]$/.test(s[i]),k=e?s[i].slice(0,-2):s[i],nx=[];for(var j=0;j<out.length;j++){var x=out[j];if(x===null||typeof x!="object")continue;var y=x[k];if(y===undefined)continue;if(e){if(y instanceof Array)for(var q=0;q<y.length&&q<M;q++)nx.push(y[q])}else nx.push(y)}out=nx}return out}
function N(x){return %s?(/^-?[0-9]+$/.test(x)?NumberLong(x):Number(x)):x}
var KS=%s,F=%s,P=%s,R=[],I={},Q=[];if(KS!==null){for(var i=0;i<KS.length;i++)if(KS[i]!==""){Q.push(KS[i]);o.push("Q"+T+KS[i])}I[KP]={$in:Q.map(N)};F=Object.keys(F).length?{$and:[F,I]}:I}
var srt={};srt[KP]=1;C.find(F,P).sort(srt).limit(%d).maxTimeMS(2000).forEach(function(d){R.push(d)});
R.forEach(function(d){var id=S(G(d,KP)[0]);o.push("R"+T+(id===null?"-":id)+T+J(d,0))});
var RP=%s,RI=%t;if(RI)R.forEach(function(d){var id=S(G(d,KP)[0]);if(id!==null)o.push("K"+T+id)});
if(RP){var ls=R.map(function(d){return G(d,RP)});for(var i=0,more=true;more&&i<M;i++){more=false;ls.forEach(function(l){if(i<l.length){more=true;var x=S(l[i]);if(x!==null)o.push("K"+T+x)}})}}
o.push("E")}catch(e){o=["ERR "+String(e.message||e).split("\n")[0]]}print(o.join("\n"))
`

func mongoCommand(s Store, fields, keys []string, first int, keyPath string, keyIDs, lookup bool) []string {
	key := s.Read.Key
	if key == "" {
		key = "_id"
	}
	projection := map[string]int{}
	for _, f := range append(slices.Clone(fields), strings.SplitN(key, ".", 2)[0]) {
		projection[f] = 1
	}
	if projection["_id"] == 0 {
		projection["_id"] = 0
	}
	proj, _ := json.Marshal(projection)
	filter, _ := s.Read.FilterJSON()
	keyList := "null"
	switch {
	case lookup:
		keyList = `"` + podread.KeysPlaceholder + `".split(" ")`
	case keys != nil:
		data, _ := json.Marshal(keys)
		keyList = string(data)
	}
	limit := first
	if limit == 0 {
		limit = MaxRecords
	}
	refPath := "null"
	if keyPath != "" {
		refPath = jsonString(keyPath)
	}
	text := fmt.Sprintf(mongoRead, jsonString(s.Read.Database), jsonString(s.Read.Collection), jsonString(key), valueBytes, MaxMembers,
		strconv.FormatBool(s.Read.KeyType == "number"), keyList, filter, string(proj), limit, refPath, keyIDs)
	return append([]string{"sh", "-c", fmt.Sprintf(mongoShell, s.port()), "sdo-data-check"}, script(text)...)
}

// postgresShell runs psql over the container's own socket as the image's
// configured user, in a read-only transaction with a statement timeout. The
// query arrives split across the positional arguments.
const postgresShell = `IFS=;U=${POSTGRES_USER:-postgres};D=%s;[ -n "$D" ]||D=${POSTGRES_DB:-$U}
export PGPASSWORD="${POSTGRES_PASSWORD:-}" PGCONNECT_TIMEOUT=2 PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=2000'
o=$(psql -X -q -At -v ON_ERROR_STOP=1 -p %d -U "$U" -d "$D" -c "$*" 2>&1);c=$?
if [ $c -eq 0 ];then printf '%%s\nE\n' "$o";else printf 'ERR %%s\n' "$(printf '%%s' "$o"|head -n1)";fi
`

func quoteIdent(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = `"` + p + `"`
	}
	return strings.Join(parts, ".")
}

func postgresCommand(s Store, fields, keys []string, first int, keyPath string, keyIDs, lookup bool) []string {
	key := quoteIdent(s.Read.Key)
	var pairs []string
	columns := []string{key + " AS sdo_k"}
	for _, f := range fields {
		c := quoteIdent(f)
		columns = append(columns, c)
		pairs = append(pairs, fmt.Sprintf(`'%s',CASE WHEN length(%s::text)>%d THEN json_build_object('$long',length(%s::text)) ELSE to_json(%s) END`, f, c, valueBytes, c, c))
	}
	where := "TRUE"
	if s.Read.Where != "" {
		where = "(" + s.Read.Where + ")"
	}
	limit := first
	if limit == 0 {
		limit = MaxRecords
	}
	var q strings.Builder
	switch {
	case lookup:
		fmt.Fprintf(&q, `WITH q AS (SELECT DISTINCT unnest(string_to_array('%s',' ')) AS k), `, podread.KeysPlaceholder)
		where += fmt.Sprintf(" AND %s::text IN (SELECT k FROM q)", key)
	case keys != nil:
		where += fmt.Sprintf(" AND %s::text IN ('%s')", key, strings.Join(keys, "','"))
		q.WriteString("WITH ")
	default:
		q.WriteString("WITH ")
	}
	fmt.Fprintf(&q, `s AS (SELECT %s FROM %s WHERE %s ORDER BY 1 LIMIT %d) `, strings.Join(columns, ","), quoteIdent(s.Read.Table), where, limit)
	fmt.Fprintf(&q, `SELECT 'R'||E'\t'||coalesce(sdo_k::text,'-')||E'\t'||json_build_object(%s)::text FROM s`, strings.Join(pairs, ","))
	if lookup {
		q.WriteString(` UNION ALL SELECT 'Q'||E'\t'||k FROM q`)
	}
	if keyIDs {
		q.WriteString(` UNION ALL SELECT 'K'||E'\t'||sdo_k::text FROM s WHERE sdo_k IS NOT NULL`)
	}
	if keyPath != "" {
		column := quoteIdent(strings.TrimSuffix(keyPath, "[]"))
		if strings.HasSuffix(keyPath, "[]") {
			fmt.Fprintf(&q, ` UNION ALL (SELECT 'K'||E'\t'||v::text FROM s, unnest(%s) WITH ORDINALITY u(v,i) WHERE v IS NOT NULL AND i<=%d ORDER BY i, sdo_k)`, column, MaxMembers)
		} else {
			fmt.Fprintf(&q, ` UNION ALL SELECT 'K'||E'\t'||%s::text FROM s WHERE %s IS NOT NULL`, column, column)
		}
	}
	return append([]string{"sh", "-c", fmt.Sprintf(postgresShell, "'"+s.Read.Database+"'", s.port()), "sdo-data-check"}, script(q.String())...)
}

// redisShell runs redis-cli against the store's own port with the read as a
// Lua script; REDISCLI_AUTH or REDIS_PASSWORD supplies a password without
// printing it. The first positional argument is the key list (ARGV[1]).
const redisShell = `k=$1;shift;IFS=;a="${REDISCLI_AUTH:-${REDIS_PASSWORD:-}}";[ -n "$a" ]&&export REDISCLI_AUTH="$a"
redis-cli -p %d EVAL "$*" 0 "$k" 2>&1|tr -d '\r'
`

// redisRead is the Lua read. It calls only SCAN, TYPE, GET, HMGET, ZREVRANGE,
// ZCARD, SCARD, SMEMBERS, LRANGE and LLEN, and returns its lines as one
// string. Mode is "first" (scan the pattern, the first n ids in id order),
// "ids" or "lookup" (the ids in ARGV[1]).
const redisRead = `local T,o,pre,suf,want,mode,n,decode,fields,refpath,refids,numeric="\t",{},%s,%s,%s,%s,%d,%s,{%s},%s,%t,%t
local function esc(s) return (s:gsub('[%%c"\\]',function(c) return string.format('\\u%%04x',c:byte()) end)) end
local function js(s,cap) if #s>cap then return '{"$long":'..#s..'}' end return '"'..esc(s)..'"' end
local function idof(k) if #k>=#pre+#suf and k:sub(1,#pre)==pre and k:sub(#k-#suf+1)==suf then return k:sub(#pre+1,#k-#suf) end end
local function walk(v,path) local out={v} for seg in string.gmatch(path,"[^.]+") do local each=seg:sub(-2)=="[]" local name=each and seg:sub(1,-3) or seg local nx={} for _,x in ipairs(out) do if type(x)=="table" then local y=x[name] if y~=nil then if each then if type(y)=="table" then for i,e in ipairs(y) do if i<=%d then nx[#nx+1]=e end end end else nx[#nx+1]=y end end end end out=nx end return out end
local function text(v) if type(v)=="string" then return v elseif type(v)=="number" then if v==math.floor(v) and math.abs(v)<1e15 then return string.format("%%d",v) end return tostring(v) end end
local ids={}
if mode=="first" then local cur,seen,i="0",{},0 repeat local r=redis.call("SCAN",cur,"MATCH",pre.."*"..suf,"COUNT",500) cur=r[1] for _,k in ipairs(r[2]) do local id=idof(k) if id and not seen[id] then seen[id]=true ids[#ids+1]=id end end i=i+1 until cur=="0" or i>=20
 table.sort(ids,function(a,b) if numeric and #a~=#b then return #a<#b end return a<b end) while #ids>n do table.remove(ids) end
else for id in string.gmatch(ARGV[1] or "","[^ ]+") do ids[#ids+1]=id if mode=="lookup" then o[#o+1]="Q"..T..id end end end
local refs={}
for _,id in ipairs(ids) do local k=pre..id..suf local t=redis.call("TYPE",k) t=t.ok or t
 if t~="none" then if t~=want then o[#o+1]="W"..T..id..T..t else local rec,vals="",{}
  if t=="string" then local v=redis.call("GET",k) rec='{"value":'..js(v,%d)..'}' if refpath~="" and decode=="json" then local ok,d=pcall(cjson.decode,v) if ok then vals=walk(d,refpath) end end
  elseif t=="hash" then local b={} if #fields>0 then local r=redis.call("HMGET",k,unpack(fields)) for i,f in ipairs(fields) do if r[i] then b[#b+1]=js(f,%d)..":"..js(r[i],%d) if f==refpath then vals={r[i]} end end end end rec="{"..table.concat(b,",").."}"
  else local m,size if t=="zset" then m=redis.call("ZREVRANGE",k,0,%d) size=redis.call("ZCARD",k) elseif t=="list" then m=redis.call("LRANGE",k,0,%d) size=redis.call("LLEN",k) else size=redis.call("SCARD",k) m=size<=%d and redis.call("SMEMBERS",k) or {} table.sort(m) end
   local b={} for i,e in ipairs(m) do b[i]=js(e,%d) end rec='{"members":['..table.concat(b,",")..'],"size":'..size..'}' if refpath~="" then vals=m end end
  o[#o+1]="R"..T..id..T..rec if refids then o[#o+1]="K"..T..id end refs[#refs+1]=vals end end end
for i=1,%d do local more=false for _,l in ipairs(refs) do if l[i]~=nil then more=true local x=text(l[i]) if x then o[#o+1]="K"..T..x end end end if not more then break end end
o[#o+1]="E" return table.concat(o,"\n")
`

func luaString(s string) string { return strconv.Quote(s) }

func redisCommand(s Store, fields, keys []string, first int, keyPath string, keyIDs, lookup bool) []string {
	pre, suf, _ := strings.Cut(s.Read.Pattern, "{id}")
	mode, arg := "ids", strings.Join(keys, " ")
	switch {
	case lookup:
		mode, arg = "lookup", podread.KeysPlaceholder
	case first > 0:
		mode = "first"
	}
	quoted := make([]string, len(fields))
	for i, f := range fields {
		quoted[i] = luaString(f)
	}
	text := fmt.Sprintf(redisRead, luaString(pre), luaString(suf), luaString(s.Read.Type), luaString(mode), first, luaString(s.Read.Decode),
		strings.Join(quoted, ","), luaString(keyPath), keyIDs, s.Read.KeyType == "number",
		MaxMembers, documentByte, valueBytes, valueBytes, MaxMembers-1, MaxMembers-1, MaxMembers, valueBytes, MaxMembers)
	return append([]string{"sh", "-c", fmt.Sprintf(redisShell, s.port()), "sdo-data-check", arg}, chunk(text)...)
}

// memcachedShell asks the store for the keys over its own port with bash's
// /dev/tcp (memcached images carry no client) and prints the protocol answer,
// capped. It sends get only.
const memcachedShell = `pre=%s;suf=%s;q="";for id in $1;do q="$q $pre$id$suf";[ %d = 1 ]&&printf 'Q\t%%s\n' "$id";[ %d = 1 ]&&printf 'K\t%%s\n' "$id";done
[ -n "$q" ]||{ echo E;exit 0;}
exec 3<>/dev/tcp/127.0.0.1/%d||{ echo "ERR cannot connect";exit 0;}
printf 'get%%s\r\nquit\r\n' "$q" >&3;head -c 3200 <&3;printf '\nE\n'
`

func memcachedCommand(s Store, keys []string, keyIDs, lookup bool) []string {
	pre, suf, _ := strings.Cut(s.Read.Pattern, "{id}")
	arg := strings.Join(keys, " ")
	if lookup {
		arg = podread.KeysPlaceholder
	}
	flag := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	return []string{"bash", "-c", fmt.Sprintf(memcachedShell, "'"+pre+"'", "'"+suf+"'", flag(lookup), flag(keyIDs), s.port()), "sdo-data-check", arg}
}
