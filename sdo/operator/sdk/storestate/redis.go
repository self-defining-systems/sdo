package storestate

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// redisSections are the sections of the redis read, in order. Each must be
// present for the read to count.
var redisSections = []string{"info", "dbsize", "role", "config", "acl"}

// redisSettings are the CONFIG GET keys the read records. requirepass is read
// only as set or empty.
var redisSettings = []string{
	"maxmemory", "maxmemory-policy", "maxclients", "min-replicas-to-write", "min-replicas-max-lag",
	"replica-read-only", "stop-writes-on-bgsave-error", "appendonly",
}

// redisScript prints one "== <section>" header per section. redis-cli runs in
// the store's container against its own port; the password the credential
// prelude found ($P) reaches it through REDISCLI_AUTH without being printed.
// INFO is filtered to the fields parsed, requirepass is reduced to set or
// empty, and ACL hashes and passwords are stripped, so the output carries no
// secret.
const redisScript = `A=$1;Z=$2;shift 2;[ -n "$P" ]&&export REDISCLI_AUTH="$P"
r(){ redis-cli -p %d "$@" 2>&1|tr -d '\r';}
echo '== info';r INFO|grep -E '^(role|master_link_status|connected_slaves|min_slaves_good_slaves|connected_clients|maxclients|paused_[a-z_]+|used_memory|maxmemory|maxmemory_policy|rdb_last_bgsave_status|aof_last_write_status|loading|db[0-9]+):|^[A-Z(]|^Could'
echo '== dbsize';r DBSIZE|head -n1
echo '== role';r ROLE|head -n1
echo '== config';for k in %s;do r CONFIG GET $k|head -n2;done
o=$(r CONFIG GET requirepass);case "$o" in requirepass*)[ -n "$(printf %%s "$o"|sed -n 2p)" ]&&v=set||v=empty;printf 'requirepass\n%%s\n' $v;;*)printf '%%s\n' "$o"|head -n1;;esac
echo '== acl';r ACL LIST|sed -E 's/ [#<>][^ ]*//g'|head -n32
echo '== holders';eval "$A"
echo '== keys';(IFS=;r EVAL "$*" 0)|head -n 26
`

// redisSectionKinds are the item kinds each section shows, so a refused
// section leaves them unread.
var redisSectionKinds = map[string][]ItemKind{
	"info":   {KindRole, KindNamespace, KindWriteBlock},
	"dbsize": {KindNamespace},
	"role":   {KindRole},
	"config": {KindSetting, KindPrincipal, KindWriteBlock},
	"acl":    {KindPrincipal},
}

type redisAdapter struct{}

func (redisAdapter) Engine() Engine   { return EngineRedis }
func (redisAdapter) DefaultPort() int { return 6379 }

func (redisAdapter) ReadCommand(e Endpoint) []string {
	args := append([]string{redisHoldersSnippet, tcpHoldersSnippet(e.Port)}, chunk(redisKeysLua, readChunkBytes)...)
	return readCommand([]string{"REDISCLI", "REDIS"}, fmt.Sprintf(redisScript, e.Port, strings.Join(redisSettings, " ")), args...)
}

// redisKeysLua counts the keys of database 0 per key pattern, scanning at
// most 20000 keys. A key's pattern is its first ':'-separated segment
// ("{n}" for digits, "*" for a long or unusual one), then ":{n}" or ":*" for
// a second segment and ":*" for any more: "123" is "{n}", "123:followers"
// "{n}:*", "user:42:cart" "user:{n}:*". It prints "complete" or "partial",
// then "<pattern>\t<count>" for the 24 largest patterns and "more" when
// there are others. Key names never leave the store beyond their pattern.
const redisKeysLua = `local seen, counts, cur, it = {}, {}, '0', 0
local function seg(x) if string.match(x, '^%d+$') then return '{n}' end if #x <= 32 and string.match(x, '^[%w_.-]*$') and not string.find(x, '%x%x%x%x%x%x%x%x') then return x end return '*' end
repeat
  local r = redis.call('SCAN', cur, 'COUNT', 1000)
  cur = r[1]
  for _, k in ipairs(r[2]) do
    if not seen[k] then
      seen[k] = true
      local a, b, rest = string.match(k, '^([^:]*):?([^:]*):?(.*)$')
      local p = seg(a)
      if string.find(k, ':', 1, true) then
        if string.match(b, '^%d+$') then p = p .. ':{n}' else p = p .. ':*' end
        if rest ~= '' or select(2, string.gsub(k, ':', '')) > 1 then p = p .. ':*' end
      end
      counts[p] = (counts[p] or 0) + 1
    end
  end
  it = it + 1
until cur == '0' or it >= 20
local list = {}
for p, n in pairs(counts) do list[#list + 1] = {p, n} end
table.sort(list, function(x, y) if x[2] ~= y[2] then return x[2] > y[2] end return x[1] < y[1] end)
local out = {cur == '0' and 'complete' or 'partial'}
for i, e in ipairs(list) do if i > 24 then out[#out + 1] = 'more' break end out[#out + 1] = e[1] .. '\t' .. e[2] end
return out
`

var refusalLine = regexp.MustCompile(`^(\(error\) )?([A-Z][A-Z]+)\b`)

// refusal classifies a section's first line. unreachable means the CLI reached
// no server, so the read is unavailable rather than a fact.
func refusal(line string) (class string, refused bool) {
	switch {
	case strings.HasPrefix(line, "Could not connect"), strings.HasPrefix(line, "Error:"):
		return "unreachable", true
	case strings.HasPrefix(line, "AUTH failed"):
		return "auth-failed", true
	}
	m := refusalLine.FindStringSubmatch(line)
	if m == nil {
		return "", false
	}
	switch {
	case m[2] == "NOAUTH":
		return "auth-required", true
	case m[2] == "WRONGPASS":
		return "auth-failed", true
	case m[2] == "NOPERM":
		return "no-permission", true
	case m[2] == "LOADING":
		return "loading", true
	case strings.Contains(line, "max number of clients"):
		return "max-clients", true
	case strings.Contains(strings.ToLower(line), "unknown command"):
		return "unsupported", true
	}
	return "refused", true
}

// splitSections splits a read into its "== <name>" sections. Lines before the
// first header, blank lines and "#" comments are dropped. A read without any
// header reached no store and is an error.
func splitSections(engine, stdout string) (map[string][]string, error) {
	sections := map[string][]string{}
	current := ""
	for _, raw := range strings.Split(stdout, "\n") {
		line := strings.TrimRight(raw, "\r")
		if engine == "redis" {
			line = strings.TrimSpace(line)
		}
		if name, ok := strings.CutPrefix(line, "== "); ok {
			if _, dup := sections[name]; dup {
				return nil, fmt.Errorf("%s read repeats section %q", engine, name)
			}
			current = name
			sections[name] = nil
			continue
		}
		if current == "" || strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		sections[current] = append(sections[current], line)
	}
	if len(sections) == 0 {
		return nil, fmt.Errorf("%s read has no sections: %q", engine, firstLine(stdout))
	}
	return sections, nil
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	if len(line) > 200 {
		line = line[:200]
	}
	return line
}

func (redisAdapter) Parse(stdout string) (Facts, error) {
	sections, err := splitSections("redis", stdout)
	if err != nil {
		return Facts{}, err
	}
	for _, name := range redisSections {
		lines, ok := sections[name]
		if !ok {
			return Facts{}, fmt.Errorf("redis read lacks section %q", name)
		}
		if len(lines) == 0 {
			return Facts{}, fmt.Errorf("redis read section %q is empty", name)
		}
	}
	var items []Item
	answered := map[string]bool{}
	for _, name := range redisSections {
		class, refused := refusal(sections[name][0])
		if class == "unreachable" {
			return Facts{}, fmt.Errorf("redis read section %s: %s", name, sections[name][0])
		}
		if refused {
			items = append(items, Item{Kind: KindRefusal, Name: name, Value: class})
			continue
		}
		answered[name] = true
	}
	info := map[string]string{}
	if answered["info"] {
		for _, line := range sections["info"] {
			if key, value, ok := strings.Cut(line, ":"); ok {
				info[key] = value
			}
		}
	}
	config := map[string]string{}
	if answered["config"] {
		lines := sections["config"]
		for i := 0; i+1 < len(lines); i += 2 {
			config[lines[i]] = lines[i+1]
		}
	}
	for key, value := range config {
		if key == "requirepass" {
			items = append(items, Item{Kind: KindPrincipal, Name: key, Value: value})
			continue
		}
		items = append(items, Item{Kind: KindSetting, Name: key, Value: value})
	}
	if answered["acl"] {
		for _, line := range sections["acl"] {
			fields := strings.Fields(line)
			if len(fields) < 2 || fields[0] != "user" {
				return Facts{}, fmt.Errorf("redis ACL line is not a user: %q", line)
			}
			var rules []string
			for _, rule := range fields[2:] {
				if !strings.ContainsAny(rule[:1], "#<>") {
					rules = append(rules, rule)
				}
			}
			items = append(items, Item{Kind: KindPrincipal, Name: fields[1], Value: strings.Join(rules, " ")})
		}
	}
	if answered["dbsize"] {
		items = append(items, Item{Kind: KindNamespace, Name: "dbsize", Value: sections["dbsize"][0]})
	}
	role := info["role"]
	if role == "" && answered["role"] {
		role = sections["role"][0]
	}
	if role != "" {
		items = append(items, Item{Kind: KindRole, Name: "role", Value: role})
	}
	if link := info["master_link_status"]; link != "" {
		items = append(items, Item{Kind: KindRole, Name: "master-link", Value: link})
	}
	for key, value := range info {
		if !strings.HasPrefix(key, "db") {
			continue
		}
		for _, field := range strings.Split(value, ",") {
			if k, v, ok := strings.Cut(field, "="); ok && k == "keys" {
				items = append(items, Item{Kind: KindNamespace, Scope: key, Name: "keys", Value: v})
			}
		}
	}

	keyItems, keysRead := redisKeyPatterns(sections["keys"])
	items = append(items, keyItems...)

	f := Facts{Engine: EngineRedis}
	holders, present := sections["holders"]
	if f.Holders, err = parseHolders(holders, present); err != nil {
		return Facts{}, fmt.Errorf("redis read: %w", err)
	}
	if answered["info"] {
		f.Connections = gauge(info["connected_clients"], first(info["maxclients"], config["maxclients"]))
		f.Memory = gauge(info["used_memory"], first(info["maxmemory"], config["maxmemory"]))
	}
	blocks := redisWriteBlocks(role, info, config, f.Memory)
	for _, b := range blocks {
		items = append(items, Item{Kind: KindWriteBlock, Name: b, Value: "active"})
	}
	if answered["info"] && answered["config"] || len(blocks) > 0 {
		writable := len(blocks) == 0
		f.Writable = &writable
	}
	built := NewFacts(EngineRedis, items)
	f.Items = built.Items
	unread := map[ItemKind]bool{}
	for section, kinds := range redisSectionKinds {
		if !answered[section] && (section != "role" || !answered["info"]) {
			for _, k := range kinds {
				unread[k] = true
			}
		}
	}
	if !keysRead {
		unread[KindKeyPattern] = true
	}
	for k := range unread {
		f.Unread = append(f.Unread, k)
	}
	slices.Sort(f.Unread)
	if err := f.Validate(); err != nil {
		return Facts{}, err
	}
	return f, nil
}

// redisWriteBlocks lists the observable reasons a redis server refuses writes.
func redisWriteBlocks(role string, info, config map[string]string, memory *Gauge) []string {
	var blocks []string
	if role == "slave" && config["replica-read-only"] != "no" {
		blocks = append(blocks, "replica-read-only")
	}
	if need, _ := strconv.Atoi(config["min-replicas-to-write"]); need > 0 && role != "slave" {
		good, err := strconv.Atoi(first(info["min_slaves_good_slaves"], info["connected_slaves"]))
		if err != nil || good < need {
			blocks = append(blocks, "min-replicas")
		}
	}
	policy := first(info["maxmemory_policy"], config["maxmemory-policy"])
	if memory != nil && memory.Max > 0 && memory.Used >= memory.Max && policy == "noeviction" {
		blocks = append(blocks, "oom")
	}
	if info["rdb_last_bgsave_status"] == "err" && config["stop-writes-on-bgsave-error"] != "no" {
		blocks = append(blocks, "bgsave-error")
	}
	if info["aof_last_write_status"] == "err" {
		blocks = append(blocks, "aof-error")
	}
	if info["loading"] == "1" {
		blocks = append(blocks, "loading")
	}
	for key, value := range info {
		if strings.HasPrefix(key, "paused_") && value != "" && value != "0" && value != "none" {
			blocks = append(blocks, "client-pause")
			break
		}
	}
	return blocks
}

func first(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func gauge(used, limit string) *Gauge {
	u, err := strconv.ParseInt(used, 10, 64)
	if err != nil {
		return nil
	}
	m, err := strconv.ParseInt(limit, 10, 64)
	if err != nil {
		return nil
	}
	return &Gauge{Used: u, Max: m}
}

// redisKeyPatterns reads the optional keys section: per-pattern key counts of
// database 0. It is optional so readings taken before it existed still parse,
// and a refused or partial scan leaves key patterns unread rather than
// failing the read or raising a refusal.
func redisKeyPatterns(lines []string) ([]Item, bool) {
	if len(lines) == 0 || lines[0] != "complete" {
		return nil, false
	}
	var items []Item
	complete := true
	for _, line := range lines[1:] {
		if line == "more" {
			complete = false
			continue
		}
		pattern, count, ok := strings.Cut(line, "\t")
		if !ok || pattern == "" {
			return nil, false
		}
		items = append(items, Item{Kind: KindKeyPattern, Scope: "db0", Name: pattern, Value: count})
	}
	return items, complete
}
