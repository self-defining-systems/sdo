package storeops

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// redisPrelude exports a password for redis-cli without printing it and
// defines r as redis-cli against the store's own port and database.
// Positional arguments: $1 port, $2 database; they are shifted away.
const redisPrelude = `a="${REDISCLI_AUTH:-${REDIS_PASSWORD:-}}";[ -n "$a" ]&&export REDISCLI_AUTH="$a"
P="$1";D="$2";shift 2
r(){ redis-cli -p "$P" -n "$D" --raw "$@";}
`

// redisScan collects the distinct keys matching ARGV[1] into keys, failing
// once more than lim match.
const redisScan = `local seen, keys, cur = {}, {}, '0'
repeat
  local res = redis.call('SCAN', cur, 'MATCH', ARGV[1], 'COUNT', 1000)
  cur = res[1]
  for _, k in ipairs(res[2]) do
    if not seen[k] then
      seen[k] = true
      keys[#keys + 1] = k
      if #keys > lim then return redis.error_reply('SDO_LIMIT ' .. lim) end
    end
  end
until cur == '0'
`

// redisCountLua counts keys matching ARGV[1].
const redisCountLua = `local seen, n, cur = {}, 0, '0'
repeat
  local res = redis.call('SCAN', cur, 'MATCH', ARGV[1], 'COUNT', 1000)
  cur = res[1]
  for _, k in ipairs(res[2]) do
    if not seen[k] then seen[k] = true; n = n + 1 end
  end
until cur == '0'
return n
`

// redisBackupLua returns "hex(key) pttl hex(DUMP)" per key matching ARGV[1]
// (at most ARGV[2]), then "sdo-ok".
const redisBackupLua = `local lim = tonumber(ARGV[2])
` + redisScan + `local function hex(s) return (string.gsub(s, '.', function(c) return string.format('%02x', string.byte(c)) end)) end
local out = {}
for _, k in ipairs(keys) do
  local d = redis.call('DUMP', k)
  if d then out[#out + 1] = hex(k) .. ' ' .. redis.call('PTTL', k) .. ' ' .. hex(d) end
end
out[#out + 1] = 'sdo-ok'
return out
`

// redisWriteLua applies one command (ARGV[5], with ARGV[6..] after the key) to
// every key matching ARGV[1], only when their number is within ARGV[2]..ARGV[3]
// (and at most ARGV[4]). The script is atomic: an out-of-bounds count changes
// nothing.
const redisWriteLua = `if redis.replicate_commands then redis.replicate_commands() end
local lim = tonumber(ARGV[4])
` + redisScan + `local n = #keys
if n < tonumber(ARGV[2]) or n > tonumber(ARGV[3]) then return 'sdo-expect-failed ' .. n end
local changed, failed = 0, 0
for _, k in ipairs(keys) do
  local args = {ARGV[5], k}
  for i = 6, #ARGV do args[#args + 1] = ARGV[i] end
  local res = redis.pcall(unpack(args))
  if type(res) == 'table' and res.err then failed = failed + 1
  elseif (type(res) == 'number' and res > 0) or (type(res) == 'table' and res.ok) then changed = changed + 1 end
end
return 'sdo-affected ' .. changed .. ' failed ' .. failed
`

// redisRestoreLua restores every "hex(key) pttl hex(DUMP)" line of ARGV[1],
// replacing existing keys. Every line is checked before the first write.
const redisRestoreLua = `if redis.replicate_commands then redis.replicate_commands() end
local function unhex(h) return (string.gsub(h, '..', function(x) return string.char(tonumber(x, 16)) end)) end
local items = {}
for line in string.gmatch(ARGV[1], '[^\n]+') do
  if string.sub(line, 1, 8) ~= '#sdo-end' then
    local k, ttl, d = string.match(line, '^(%x*) (%-?%d+) (%x+)$')
    if not k then return redis.error_reply('SDO_BAD_DUMP') end
    ttl = tonumber(ttl)
    if ttl < 0 then ttl = 0 end
    items[#items + 1] = {unhex(k), ttl, unhex(d)}
  end
end
local n = 0
for _, it in ipairs(items) do
  local res = redis.pcall('RESTORE', it[1], it[2], it[3], 'REPLACE')
  if not (type(res) == 'table' and res.err) then n = n + 1 end
end
return 'sdo-restored ' .. n .. ' of ' .. #items
`

// redisInvalidateLua unlinks every key matching ARGV[1].
const redisInvalidateLua = `if redis.replicate_commands then redis.replicate_commands() end
local seen, n, cur = {}, 0, '0'
repeat
  local res = redis.call('SCAN', cur, 'MATCH', ARGV[1], 'COUNT', 1000)
  cur = res[1]
  for _, k in ipairs(res[2]) do
    if not seen[k] then seen[k] = true; n = n + redis.call('DEL', k) end
  end
until cur == '0'
return 'sdo-affected ' .. n
`

// redisWriteCommands are the single-key writes a redis repair may apply; the
// key is always the matched key.
var redisWriteCommands = map[string]bool{
	"DEL": true, "UNLINK": true, "SET": true, "APPEND": true, "INCRBY": true, "DECRBY": true,
	"EXPIRE": true, "PEXPIRE": true, "EXPIREAT": true, "PEXPIREAT": true, "PERSIST": true,
	"HDEL": true, "HSET": true, "HSETNX": true, "HINCRBY": true,
	"SREM": true, "SADD": true,
	"ZREM": true, "ZADD": true, "ZREMRANGEBYSCORE": true, "ZREMRANGEBYRANK": true, "ZREMRANGEBYLEX": true,
	"LREM": true, "LPUSH": true, "RPUSH": true, "LSET": true, "LTRIM": true,
	"XDEL": true, "XTRIM": true,
}

// redisWriteCommandNames lists redisWriteCommands for an error message.
func redisWriteCommandNames() string {
	names := make([]string, 0, len(redisWriteCommands))
	for name := range redisWriteCommands {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

var redisDatabase = regexp.MustCompile(`^[0-9]{1,2}$`)

type redis struct{}

func (redis) Name() string     { return "redis" }
func (redis) DefaultPort() int { return 6379 }
func (redis) Image() string    { return `(^|/)(redis|valkey|keydb)[^/]*([:@]|$)` }

func (redis) Normalize(s Scope) Scope {
	if s.Database == "" {
		s.Database = "0"
	}
	if s.Selector == "" {
		s.Selector = "*"
	}
	return s
}

func (e redis) ValidateScope(s Scope) error {
	s = e.Normalize(s)
	if !redisDatabase.MatchString(s.Database) {
		return fmt.Errorf("redis scope database %q is not a database number", s.Database)
	}
	if s.Collection != "" {
		return fmt.Errorf("redis scope has no collection; select keys with a glob")
	}
	if strings.ContainsAny(s.Selector, "\n\r\x00") || len(s.Selector) > 512 {
		return fmt.Errorf("redis key glob must be one line of at most 512 bytes")
	}
	return validateLimit(s)
}

// ValidateStatement accepts one single-key write command, applied to each
// matched key: "SREM member", "HDEL field", "DEL". Arguments are split on
// whitespace and follow the key.
func (redis) ValidateStatement(statement string) error {
	words := strings.Fields(statement)
	if len(words) == 0 {
		return fmt.Errorf("redis repair statement is empty")
	}
	if !redisWriteCommands[strings.ToUpper(words[0])] {
		return fmt.Errorf("redis repair command %q is not a single-key write SDO applies per matched key (%s)", words[0], redisWriteCommandNames())
	}
	if len(words) > 17 {
		return fmt.Errorf("redis repair statement has more than 16 arguments")
	}
	return nil
}

func (e redis) eval(port int, s Scope, script, lua string, args ...string) Command {
	s = e.Normalize(s)
	return Command{Args: append([]string{"sh", "-c", redisPrelude + script, "sh", strconv.Itoa(port), s.Database, lua}, args...)}
}

func (e redis) Count(port int, s Scope) Command {
	s = e.Normalize(s)
	return e.eval(port, s, `n=$(r EVAL "$1" 0 "$2") || exit 1
case "$n" in ''|*[!0-9]*) exit 1;; esac
echo "sdo-count $n"`, redisCountLua, s.Selector)
}

func (e redis) Backup(port int, s Scope) Command {
	s = e.Normalize(s)
	return e.eval(port, s, `L="$3"
`+dumpWrapper(`r EVAL "$1" 0 "$2" "$3" | awk '/^SDO_LIMIT/ { over = 1 } $0 == "sdo-ok" { ok = 1; next } { if (ok) bad = 1; print } END { if (over) exit `+strconv.Itoa(ExitLimit)+`; exit (ok && !bad) ? 0 : 1 }'`), redisBackupLua, s.Selector, strconv.Itoa(s.limit()))
}

func (e redis) Write(port int, s Scope, statement string, min, max int) Command {
	s = e.Normalize(s)
	words := strings.Fields(statement)
	words[0] = strings.ToUpper(words[0])
	args := append([]string{s.Selector, strconv.Itoa(min), strconv.Itoa(max), strconv.Itoa(s.limit())}, words...)
	return e.eval(port, s, `L="$1";shift
o=$(r EVAL "$L" 0 "$@") || exit 1
case "$o" in "sdo-affected "*|"sdo-expect-failed "*) echo "$o";; *) exit 1;; esac`, redisWriteLua, args...)
}

func (e redis) Restore(port int, s Scope, dump string) (Command, error) {
	c := e.eval(port, s, `o=$(r -x EVAL "$1" 0) || exit 1
case "$o" in "sdo-restored "*) echo "$o";; *) exit 1;; esac`, redisRestoreLua)
	c.Stdin = dump
	return c, nil
}

func (e redis) Invalidate(port int, s Scope) (Command, error) {
	s = e.Normalize(s)
	return e.eval(port, s, `o=$(r EVAL "$1" 0 "$2") || exit 1
case "$o" in "sdo-affected "*) echo "$o";; *) exit 1;; esac`, redisInvalidateLua, s.Selector), nil
}

// redisRemoveMembersLua removes from every set, sorted set or list matching
// ARGV[1] (only when their number is within ARGV[2]..ARGV[3], at most
// ARGV[4]) each member the predicate selects: ARGV[5] "1" for not digits,
// ARGV[6] "1" with ARGV[7]..ARGV[8] the prefix and suffix of a key that must
// exist, ARGV[9] the count of In members that follow, then the NotIn members.
// The script is atomic.
const redisRemoveMembersLua = `if redis.replicate_commands then redis.replicate_commands() end
local lim = tonumber(ARGV[4])
` + redisScan + `local n = #keys
if n < tonumber(ARGV[2]) or n > tonumber(ARGV[3]) then return 'sdo-expect-failed ' .. n end
local digits, check, pre, suf, nin = ARGV[5] == '1', ARGV[6] == '1', ARGV[7], ARGV[8], tonumber(ARGV[9])
local inset, notin, hasnotin = {}, {}, false
for i = 10, 9 + nin do inset[ARGV[i]] = true end
for i = 10 + nin, #ARGV do notin[ARGV[i]] = true; hasnotin = true end
local function selected(m)
  if digits and not string.match(m, '^%-?%d+$') then return true end
  if inset[m] then return true end
  if hasnotin and not notin[m] then return true end
  if check and redis.call('EXISTS', pre .. m .. suf) == 0 then return true end
  return false
end
local removed, failed = 0, 0
for _, k in ipairs(keys) do
  local t = redis.call('TYPE', k)
  t = t.ok or t
  local members, verb
  if t == 'set' then members, verb = redis.call('SMEMBERS', k), 'SREM'
  elseif t == 'zset' then members, verb = redis.call('ZRANGE', k, 0, -1), 'ZREM'
  elseif t == 'list' then members, verb = redis.call('LRANGE', k, 0, -1), 'LREM' end
  if members == nil then failed = failed + 1 else
    local seen = {}
    for _, m in ipairs(members) do
      if not seen[m] then
        seen[m] = true
        if selected(m) then
          if verb == 'LREM' then removed = removed + redis.call('LREM', k, 0, m) else removed = removed + redis.call(verb, k, m) end
        end
      end
    end
  end
end
return 'sdo-affected ' .. removed .. ' failed ' .. failed
`

// redisCopyLua copies every key matching ARGV[1] (only when their number is
// within ARGV[2]..ARGV[3], at most ARGV[4]) from prefix/suffix ARGV[5],
// ARGV[6] to ARGV[7], ARGV[8]: a missing target is restored from the
// source's DUMP with its TTL; an existing set or sorted set is merged member
// by member (sorted sets keep the higher score), a hash gains the fields it
// lacks, and a string or list is kept. A target of another type fails. It
// counts the targets written.
const redisCopyLua = `if redis.replicate_commands then redis.replicate_commands() end
local lim = tonumber(ARGV[4])
` + redisScan + `local n = #keys
if n < tonumber(ARGV[2]) or n > tonumber(ARGV[3]) then return 'sdo-expect-failed ' .. n end
local fp, fs, ip, is = ARGV[5], ARGV[6], ARGV[7], ARGV[8]
local wrote, failed = 0, 0
local function typ(k) local t = redis.call('TYPE', k) return t.ok or t end
for _, k in ipairs(keys) do
  local t = ip .. string.sub(k, #fp + 1, #k - #fs) .. is
  if t ~= k then
    if redis.call('EXISTS', t) == 0 then
      local d = redis.call('DUMP', k)
      if d then
        local ttl = redis.call('PTTL', k)
        if ttl < 0 then ttl = 0 end
        redis.call('RESTORE', t, ttl, d)
        wrote = wrote + 1
      end
    else
      local ts, tt = typ(k), typ(t)
      if ts ~= tt then failed = failed + 1
      elseif ts == 'set' then
        local before = redis.call('SCARD', t)
        if redis.call('SUNIONSTORE', t, t, k) > before then wrote = wrote + 1 end
      elseif ts == 'zset' then
        local before = redis.call('ZCARD', t)
        local ttl = redis.call('PTTL', t)
        redis.call('ZUNIONSTORE', t, 2, t, k, 'AGGREGATE', 'MAX')
        if ttl > 0 then redis.call('PEXPIRE', t, ttl) end
        if redis.call('ZCARD', t) > before then wrote = wrote + 1 end
      elseif ts == 'hash' then
        local h, added = redis.call('HGETALL', k), 0
        for i = 1, #h, 2 do added = added + redis.call('HSETNX', t, h[i], h[i + 1]) end
        if added > 0 then wrote = wrote + 1 end
      end
    end
  end
end
return 'sdo-affected ' .. wrote .. ' failed ' .. failed
`

// redisAffixes splits a copy glob "<prefix>*<suffix>" into its literal parts.
func redisAffixes(glob string) (string, string, error) {
	pre, suf, ok := strings.Cut(glob, "*")
	if !ok || strings.ContainsAny(pre+suf, "*?[]\\") {
		return "", "", fmt.Errorf("redis copy glob %q must be <prefix>*<suffix> with one * and no other wildcard", glob)
	}
	return pre, suf, nil
}

// Copy copies the keys of from's glob to the same names under into's glob,
// in the same database.
func (e redis) Copy(port int, from, into Scope, min, max int) (Command, error) {
	from, into = e.Normalize(from), e.Normalize(into)
	if from.Database != into.Database {
		return Command{}, fmt.Errorf("redis copy stays in one database")
	}
	fp, fs, err := redisAffixes(from.Selector)
	if err != nil {
		return Command{}, err
	}
	ip, is, err := redisAffixes(into.Selector)
	if err != nil {
		return Command{}, err
	}
	return e.eval(port, from, `L="$1";shift
o=$(r EVAL "$L" 0 "$@") || exit 1
case "$o" in "sdo-affected "*|"sdo-expect-failed "*) echo "$o";; *) exit 1;; esac`, redisCopyLua,
		from.Selector, strconv.Itoa(min), strconv.Itoa(max), strconv.Itoa(from.limit()), fp, fs, ip, is), nil
}

// RemoveMembers removes the selected members from the collection values the
// scope selects, in one atomic script.
func (e redis) RemoveMembers(port int, s Scope, p MemberPredicate, min, max int) (Command, error) {
	if err := p.Validate(); err != nil {
		return Command{}, err
	}
	s = e.Normalize(s)
	flag := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	pre, suf, _ := strings.Cut(p.MissingKey, "{member}")
	args := []string{s.Selector, strconv.Itoa(min), strconv.Itoa(max), strconv.Itoa(s.limit()), flag(p.NotDigits), flag(p.MissingKey != ""), pre, suf, strconv.Itoa(len(p.In))}
	args = append(append(args, p.In...), p.NotIn...)
	return e.eval(port, s, `L="$1";shift
o=$(r EVAL "$L" 0 "$@") || exit 1
case "$o" in "sdo-affected "*|"sdo-expect-failed "*) echo "$o";; *) exit 1;; esac`, redisRemoveMembersLua, args...), nil
}
