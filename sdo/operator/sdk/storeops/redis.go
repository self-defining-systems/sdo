package storeops

import (
	"fmt"
	"regexp"
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
	"DEL": true, "UNLINK": true, "HDEL": true, "HSET": true, "SREM": true, "SADD": true,
	"ZREM": true, "ZADD": true, "LREM": true, "SET": true, "EXPIRE": true, "PERSIST": true,
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
		return fmt.Errorf("redis repair command %q is not a single-key write SDO applies per matched key (%s)", words[0], "DEL, UNLINK, HDEL, HSET, SREM, SADD, ZREM, ZADD, LREM, SET, EXPIRE, PERSIST")
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
