package storeops

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// MemcachedClient is a shell fragment that defines the memcached text-protocol
// clients over the store's own port $P, for images without a memcached CLI:
// bash's /dev/tcp when the image has bash, else nc. mc sends its stdin, which
// ends with "quit", and prints the answer until the server closes. mcl prints
// a line answer up to its END or error line, for a request after which an
// older server ignores a pipelined quit (lru_crawler metadump). Both print
// "ERR ..." and fail when neither transport exists.
const MemcachedClient = `mc(){ if command -v bash >/dev/null 2>&1;then bash -c 'exec 3<>/dev/tcp/127.0.0.1/$0||exit 1;cat >&3;cat <&3' "$P";elif command -v nc >/dev/null 2>&1;then nc -w 5 127.0.0.1 "$P";else echo "ERR no memcached client: the image has neither bash nor nc";return 1;fi;}
mcl(){ if command -v bash >/dev/null 2>&1;then bash -c 'exec 3<>/dev/tcp/127.0.0.1/$0||exit 1;cat >&3;while IFS= read -r l <&3;do printf "%s\n" "$l";case $l in END*|ERROR*|BUSY*|CLIENT_ERROR*|SERVER_ERROR*)exit 0;;esac;done' "$P";elif command -v nc >/dev/null 2>&1;then nc -w 5 127.0.0.1 "$P";else echo "ERR no memcached client: the image has neither bash nor nc";return 1;fi;}
`

// MemcachedKeys is a shell fragment, after MemcachedClient, that defines
// keys: it lists "<key> <exp>" for every key "lru_crawler metadump all"
// reports whose key matches the ERE in $RE, and exits 4 once more than $L
// match (0 is no bound). exp is 0 for an item without expiry, else the
// server's absolute expiry time. A metadump that does not end with END (the
// crawler is busy or disabled) fails.
const MemcachedKeys = `keys(){ printf 'lru_crawler metadump all\r\nquit\r\n'|mcl|awk -v L="${L:-0}" '
function dec(s,  o,i,c){o="";for(i=1;i<=length(s);i++){c=substr(s,i,1);if(c=="%"&&i+2<=length(s)){o=o sprintf("%c",16*index("0123456789ABCDEF",toupper(substr(s,i+1,1)))-17+index("0123456789ABCDEF",toupper(substr(s,i+2,1))));i+=2}else o=o c}return o}
{sub(/\r$/,"")}
/^key=/{k=dec(substr($1,5));e=0;for(i=2;i<=NF;i++)if(substr($i,1,4)=="exp=")e=substr($i,5);if(e<0)e=0;if(k ~ ENVIRON["RE"]){n++;if(L>0&&n>L)exit 4;print k" "e};next}
/^OK$/&&!n{next}
/^END/{end=1;exit}
{bad=1;exit}
END{if(n>L&&L>0)exit 4;if(!end||bad)exit 1}';}
`

// memcachedPrelude defines the clients and keys. Positional arguments: $1
// port, $2 the key ERE; they are shifted away.
const memcachedPrelude = `P="$1";RE="$2";export RE;shift 2
` + MemcachedClient + MemcachedKeys

// memcachedDump is a shell fragment, after MemcachedKeys, that defines dump:
// it lists the matching keys, gets them in one connection and prints one line
// per hit, "hex(key) flags exp hex(value)" with "-" for an empty value. The
// answer is read as "od" hex, so a value may hold any byte. A key that expired
// or was evicted between the listing and the get is not in the dump.
const memcachedDump = `dump(){ k=$(keys)||return $?;[ -n "$k" ]||return 0;KL=$k;export KL
{ printf '%s\n' "$k"|awk '{printf "get %s\r\n",$1}';printf 'quit\r\n';}|mc|od -An -v -tx1|awk 'BEGIN{A="";for(i=32;i<127;i++){c=sprintf("%c",i);A=A c;ch[sprintf("%02x",i)]=c}n=split(ENVIRON["KL"],kl,"\n");for(i=1;i<=n;i++){split(kl[i],f," ");ex[f[1]]=f[2]}}
function hx(s,  o,i){o="";for(i=1;i<=length(s);i++)o=o sprintf("%02x",index(A,substr(s,i,1))+31);return o}
{for(i=1;i<=NF;i++){b=$i
 if(st==1){if(left>0){v=v b;left--;continue}skip--;if(skip==0){print hx(k)" "fl" "((k in ex)?ex[k]:0)" "(v==""?"-":v);st=0}continue}
 if(b=="0d")continue
 if(b=="0a"){if(h=="END"){h="";continue}m=split(h,w," ");if(w[1]!="VALUE"||m<4){bad=1;exit}k=w[2];fl=w[3];left=w[4]+0;v="";skip=2;st=1;h="";continue}
 h=h ch[b]}}
END{if(bad||st==1)exit 1}';}
`

// memcachedSelector is the glob a memcached scope accepts: key characters
// and the wildcards * and ?.
var memcachedSelector = regexp.MustCompile(`^[A-Za-z0-9_.:@/+=,*?-]{1,250}$`)

type memcached struct{}

func (memcached) Name() string     { return "memcached" }
func (memcached) DefaultPort() int { return 11211 }
func (memcached) Image() string    { return `(^|/)memcached[^/]*([:@]|$)` }

func (memcached) Normalize(s Scope) Scope {
	if s.Selector == "" {
		s.Selector = "*"
	}
	return s
}

// ValidateScope accepts a key glob; memcached has no databases or
// collections.
func (e memcached) ValidateScope(s Scope) error {
	s = e.Normalize(s)
	if s.Database != "" || s.Collection != "" {
		return fmt.Errorf("memcached scope has no database or collection; select keys with a glob")
	}
	if !memcachedSelector.MatchString(s.Selector) {
		return fmt.Errorf("memcached key glob %q must be 1-250 key characters (letters, digits, _.:@/+=,-) and the wildcards * and ?", s.Selector)
	}
	return validateLimit(s)
}

// ValidateStatement accepts "delete": a cache entry is repaired by dropping
// it, so its reader fills it again from the source.
func (memcached) ValidateStatement(statement string) error {
	if statement != "delete" {
		return fmt.Errorf("memcached repair statement must be \"delete\"; a cache entry is refilled from its source")
	}
	return nil
}

// memcachedERE is the anchored ERE of a key glob.
func memcachedERE(glob string) string {
	var b strings.Builder
	b.WriteString("^")
	for _, c := range glob {
		switch c {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		case '.', '+':
			b.WriteString("[" + string(c) + "]")
		default:
			b.WriteRune(c)
		}
	}
	b.WriteString("$")
	return b.String()
}

func (e memcached) run(port int, s Scope, script string, args ...string) Command {
	s = e.Normalize(s)
	return Command{Args: append([]string{"sh", "-c", memcachedPrelude + script, "sh", strconv.Itoa(port), memcachedERE(s.Selector)}, args...)}
}

// Count counts the keys metadump lists; curr_items is not used because it
// still counts items a flush_all has invalidated.
func (e memcached) Count(port int, s Scope) Command {
	return e.run(port, s, `k=$(keys)||exit 1;echo "sdo-count $(printf '%s' "$k"|grep -c .)"`)
}

// Backup lists the matching keys with metadump, gets them in one connection
// and prints "hex(key) flags exp hex(value)" per hit; a key that expired or
// was evicted in between is not in the dump.
func (e memcached) Backup(port int, s Scope) Command {
	s = e.Normalize(s)
	return e.run(port, s, memcachedDump+`L="$1"
`+dumpWrapper(`dump`), strconv.Itoa(s.limit()))
}

// Write deletes the matching keys when their number is within [min, max].
// memcached has no transaction: a key set between the count and the delete
// is not deleted, and the executor counts again afterwards.
func (e memcached) Write(port int, s Scope, _ string, min, max int) Command {
	return e.run(port, s, `L=0;k=$(keys)||exit 1;n=$(printf '%s' "$k"|grep -c .)
if [ "$n" -lt "$1" ]||[ "$n" -gt "$2" ];then echo "sdo-expect-failed $n";exit 0;fi
`+memcachedDelete, strconv.Itoa(min), strconv.Itoa(max))
}

// memcachedDelete deletes the keys listed in $k and prints how many the
// server reports deleted.
const memcachedDelete = `[ -n "$k" ]||{ echo "sdo-affected 0";exit 0;}
{ printf '%s\n' "$k"|awk '{printf "delete %s\r\n",$1}';printf 'quit\r\n';}|mc|awk '{sub(/\r$/,"")} $0=="DELETED"{d++;next} $0=="NOT_FOUND"{next} {bad=1} END{if(bad)exit 1;print "sdo-affected " d+0}'
`

// Restore sets every backed-up item again with its flags and expiry,
// replacing the current value.
func (e memcached) Restore(port int, s Scope, dump string) (Command, error) {
	c := e.run(port, s, `t=$(sed '$d'|awk 'BEGIN{for(i=0;i<256;i++)o[sprintf("%02x",i)]=sprintf("\\%03o",i)}
function oct(h,  r,i){r="";for(i=1;i<=length(h);i+=2)r=r o[substr(h,i,2)];return r}
NF!=4{bad=1;exit}
{v=($4=="-"?"":$4);print "set\\040" oct($1) "\\040" $2 "\\040" $3 "\\040" length(v)/2 "\\015\\012" oct(v) "\\015\\012"}
END{if(bad)exit 1}')||exit 1
n=$(printf '%s\n' "$t"|grep -c .)
{ printf '%s\n' "$t"|while IFS= read -r l;do [ -n "$l" ]&&printf "$l";done;printf 'quit\r\n';}|mc|awk -v n="$n" '{sub(/\r$/,"")} $0=="STORED"{s++;next} {bad=1} END{if(bad||s!=n)exit 1;print "sdo-restored " s+0}'`)
	c.Stdin = dump
	return c, nil
}

// Invalidate flushes every item for the whole cache, reporting the keys
// listed before the flush, and otherwise deletes the matching keys.
func (e memcached) Invalidate(port int, s Scope) (Command, error) {
	s = e.Normalize(s)
	if s.Selector == "*" {
		return e.run(port, s, `k=$(keys)||exit 1;n=$(printf '%s' "$k"|grep -c .)
o=$(printf 'flush_all\r\nquit\r\n'|mc|tr -d '\r')||exit 1
[ "$o" = OK ]||exit 1;echo "sdo-affected $n"`), nil
	}
	return e.run(port, s, `L=0;k=$(keys)||exit 1
`+memcachedDelete), nil
}

// Copy is refused: a cache's entries are refilled from their source.
func (memcached) Copy(int, Scope, Scope, int, int) (Command, error) {
	return Command{}, fmt.Errorf("memcached has no cross-namespace copy; a cache is refilled from its source")
}

// RemoveMembers is refused: memcached values have no members.
func (memcached) RemoveMembers(int, Scope, MemberPredicate, int, int) (Command, error) {
	return Command{}, fmt.Errorf("memcached values have no members; delete the entry instead")
}
