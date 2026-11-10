package storestate

import (
	"fmt"
	"strings"
)

// memcachedSections are the sections of the memcached read, in order.
var memcachedSections = []string{"stats", "settings"}

// memcachedStats are the "stats" fields the read keeps; memcachedSettings the
// "stats settings" fields.
var (
	memcachedStats    = []string{"curr_items", "evictions", "limit_maxbytes", "bytes", "curr_connections", "rejected_connections", "accepting_conns", "version"}
	memcachedSettings = []string{"maxbytes", "maxconns", "evictions", "item_size_max", "lru_crawler"}
)

// memcachedScript asks the server over its own port with the transport of
// storeops.MemcachedClient (bash /dev/tcp, else nc; memcached images carry no
// client). memcached keeps no credentials in its text protocol, so the read
// needs none. Only the listed STAT fields are kept; any other line (a
// transport or protocol error) is kept so the parser can classify it.
const memcachedScript = `P=%d
%s
k(){ awk -v keep="$1" '{sub(/\r$/,"")} /^STAT /{if(index(" "keep" "," "$2" "))print;next} {print}' ;}
echo '== stats';printf 'stats\r\nquit\r\n'|mc 2>&1|k '%s'
echo '== settings';printf 'stats settings\r\nquit\r\n'|mc 2>&1|k '%s'
`

// splitScript runs a read script that arrives split across its arguments
// (each within the pod-read argument bound), joined back without separators.
const splitScript = `IFS=;s="$*";IFS=' 	
';eval "$s"`

type memcachedAdapter struct{}

func (memcachedAdapter) Engine() Engine   { return EngineMemcached }
func (memcachedAdapter) DefaultPort() int { return 11211 }

// memcachedClient mirrors storeops.MemcachedClient; storestate cannot import
// storeops, which builds on it.
const memcachedClient = `mc(){ if command -v bash >/dev/null 2>&1;then bash -c 'exec 3<>/dev/tcp/127.0.0.1/$0||exit 1;cat >&3;cat <&3' "$P";elif command -v nc >/dev/null 2>&1;then nc -w 5 127.0.0.1 "$P";else echo "ERR no memcached client: the image has neither bash nor nc";return 1;fi;}`

func (memcachedAdapter) ReadCommand(e Endpoint) []string {
	return append([]string{"sh", "-c", splitScript, "sdo-store-read"}, chunk(fmt.Sprintf(memcachedScript, e.Port, memcachedClient, strings.Join(memcachedStats, " "), strings.Join(memcachedSettings, " ")), readChunkBytes)...)
}

// memcachedRefusal classifies a section line that is not a STAT or END line.
func memcachedRefusal(line string) string {
	lower := strings.ToLower(line)
	switch {
	case strings.Contains(lower, "connection refused"), strings.Contains(lower, "no memcached client"), strings.Contains(lower, "not found"),
		strings.Contains(lower, "no such file"), strings.Contains(lower, "timed out"), strings.Contains(lower, "connect"):
		return "unreachable"
	case strings.HasPrefix(line, "SERVER_ERROR") && strings.Contains(lower, "connections"):
		return "max-clients"
	case strings.HasPrefix(line, "ERROR"), strings.HasPrefix(line, "CLIENT_ERROR"):
		return "unsupported"
	}
	return "refused"
}

func (memcachedAdapter) Parse(stdout string) (Facts, error) {
	sections, err := splitSections("memcached", stdout)
	if err != nil {
		return Facts{}, err
	}
	values := map[string]map[string]string{}
	var items []Item
	unread := map[ItemKind]bool{}
	for _, name := range memcachedSections {
		lines, ok := sections[name]
		if !ok {
			return Facts{}, fmt.Errorf("memcached read lacks section %q", name)
		}
		fields, ended := map[string]string{}, false
		var other string
		for _, line := range lines {
			switch {
			case line == "END":
				ended = true
			case strings.HasPrefix(line, "STAT "):
				parts := strings.SplitN(line, " ", 3)
				if len(parts) == 3 {
					fields[parts[1]] = parts[2]
				}
			case other == "":
				other = line
			}
		}
		if !ended {
			class := "unreachable"
			if other != "" {
				class = memcachedRefusal(other)
			}
			if class == "unreachable" {
				return Facts{}, fmt.Errorf("memcached read section %s gave no answer: %q", name, other)
			}
			items = append(items, Item{Kind: KindRefusal, Name: name, Value: class})
			unread[KindSetting], unread[KindNamespace], unread[KindWriteBlock] = true, true, true
			continue
		}
		values[name] = fields
	}
	stats, settings := values["stats"], values["settings"]
	f := Facts{Engine: EngineMemcached}
	for _, name := range []string{"maxbytes", "maxconns", "evictions", "item_size_max", "lru_crawler"} {
		if v, ok := settings[name]; ok {
			items = append(items, Item{Kind: KindSetting, Name: name, Value: v})
		}
	}
	for _, name := range []string{"curr_items", "evictions", "rejected_connections"} {
		if v, ok := stats[name]; ok {
			items = append(items, Item{Kind: KindNamespace, Name: name, Value: v})
		}
	}
	if stats != nil {
		f.Connections = gauge(stats["curr_connections"], settings["maxconns"])
		f.Memory = gauge(stats["bytes"], stats["limit_maxbytes"])
	}
	var blocks []string
	if stats["accepting_conns"] == "0" {
		blocks = append(blocks, "not-accepting-connections")
	}
	if settings["evictions"] == "off" && f.Memory != nil && f.Memory.Max > 0 && f.Memory.Used >= f.Memory.Max {
		blocks = append(blocks, "oom")
	}
	for _, b := range blocks {
		items = append(items, Item{Kind: KindWriteBlock, Name: b, Value: "active"})
	}
	if stats != nil && settings != nil || len(blocks) > 0 {
		writable := len(blocks) == 0
		f.Writable = &writable
	}
	f.Items = NewFacts(EngineMemcached, items).Items
	f.Unread = sortedKinds(unread)
	if err := f.Validate(); err != nil {
		return Facts{}, err
	}
	return f, nil
}
