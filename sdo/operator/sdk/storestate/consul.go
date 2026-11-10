package storestate

import (
	"fmt"
	"strings"
)

// consulSections are the sections of the consul read, in order.
var consulSections = []string{"info", "services", "instances", "nodes", "members", "kv", "acl"}

var consulSectionKinds = map[string][]ItemKind{
	"info":      {KindRole, KindSetting, KindWriteBlock},
	"services":  {KindRegistryEntry},
	"instances": {KindRegistryEntry},
	"nodes":     {KindRegistryEntry},
	"members":   {KindRegistryEntry},
	"kv":        {KindNamespace},
	"acl":       {KindSetting},
}

// consulScript runs the image's consul CLI against the agent's own HTTP
// port; the CLI reads a token from the container's CONSUL_HTTP_TOKEN itself.
// It prints the leader state, service names with the number of nodes that
// provide each (the first 16), node names, member status, KV key counts per
// top-level prefix (key names only, never a value) and whether ACLs are on.
// A token read is reduced to that classification; no token is printed.
const consulScript = `export CONSUL_HTTP_ADDR=127.0.0.1:%d
c(){ o=$(consul "$@" 2>&1)||{ printf 'ERR %%s\n' "$(printf '%%s' "$o"|grep -v '^$'|head -n1)";return 1;};printf '%%s\n' "$o";}
echo '== info';c info|awk '/^ERR/{print;next} /^[ \t]+(leader|leader_addr|server|known_servers|num_peers|state) = /{sub(/^[ \t]+/,"");print}'
echo '== services';s=$(c catalog services)&&printf '%%s\n' "$s"|head -n 65||printf '%%s\n' "$s"
echo '== instances';case "$s" in ERR*);;*)for n in $(printf '%%s\n' "$s"|head -n 16);do printf '%%s\t%%s\n' "$n" "$(consul catalog nodes -service="$n" 2>/dev/null|tail -n +2|grep -c .)";done;;esac
echo '== nodes';c catalog nodes|awk 'NR==1&&/^ERR/{print;exit} NR>1{print $1}'|head -n 33
echo '== members';c members|awk 'NR==1&&/^ERR/{print;exit} NR>1{print $1"\t"$3"\t"$4}'|head -n 33
echo '== kv';c kv get -keys -separator= ''|awk '/^ERR/{print;exit} {sub(/\/.*/,"/");n[$0]++;t++} END{for(k in n)print "prefix\t"k"\t"n[k];print "total\t"t+0}'|sort|head -n 33
echo '== acl';o=$(consul acl token read -self 2>&1);case "$o" in *"ACL support disabled"*)echo disabled;;*AccessorID*|*"Permission denied"*|*"ACL not found"*|*403*)echo enabled;;*)printf 'ERR %%s\n' "$(printf '%%s' "$o"|head -n1|cut -c1-120|sed 's/[0-9a-f]\{8\}-[0-9a-f-]\{27\}/<id>/g')";;esac
`

type consulAdapter struct{}

func (consulAdapter) Engine() Engine   { return EngineConsul }
func (consulAdapter) DefaultPort() int { return 8500 }

func (consulAdapter) ReadCommand(e Endpoint) []string {
	return append([]string{"sh", "-c", splitScript, "sdo-store-read"}, chunk(fmt.Sprintf(consulScript, e.Port), readChunkBytes)...)
}

// consulRefusal classifies an "ERR <message>" line.
func consulRefusal(line string) string {
	lower := strings.ToLower(line)
	switch {
	case strings.Contains(lower, "connection refused"), strings.Contains(lower, "not found") && !strings.Contains(lower, "acl"),
		strings.Contains(lower, "no such host"), strings.Contains(lower, "timeout"):
		return "unreachable"
	case strings.Contains(lower, "permission denied"), strings.Contains(lower, "403"):
		return "no-permission"
	case strings.Contains(lower, "no cluster leader"), strings.Contains(lower, "no leader"):
		return "loading"
	case strings.Contains(lower, "unknown command"), strings.Contains(lower, "flag provided but not defined"):
		return "unsupported"
	}
	return "refused"
}

func (consulAdapter) Parse(stdout string) (Facts, error) {
	sections, err := splitSections("consul", stdout)
	if err != nil {
		return Facts{}, err
	}
	var items []Item
	unread := map[ItemKind]bool{}
	answered := map[string]bool{}
	for _, name := range consulSections {
		lines, ok := sections[name]
		if !ok {
			return Facts{}, fmt.Errorf("consul read lacks section %q", name)
		}
		if len(lines) > 0 && strings.HasPrefix(lines[0], "ERR") {
			class := consulRefusal(lines[0])
			if class == "unreachable" {
				return Facts{}, fmt.Errorf("consul read section %s: %s", name, lines[0])
			}
			items = append(items, Item{Kind: KindRefusal, Name: name, Value: class})
			for _, k := range consulSectionKinds[name] {
				unread[k] = true
			}
			continue
		}
		answered[name] = true
	}
	if answered["info"] && len(sections["info"]) == 0 {
		return Facts{}, fmt.Errorf("consul read section info is empty")
	}
	malformed := func(section, line string) error {
		return fmt.Errorf("consul %s line is malformed: %q", section, line)
	}
	f := Facts{Engine: EngineConsul}
	info := map[string]string{}
	for _, line := range sections["info"] {
		if !answered["info"] {
			break
		}
		key, value, ok := strings.Cut(line, " = ")
		if !ok {
			return Facts{}, malformed("info", line)
		}
		if _, seen := info[key]; !seen {
			info[key] = value
		}
	}
	var blocks []string
	if answered["info"] {
		leader := "none"
		switch {
		case info["leader"] == "true":
			leader = "self"
		case info["leader_addr"] != "":
			leader = "other"
		}
		items = append(items, Item{Kind: KindRole, Name: "leader", Value: leader})
		if s, ok := info["server"]; ok {
			items = append(items, Item{Kind: KindSetting, Name: "server", Value: s})
		}
		if leader == "none" {
			blocks = append(blocks, "no-leader")
		}
	}
	if answered["services"] {
		names := sections["services"]
		if len(names) > 64 {
			names = names[:64]
			unread[KindRegistryEntry] = true
		}
		items = append(items, Item{Kind: KindRegistryEntry, Name: "services", Value: fmt.Sprint(len(sections["services"]))})
		counts := map[string]string{}
		if answered["instances"] {
			for _, line := range sections["instances"] {
				name, n, ok := strings.Cut(line, "\t")
				if !ok {
					return Facts{}, malformed("instances", line)
				}
				counts[name] = n
			}
		}
		for _, name := range names {
			n, ok := counts[name]
			if !ok {
				n = "listed"
			}
			items = append(items, Item{Kind: KindRegistryEntry, Scope: "service", Name: name, Value: n})
		}
	}
	if answered["nodes"] {
		items = append(items, Item{Kind: KindRegistryEntry, Name: "nodes", Value: fmt.Sprint(len(sections["nodes"]))})
		if len(sections["nodes"]) >= 33 {
			unread[KindRegistryEntry] = true
		}
	}
	if answered["members"] {
		for _, line := range sections["members"] {
			fields := strings.Split(line, "\t")
			if len(fields) != 3 {
				return Facts{}, malformed("members", line)
			}
			items = append(items, Item{Kind: KindRegistryEntry, Scope: "member", Name: fields[0], Value: fields[1] + " " + fields[2]})
		}
	}
	if answered["kv"] {
		for _, line := range sections["kv"] {
			fields := strings.Split(line, "\t")
			switch {
			case len(fields) == 3 && fields[0] == "prefix":
				items = append(items, Item{Kind: KindNamespace, Scope: "kv", Name: fields[1], Value: fields[2]})
			case len(fields) == 2 && fields[0] == "total":
				items = append(items, Item{Kind: KindNamespace, Name: "kv-keys", Value: fields[1]})
			default:
				return Facts{}, malformed("kv", line)
			}
		}
		if len(sections["kv"]) >= 33 {
			unread[KindNamespace] = true
		}
	}
	if answered["acl"] {
		if len(sections["acl"]) != 1 {
			return Facts{}, malformed("acl", strings.Join(sections["acl"], " | "))
		}
		items = append(items, Item{Kind: KindSetting, Name: "acl", Value: sections["acl"][0]})
	}
	for _, b := range blocks {
		items = append(items, Item{Kind: KindWriteBlock, Name: b, Value: "active"})
	}
	if answered["info"] {
		writable := len(blocks) == 0
		f.Writable = &writable
	}
	f.Items = NewFacts(EngineConsul, items).Items
	f.Unread = sortedKinds(unread)
	if err := f.Validate(); err != nil {
		return Facts{}, err
	}
	return f, nil
}
