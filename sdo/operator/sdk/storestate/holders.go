package storestate

import (
	"encoding/hex"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sdo.dev/operator/sdk"
)

// Holder states. A store that reports sessions says which state each is in; a
// store that refused its session list (for example at its connection cap) is
// read from the container's TCP table, where a connection is only connected.
const (
	HolderIdle              = "idle"
	HolderActive            = "active"
	HolderIdleInTransaction = "idle-in-transaction"
	HolderBlocked           = "blocked"
	HolderConnected         = "connected"
)

// PeerLocal is the peer of a session over the store's own socket or loopback
// address: a process inside the store's Pod.
const PeerLocal = "local"

// MaxHolders bounds the holder groups of one read; the read lists the largest
// groups first.
const MaxHolders = 12

// Holder is one group of the connections a store holds: a peer address (an IP,
// or PeerLocal) and a state, with their count. It is value-free: no user,
// query or record.
type Holder struct {
	Peer  string `json:"peer"`
	State string `json:"state"`
	Count int    `json:"count"`
}

// HolderGroup is a Holder with its peer resolved through the snapshot's Pod
// IPs: the Pod and its controlling workload ("Kind/name"), or neither for an
// address no Pod of the snapshot has.
type HolderGroup struct {
	Holder
	Pod      string `json:"pod,omitempty"`
	Workload string `json:"workload,omitempty"`
}

// holderLine parses one holders line: "holder\t<peer>\t<state>\t<n>" from the
// store's session list, or "tcp\t<hex address>\t<n>" from /proc/net/tcp.
func holderLine(line string) (Holder, error) {
	fields := strings.Split(line, "\t")
	switch {
	case fields[0] == "holder" && len(fields) == 4:
		n, err := strconv.Atoi(fields[3])
		if err != nil || n < 0 {
			return Holder{}, fmt.Errorf("holder count %q", fields[3])
		}
		return Holder{Peer: peer(fields[1]), State: fields[2], Count: n}, nil
	case fields[0] == "tcp" && len(fields) == 3:
		n, err := strconv.Atoi(fields[2])
		if err != nil || n < 0 {
			return Holder{}, fmt.Errorf("holder count %q", fields[2])
		}
		ip, err := procNetIP(fields[1])
		if err != nil {
			return Holder{}, err
		}
		return Holder{Peer: peer(ip), State: HolderConnected, Count: n}, nil
	}
	return Holder{}, fmt.Errorf("holders line is malformed: %q", line)
}

// peer normalizes an address: a socket path, an empty address or a loopback
// IP is PeerLocal; an IPv4-mapped IPv6 address is its IPv4 form.
func peer(address string) string {
	address = strings.TrimSpace(address)
	if address == "" || address == PeerLocal || strings.HasPrefix(address, "/") {
		return PeerLocal
	}
	if host, _, err := net.SplitHostPort(address); err == nil {
		address = host
	}
	ip := net.ParseIP(strings.Trim(address, "[]"))
	if ip == nil {
		return address
	}
	if ip.IsLoopback() {
		return PeerLocal
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	return ip.String()
}

// procNetIP decodes a /proc/net/tcp{,6} address: host-order 32-bit words in
// hex.
func procNetIP(h string) (string, error) {
	raw, err := hex.DecodeString(h)
	if err != nil || (len(raw) != 4 && len(raw) != 16) {
		return "", fmt.Errorf("tcp address %q is malformed", h)
	}
	ip := make(net.IP, len(raw))
	for word := 0; word < len(raw); word += 4 {
		for i := 0; i < 4; i++ {
			ip[word+i] = raw[word+3-i]
		}
	}
	return ip.String(), nil
}

// parseHolders parses a holders section. A missing section leaves the holders
// unknown (nil); "more" after the bound is ignored.
func parseHolders(lines []string, present bool) ([]Holder, error) {
	if !present {
		return nil, nil
	}
	merged := map[[2]string]int{}
	for _, line := range lines {
		if line == "more" {
			continue
		}
		h, err := holderLine(line)
		if err != nil {
			return nil, err
		}
		merged[[2]string{h.Peer, h.State}] += h.Count
	}
	out := make([]Holder, 0, len(merged))
	for k, n := range merged {
		out = append(out, Holder{Peer: k[0], State: k[1], Count: n})
	}
	sortHolders(out)
	if len(out) > MaxHolders {
		out = out[:MaxHolders]
	}
	return out, nil
}

func sortHolders(h []Holder) {
	sort.Slice(h, func(i, j int) bool {
		if h[i].Count != h[j].Count {
			return h[i].Count > h[j].Count
		}
		if h[i].Peer != h[j].Peer {
			return h[i].Peer < h[j].Peer
		}
		return h[i].State < h[j].State
	})
}

// tcpHolders is a shell snippet that prints the TCP fallback: the
// established connections to the store's port grouped by remote address. It
// needs no credential, so it answers when the store refuses its session list.
// %[1]s is the port as four upper-case hex digits. A read passes it as an
// argument and evaluates it.
const tcpHolders = `cat /proc/net/tcp /proc/net/tcp6 2>/dev/null|awk -v p=":%[1]s" '$4=="01"&&substr($2,length($2)-4)==p{split($3,r,":");n[r[1]]++}END{for(k in n)print n[k]"\t"k}'|sort -rn|head -n 12|awk -F'\t' '{print "tcp\t"$2"\t"$1}'`

func tcpHoldersSnippet(port int) string { return fmt.Sprintf(tcpHolders, fmt.Sprintf("%04X", port)) }

// redisHoldersSnippet prints the redis holders section: CLIENT LIST lines
// (other than the read's own) grouped by address and state, or the TCP
// fallback ($Z) when the store refuses the list. The read evaluates it.
const redisHoldersSnippet = `o=$(r CLIENT LIST);case "$o" in id=*)printf '%s\n' "$o"|awk '` + redisHolders + `'|sort -rn|head -n 12|awk -F'\t' '{print "holder\t"$2"\t"$3"\t"$1}';;*)eval "$Z";;esac`

// redisHolders is the awk program grouping CLIENT LIST lines by address and
// state, printing "<count>\t<addr>\t<state>".
const redisHolders = `{a="";f="";i=0;c="";for(j=1;j<=NF;j++){e=index($j,"=");k=substr($j,1,e-1);v=substr($j,e+1);if(k=="addr")a=v;else if(k=="flags")f=v;else if(k=="idle")i=v+0;else if(k=="cmd")c=v}if(c=="client|list"||c=="client")next;sub(/:[0-9]+$/,"",a);s=(f~/b/)?"blocked":(f~/x/)?"idle-in-transaction":(i>0)?"idle":"active";n[a"\t"s]++}END{for(k in n)print n[k]"\t"k}`

// ResolveHolders names the Pod and workload of each holder whose peer is a
// Pod IP of the snapshot. PeerLocal resolves to the store Pod itself.
func ResolveHolders(view sdk.DetectionContext, store sdk.ObjectRef, holders []Holder) []HolderGroup {
	byIP := map[string]corev1.Pod{}
	var self *corev1.Pod
	if view != nil {
		for _, pod := range view.Pods() {
			if pod.Namespace == store.Namespace && pod.Name == store.Name {
				p := pod
				self = &p
			}
			if pod.Spec.HostNetwork {
				continue
			}
			for _, ip := range pod.Status.PodIPs {
				byIP[ip.IP] = pod
			}
			if pod.Status.PodIP != "" {
				byIP[pod.Status.PodIP] = pod
			}
		}
	}
	out := make([]HolderGroup, 0, len(holders))
	for _, h := range holders {
		group := HolderGroup{Holder: h}
		if h.Peer == PeerLocal && self != nil {
			group.Pod, group.Workload = self.Name, workloadOf(*self)
		} else if pod, ok := byIP[h.Peer]; ok {
			group.Pod, group.Workload = pod.Name, workloadOf(pod)
		}
		out = append(out, group)
	}
	return out
}

// workloadOf is the Pod's controlling workload as "Kind/name"; a ReplicaSet
// Pod with a template hash belongs to its Deployment.
func workloadOf(pod corev1.Pod) string {
	for _, owner := range pod.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if hash := pod.Labels["pod-template-hash"]; owner.Kind == "ReplicaSet" && hash != "" && strings.HasSuffix(owner.Name, "-"+hash) {
			return "Deployment/" + strings.TrimSuffix(owner.Name, "-"+hash)
		}
		return owner.Kind + "/" + owner.Name
	}
	return "Pod/" + pod.Name
}

// RenderHolders is a short value-free account of the largest holder groups.
func RenderHolders(groups []HolderGroup) string {
	parts := make([]string, 0, len(groups))
	for _, g := range groups {
		who := g.Peer
		switch {
		case g.Peer == PeerLocal && g.Workload != "":
			who = "local (inside " + g.Pod + ")"
		case g.Workload != "":
			who = g.Workload + " " + g.Pod + " " + g.Peer
		case g.Peer != PeerLocal:
			who = g.Peer + " (no Pod of the namespace)"
		}
		parts = append(parts, fmt.Sprintf("%d %s from %s", g.Count, g.State, who))
	}
	return strings.Join(parts, ", ")
}
