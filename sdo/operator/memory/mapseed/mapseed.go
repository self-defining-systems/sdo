// Package mapseed proposes an application map from rendered manifests alone,
// for an application deployed without its source (a third-party chart) or
// with source SDO cannot read. It is deterministic: the same objects give the
// same map. Every component and edge it proposes carries evidence
// `manifest`; the detection designer reviews the seed, corrects it from what
// it knows or observes, and adds operations and probes.
//
// What it reads: workloads and their pod templates (images, volumes, env,
// args, health probes), Services and their selectors, the ConfigMaps a
// workload mounts or imports, and Ingress backends. Secrets are never read.
package mapseed

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/rendered"
)

// Seed is a proposed application map and the entry points it suggests.
type Seed struct {
	Map     appmap.Map
	Entries []Entry
	// Possible are calls only a Secret reference suggests; see PossibleEdge.
	Possible []PossibleEdge
}

// Entry is a Service the application is probably entered through: exposed
// outside the cluster, or one no other component calls.
type Entry struct {
	Component string
	Service   string
	Port      int
	PortName  string
	// Exposure says why it is an entry candidate.
	Exposure string
	// Health lists the HTTP health endpoints the workload's own probes call,
	// such as "readiness GET /healthz port 8080".
	Health []string
}

// componentKinds are the workload kinds a component can name; they are the
// kinds a pod read can target, so a seeded store gets Go's store reader.
var componentKinds = map[string]bool{"Deployment": true, "StatefulSet": true, "DaemonSet": true, "ReplicaSet": true}

type component struct {
	appmap.Component
	object     *unstructured.Unstructured
	namespace  string
	labels     map[string]string
	persistent bool
	engine     string
}

type service struct {
	name, namespace string
	object          *unstructured.Unstructured
	components      []string
}

type seeder struct {
	components map[string]*component
	order      []string
	services   map[string]*service // by name
	configMaps map[string]*unstructured.Unstructured
	namespaces map[string]bool
	edges      map[[2]string]appmap.Edge
	ingress    map[string][]string // Service -> how an Ingress exposes it
	jobs       []string            // Job names, each a procedure the application ships
}

// FromObjects seeds an application map from rendered objects.
func FromObjects(objects []*unstructured.Unstructured) Seed {
	s := &seeder{components: map[string]*component{}, services: map[string]*service{}, configMaps: map[string]*unstructured.Unstructured{}, namespaces: map[string]bool{}, edges: map[[2]string]appmap.Edge{}, ingress: map[string][]string{}}
	sorted := append([]*unstructured.Unstructured(nil), objects...)
	sort.SliceStable(sorted, func(i, j int) bool { return objectKey(sorted[i]) < objectKey(sorted[j]) })
	for _, object := range sorted {
		s.add(object)
	}
	s.bindServices()
	for _, name := range s.order {
		s.findEdges(s.components[name])
	}
	s.classify()
	return Seed{Map: s.buildMap(), Entries: s.entries(), Possible: s.findPossibleEdges()}
}

func objectKey(o *unstructured.Unstructured) string {
	return o.GetNamespace() + "\x00" + o.GetKind() + "\x00" + o.GetName()
}

func namespaceOf(o *unstructured.Unstructured) string {
	if ns := o.GetNamespace(); ns != "" {
		return ns
	}
	return "default"
}

var nonID = regexp.MustCompile(`[^a-z0-9-]+`)

func logicalName(name string) string {
	return strings.Trim(nonID.ReplaceAllString(strings.ToLower(name), "-"), "-")
}

func (s *seeder) add(o *unstructured.Unstructured) {
	kind, name := o.GetKind(), o.GetName()
	if name == "" {
		return
	}
	s.namespaces[namespaceOf(o)] = true
	switch {
	case kind == "Service":
		if _, dup := s.services[name]; !dup {
			s.services[name] = &service{name: name, namespace: namespaceOf(o), object: o}
		}
	case kind == "ConfigMap":
		s.configMaps[namespaceOf(o)+"/"+name] = o
	case kind == "Ingress":
		s.addIngress(o)
	case kind == "Job":
		s.jobs = append(s.jobs, name)
	case componentKinds[kind]:
		id := logicalName(name)
		if _, dup := s.components[id]; dup || id == "" {
			id = logicalName(name + "-" + kind)
		}
		if _, dup := s.components[id]; dup || id == "" {
			return
		}
		meta, _ := rendered.PodTemplateMetadata(o.Object, false)
		labels := map[string]string{}
		for key, value := range mapValue(meta["labels"]) {
			if text, ok := value.(string); ok {
				labels[key] = text
			}
		}
		c := &component{Component: appmap.Component{Name: id, Workload: kind + "/" + name, Evidence: []string{appmap.EvidenceManifest}}, object: o, namespace: namespaceOf(o), labels: labels}
		c.persistent = persistent(o)
		c.engine = engineOf(o)
		s.components[id] = c
		s.order = append(s.order, id)
	}
}

func (s *seeder) addIngress(o *unstructured.Unstructured) {
	spec := mapValue(o.Object["spec"])
	note := func(svc, host, path string) {
		if svc == "" {
			return
		}
		s.ingress[svc] = appendUnique(s.ingress[svc], strings.TrimSpace("Ingress "+o.GetName()+" "+host+path))
	}
	if backend := mapValue(mapValue(spec["defaultBackend"])["service"]); backend != nil {
		name, _ := backend["name"].(string)
		note(name, "", "")
	}
	for _, rule := range listValue(spec["rules"]) {
		host, _ := mapValue(rule)["host"].(string)
		for _, p := range listValue(mapValue(mapValue(rule)["http"])["paths"]) {
			path, _ := mapValue(p)["path"].(string)
			name, _ := mapValue(mapValue(mapValue(p)["backend"])["service"])["name"].(string)
			note(name, host, path)
		}
	}
}

func (s *seeder) bindServices() {
	names := make([]string, 0, len(s.services))
	for name := range s.services {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		svc := s.services[name]
		selector := map[string]string{}
		for key, value := range mapValue(mapValue(svc.object.Object["spec"])["selector"]) {
			if text, ok := value.(string); ok {
				selector[key] = text
			}
		}
		if len(selector) == 0 {
			continue
		}
		for _, id := range s.order {
			c := s.components[id]
			if c.namespace != svc.namespace || !matches(selector, c.labels) {
				continue
			}
			c.Services = append(c.Services, name)
			svc.components = append(svc.components, id)
		}
	}
}

func matches(selector, labels map[string]string) bool {
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func persistent(o *unstructured.Unstructured) bool {
	if o.GetKind() == "StatefulSet" && len(listValue(mapValue(o.Object["spec"])["volumeClaimTemplates"])) > 0 {
		return true
	}
	pod, _ := rendered.PodSpec(o.Object)
	for _, volume := range listValue(pod["volumes"]) {
		if mapValue(volume)["persistentVolumeClaim"] != nil {
			return true
		}
	}
	return false
}

// imageEngines maps the last path segment of a well-known store image to its
// engine. Only exact names count, so an exporter or client image named after
// a store (postgres-exporter) is not a store.
var imageEngines = func() map[string]string {
	out := map[string]string{}
	for engine, names := range map[string][]string{
		appmap.EnginePostgres:  {"postgres", "postgresql", "postgis", "timescaledb", "timescaledb-ha", "pgvector", "postgresql-repmgr"},
		appmap.EngineMySQL:     {"mysql", "mysql-server", "mariadb", "percona", "percona-server", "mariadb-galera"},
		appmap.EngineMongoDB:   {"mongo", "mongodb", "mongodb-community-server"},
		appmap.EngineRedis:     {"redis", "redis-stack", "redis-stack-server", "valkey", "keydb", "redis-cluster"},
		appmap.EngineMemcached: {"memcached"},
		appmap.EngineConsul:    {"consul"},
		appmap.EngineOther: {"minio", "seaweedfs", "garage", "ceph", "rabbitmq", "kafka", "cp-kafka", "nats", "zookeeper", "cp-zookeeper",
			"etcd", "elasticsearch", "opensearch", "cassandra", "scylla", "clickhouse", "clickhouse-server", "influxdb", "couchdb",
			"couchbase", "neo4j", "cockroach", "pulsar", "meilisearch", "solr", "qdrant", "weaviate", "milvus", "typesense", "nsqd",
			"activemq", "artemis", "dragonfly"},
	} {
		for _, name := range names {
			out[name] = engine
		}
	}
	return out
}()

// imageName is the last path segment of an image reference, without tag or
// digest.
func imageName(image string) string {
	image, _, _ = strings.Cut(image, "@")
	if slash := strings.LastIndex(image, "/"); slash >= 0 {
		image = image[slash+1:]
	}
	image, _, _ = strings.Cut(image, ":")
	return strings.ToLower(image)
}

// engineOf is the engine of the first main container whose image is a
// well-known store.
func engineOf(o *unstructured.Unstructured) string {
	pod, _ := rendered.PodSpec(o.Object)
	for _, container := range listValue(pod[rendered.Containers]) {
		image, _ := mapValue(container)["image"].(string)
		if engine, ok := imageEngines[imageName(image)]; ok {
			return engine
		}
	}
	return ""
}

// classify sets holds_data and the data role. A database, object store or
// queue holds data; a redis or memcached does only with persistent storage; a
// workload with persistent storage and an unknown image is a store of engine
// other. A memcached whose callers also call authoritative stores is derived
// from them.
func (s *seeder) classify() {
	for _, id := range s.order {
		c := s.components[id]
		switch c.engine {
		case "":
			if c.persistent {
				c.HoldsData = true
				c.Data = &appmap.Data{Engine: appmap.EngineOther, Role: appmap.RoleAuthoritative}
			}
		case appmap.EngineRedis:
			c.HoldsData = c.persistent
			c.Data = &appmap.Data{Engine: c.engine, Role: appmap.RoleAuthoritative}
		case appmap.EngineMemcached:
			c.HoldsData = c.persistent
		default:
			c.HoldsData = true
			c.Data = &appmap.Data{Engine: c.engine, Role: appmap.RoleAuthoritative}
		}
	}
	for _, id := range s.order {
		c := s.components[id]
		if c.engine != appmap.EngineMemcached || c.persistent {
			continue
		}
		var sources []string
		for key := range s.edges {
			if key[1] != id {
				continue
			}
			for other := range s.edges {
				target := s.components[other[1]]
				if other[0] == key[0] && target != nil && target.Data != nil && target.Data.Role == appmap.RoleAuthoritative {
					sources = appendUnique(sources, other[1])
				}
			}
		}
		sort.Strings(sources)
		if len(sources) > 0 {
			c.Data = &appmap.Data{Engine: appmap.EngineMemcached, Role: appmap.RoleDerived, DerivedFrom: sources}
		}
	}
}

var (
	hostToken = regexp.MustCompile(`(?i)[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)*`)
	// hostKey ends the text before a value that names a host: `host: "x"`,
	// `address = x`, `--db-host=x`.
	hostKey = regexp.MustCompile(`(?i)(host|hostname|server|servers|address|addr|endpoint|url|uri|dsn)["']?\s*[:=]\s*["']?$`)
	// hostVar is an env or ConfigMap key whose whole value is an address.
	hostVar = regexp.MustCompile(`(?i)(^|[_.-])(host|hostname|addr|address|server|servers|url|uri|dsn|endpoint|endpoints|brokers?|service)$`)
)

// refs returns the Services a text names as a network peer. A qualified name
// (svc.namespace, svc.svc..., pod.svc.namespace) always counts; a bare name
// counts only in an address position: after `://` or `@`, before `:port`,
// after a host-like key, or anywhere in a value whose key names an address.
func (s *seeder) refs(text string, addressValue bool) []string {
	var out []string
	for _, loc := range hostToken.FindAllStringIndex(text, -1) {
		labels := strings.Split(strings.ToLower(text[loc[0]:loc[1]]), ".")
		// qualified reports whether the labels after i make labels[i] a
		// Service's DNS name: svc.svc..., svc.<namespace>, svc.<any>.svc...
		qualified := func(i int) bool {
			rest := labels[i+1:]
			return len(rest) > 0 && (rest[0] == "svc" || s.namespaces[rest[0]] || (len(rest) > 1 && rest[1] == "svc"))
		}
		var name string
		isQualified := false
		switch {
		case s.services[labels[0]] != nil && (len(labels) == 1 || qualified(0)):
			name, isQualified = labels[0], len(labels) > 1
		case len(labels) >= 3 && s.services[labels[1]] != nil && qualified(1):
			// A Pod of a headless Service: pod.svc.namespace...
			name, isQualified = labels[1], true
		default:
			continue
		}
		before, after := text[:loc[0]], text[loc[1]:]
		if isQualified || addressValue || strings.HasSuffix(before, "://") || strings.HasSuffix(before, "@") ||
			portSuffix.MatchString(after) || hostKey.MatchString(tail(before, 48)) {
			out = appendUnique(out, name)
		}
	}
	return out
}

var portSuffix = regexp.MustCompile(`^:[0-9]`)

// metricsPort names a port that serves metrics, not users.
var metricsPort = regexp.MustCompile(`(?i)metrics|prometheus|exporter`)

func tail(text string, n int) string {
	if len(text) > n {
		return text[len(text)-n:]
	}
	return text
}

// findEdges reads what c's pod template says about its peers, in a stable
// order: per container, env values, ConfigMap keys imported into the
// environment, command and args; then mounted ConfigMaps by name and key.
func (s *seeder) findEdges(c *component) {
	pod, _ := rendered.PodSpec(c.object.Object)
	cite := func(what string, services []string) {
		for _, name := range services {
			for _, target := range s.services[name].components {
				key := [2]string{c.Name, target}
				if target == c.Name {
					continue
				}
				if _, seen := s.edges[key]; !seen {
					s.edges[key] = appmap.Edge{From: c.Name, To: target, Evidence: []string{appmap.EvidenceManifest}, Source: c.Workload + " " + what}
				}
			}
		}
	}
	var mounted []string
	for _, group := range rendered.ContainerFields {
		for _, value := range listValue(pod[group]) {
			container := mapValue(value)
			for _, item := range listValue(container["env"]) {
				env := mapValue(item)
				name, _ := env["name"].(string)
				if text, ok := env["value"].(string); ok {
					cite("env "+name, s.refs(text, hostVar.MatchString(name)))
				}
				ref := mapValue(mapValue(env["valueFrom"])["configMapKeyRef"])
				if cm, key := stringValue(ref["name"]), stringValue(ref["key"]); cm != "" && key != "" {
					if text, ok := s.configMapData(c.namespace, cm)[key]; ok {
						cite("env "+name+" from ConfigMap/"+cm+" key "+key, s.refs(text, hostVar.MatchString(name) || hostVar.MatchString(key)))
					}
				}
			}
			for _, item := range listValue(container["envFrom"]) {
				if cm := stringValue(mapValue(mapValue(item)["configMapRef"])["name"]); cm != "" {
					mounted = appendUnique(mounted, cm)
				}
			}
			for _, field := range []string{"command", "args"} {
				for _, arg := range listValue(container[field]) {
					if text, ok := arg.(string); ok {
						cite(field, s.refs(text, false))
					}
				}
			}
		}
	}
	for _, volume := range listValue(pod["volumes"]) {
		v := mapValue(volume)
		if cm := stringValue(mapValue(v["configMap"])["name"]); cm != "" {
			mounted = appendUnique(mounted, cm)
		}
		for _, source := range listValue(mapValue(v["projected"])["sources"]) {
			if cm := stringValue(mapValue(mapValue(source)["configMap"])["name"]); cm != "" {
				mounted = appendUnique(mounted, cm)
			}
		}
	}
	sort.Strings(mounted)
	for _, cm := range mounted {
		data := s.configMapData(c.namespace, cm)
		keys := make([]string, 0, len(data))
		for key := range data {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			cite("ConfigMap/"+cm+" key "+key, s.refs(data[key], hostVar.MatchString(key)))
		}
	}
}

func (s *seeder) configMapData(namespace, name string) map[string]string {
	out := map[string]string{}
	if cm := s.configMaps[namespace+"/"+name]; cm != nil {
		for key, value := range mapValue(cm.Object["data"]) {
			if text, ok := value.(string); ok {
				out[key] = text
			}
		}
	}
	return out
}

func (s *seeder) buildMap() appmap.Map {
	m := appmap.Map{Version: appmap.Version, Components: []appmap.Component{}, Edges: []appmap.Edge{}, Operations: []appmap.Operation{}, Probes: []appmap.Probe{}}
	names := append([]string(nil), s.order...)
	sort.Strings(names)
	for _, id := range names {
		m.Components = append(m.Components, s.components[id].Component)
	}
	for _, e := range s.edges {
		m.Edges = append(m.Edges, e)
	}
	sort.Slice(m.Edges, func(i, j int) bool {
		return m.Edges[i].From+"\x00"+m.Edges[i].To < m.Edges[j].From+"\x00"+m.Edges[j].To
	})
	// A Job is a procedure the application ships, re-created from its
	// template. Its effect is unknown from the manifest; long is the seed and
	// the designer corrects it.
	seen := map[string]bool{}
	for _, name := range s.jobs {
		id := logicalName(name)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		m.Procedures = append(m.Procedures, appmap.Procedure{ID: id, Job: "Job/" + name, Effect: "long", Source: "manifest Job/" + name})
	}
	sort.Slice(m.Procedures, func(i, j int) bool { return m.Procedures[i].ID < m.Procedures[j].ID })
	return m
}

// entries lists the Service ports, other than metrics ports, of components
// that are not stores and are exposed outside the cluster or called by no
// other component.
func (s *seeder) entries() []Entry {
	called := map[string]bool{}
	for key := range s.edges {
		called[key[1]] = true
	}
	var out []Entry
	names := append([]string(nil), s.order...)
	sort.Strings(names)
	for _, id := range names {
		c := s.components[id]
		if c.Data != nil || c.engine != "" {
			continue
		}
		health := healthEndpoints(c.object)
		for _, name := range c.Services {
			svc := s.services[name]
			spec := mapValue(svc.object.Object["spec"])
			kind, _ := spec["type"].(string)
			var why []string
			if kind == "NodePort" || kind == "LoadBalancer" {
				why = append(why, kind)
			}
			why = append(why, s.ingress[name]...)
			if len(why) == 0 && !called[id] {
				why = append(why, "no other component calls it")
			}
			if len(why) == 0 {
				continue
			}
			for _, port := range listValue(spec["ports"]) {
				p := mapValue(port)
				number := intValue(p["port"])
				if number < 1 || metricsPort.MatchString(stringValue(p["name"])) {
					continue
				}
				out = append(out, Entry{Component: id, Service: name, Port: number, PortName: stringValue(p["name"]), Exposure: strings.Join(why, ", "), Health: health})
			}
		}
	}
	return out
}

// healthEndpoints lists the HTTP probes of the workload's main containers,
// with named ports resolved to numbers.
func healthEndpoints(o *unstructured.Unstructured) []string {
	pod, _ := rendered.PodSpec(o.Object)
	var out []string
	for _, value := range listValue(pod[rendered.Containers]) {
		container := mapValue(value)
		ports := map[string]int{}
		for _, port := range listValue(container["ports"]) {
			if name := stringValue(mapValue(port)["name"]); name != "" {
				ports[name] = intValue(mapValue(port)["containerPort"])
			}
		}
		for _, probe := range []string{"readiness", "liveness", "startup"} {
			get := mapValue(mapValue(container[probe+"Probe"])["httpGet"])
			path := stringValue(get["path"])
			if get == nil || path == "" {
				continue
			}
			port := intValue(get["port"])
			if port == 0 {
				port = ports[stringValue(get["port"])]
			}
			text := fmt.Sprintf("%s GET %s", probe, path)
			if port > 0 {
				text += " port " + strconv.Itoa(port)
			}
			out = appendUnique(out, text)
		}
	}
	return out
}

// regionHeader is the comment the seeded map opens with.
const regionHeader = `# Seeded by SDO from the rendered manifests, without application source
# (evidence: manifest). Review it: fix roles and edges you know better, add
# observed edges, then add operations and probes for the entry candidates,
# and the procedures the application ships (each Job is seeded as one).
`

// Region is the seeded map as an application-map region, markers included.
func (seed Seed) Region() string {
	var b strings.Builder
	b.WriteString(appmap.BeginMarker + "\n```yaml\n" + regionHeader)
	fmt.Fprintf(&b, "version: %d\n", seed.Map.Version)
	if len(seed.Map.Components) == 0 {
		b.WriteString("components: []\n")
	} else {
		b.WriteString("components:\n")
	}
	for _, c := range seed.Map.Components {
		fmt.Fprintf(&b, "  - {name: %s, workload: %s, services: [%s], holds_data: %t, evidence: [%s]", c.Name, quote(c.Workload), joinQuoted(c.Services), c.HoldsData, strings.Join(c.Evidence, ", "))
		if c.Data != nil {
			fmt.Fprintf(&b, ", data: {engine: %s, role: %s", c.Data.Engine, c.Data.Role)
			if len(c.Data.DerivedFrom) > 0 {
				fmt.Fprintf(&b, ", derived_from: [%s]", strings.Join(c.Data.DerivedFrom, ", "))
			}
			b.WriteString("}")
		}
		b.WriteString("}\n")
	}
	if len(seed.Map.Edges) == 0 {
		b.WriteString("edges: []\n")
	} else {
		b.WriteString("edges:\n")
	}
	for _, e := range seed.Map.Edges {
		fmt.Fprintf(&b, "  - {from: %s, to: %s, evidence: [%s], source: %s}\n", e.From, e.To, strings.Join(e.Evidence, ", "), quote(e.Source))
	}
	b.WriteString("operations: []\nprobes: []\n")
	if len(seed.Map.Procedures) > 0 {
		b.WriteString("procedures:\n")
	}
	for _, p := range seed.Map.Procedures {
		fmt.Fprintf(&b, "  - {id: %s, job: %s, effect: %s, source: %s}\n", p.ID, quote(p.Job), p.Effect, quote(p.Source))
	}
	b.WriteString("```\n" + appmap.EndMarker + "\n")
	return b.String()
}

// Summary is the Go-owned architecture text that introduces the seeded map:
// what it was built from and the entry candidates for the designer's probes.
func (seed Seed) Summary() string {
	var b strings.Builder
	b.WriteString("# Seeded application map\n\nGo seeded the application map at the end of this file from the rendered manifests alone (images, volumes, Services, env values and ConfigMap entries; evidence `manifest`). Where application source is shipped, it is the better authority.\n")
	if len(seed.Possible) > 0 {
		b.WriteString("\nPossible edges: a Secret reference (env var, Secret or key name) names the callee's Service, so the address is not in the manifests and Go did not read it. Confirm each on the live application (the caller's connections or logs) and map it, or leave it:\n")
		for _, e := range seed.Possible {
			b.WriteString(possibleEdgeLine(e) + "\n")
		}
	}
	if len(seed.Entries) == 0 {
		b.WriteString("\nNo entry candidates were found in the manifests.\n")
		return b.String()
	}
	b.WriteString("\nEntry candidates:\n")
	for _, e := range seed.Entries {
		port := strconv.Itoa(e.Port)
		if e.PortName != "" {
			port += " (" + e.PortName + ")"
		}
		fmt.Fprintf(&b, "- Service %s port %s, component %s: %s", e.Service, port, e.Component, e.Exposure)
		if len(e.Health) > 0 {
			b.WriteString("; health: " + strings.Join(e.Health, ", "))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func quote(text string) string {
	data, _ := json.Marshal(text)
	return string(data)
}

func joinQuoted(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = quote(item)
	}
	return strings.Join(quoted, ", ")
}

func mapValue(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func listValue(value any) []any {
	result, _ := value.([]any)
	return result
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func intValue(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

func appendUnique(list []string, value string) []string {
	for _, item := range list {
		if item == value {
			return list
		}
	}
	return append(list, value)
}
