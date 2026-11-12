package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/vantage"
)

// ClientLinksDetectorID names the detector Go installs to dial each mapped
// link from inside its calling component's Pods.
const ClientLinksDetectorID = "client-links"

// MaxClientLinks bounds the client dials Go installs. Each is one vantage
// probe, and vantage probes share vantage.MaxProbes with the designer's own,
// so Go takes at most half of that budget.
const MaxClientLinks = vantage.MaxProbes / 2

// plannedClientLinks lists the links of the link-probe workloads whose
// calling Service belongs to a map component with a workload, links from
// entry components first, then by edge, capped at MaxClientLinks. The prober
// dials those links from its own Pod; these dial them from the client, so a
// loss or partition on the client's side of an edge is seen too.
func plannedClientLinks(root string) []vantage.ClientLink {
	parsed, coverage, ok := acceptedMap(root)
	if !ok {
		return nil
	}
	owner := map[string]appmap.Component{}
	for _, component := range parsed.Components {
		for _, service := range component.Services {
			owner[service] = component
		}
	}
	directory := filepath.Join(root, layout.TrafficWorkloadsDir)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var links []vantage.ClientLink
	for _, file := range entries {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".yaml") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(directory, file.Name()))
		if err != nil {
			continue
		}
		workload, err := parseTrafficWorkload(file.Name(), content)
		if err != nil || workload.GetPurpose() != string(traffic.PurposeLinkProbe) {
			continue
		}
		for _, l := range workload.GetLinks() {
			component, ok := owner[l.GetFrom()]
			target := podread.Target{Workload: component.Workload}
			if !ok || target.Validate() != nil {
				continue
			}
			link := traffic.Link{From: l.GetFrom(), To: l.GetTo(), Port: int(l.GetPort()), Protocol: traffic.LinkProtocol(l.GetProtocol())}
			if path := l.GetHttp().GetPath(); path != "" {
				link.HTTP = &traffic.LinkHTTPOptions{Path: path}
			}
			if seen[link.ID()] {
				continue
			}
			seen[link.ID()] = true
			links = append(links, vantage.ClientLink{ID: "client." + link.ID(), From: target, Link: link})
		}
	}
	callerEntry := func(l vantage.ClientLink) bool { return coverage.Components[owner[l.Link.From].Name].Entry }
	sort.SliceStable(links, func(i, j int) bool {
		if a, b := callerEntry(links[i]), callerEntry(links[j]); a != b {
			return a
		}
		return links[i].ID < links[j].ID
	})
	if len(links) > MaxClientLinks {
		links = links[:MaxClientLinks]
	}
	if _, err := vantage.NewClientLinksDetector(sdk.DetectorSpec{ID: ClientLinksDetectorID, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}}, links, vantage.MinInterval); err != nil {
		return nil
	}
	return links
}

// prepareClientLinksDetector installs the client-links detector when a link
// resolves to a client workload and removes Go's package otherwise.
func prepareClientLinksDetector(root string) error {
	links := plannedClientLinks(root)
	if len(links) == 0 {
		return removeBootstrapDetector(root, ClientLinksDetectorID)
	}
	manifest := genericManifest(ClientLinksDetectorID, vantage.MinInterval.String(), []layout.Watch{{APIVersion: "v1", Kind: "Pod"}})
	// A slow or lossy client link is a warning finding, as the prober's are.
	manifest.Batching.Severity = string(sdk.SeverityWarning)
	return writeGenericDetector(root, manifest, clientLinksSource(links))
}

func clientLinksSource(links []vantage.ClientLink) string {
	var b strings.Builder
	for _, l := range links {
		httpOptions := "nil"
		if l.Link.HTTP != nil {
			httpOptions = fmt.Sprintf("&traffic.LinkHTTPOptions{Path: %q}", l.Link.HTTP.Path)
		}
		fmt.Fprintf(&b, "\t\t{ID: %q, From: podread.Target{Workload: %q}, Link: traffic.Link{From: %q, To: %q, Port: %d, Protocol: traffic.LinkProtocol(%q), HTTP: %s}},\n",
			l.ID, l.From.Workload, l.Link.From, l.Link.To, l.Link.Port, string(l.Link.Protocol), httpOptions)
	}
	return fmt.Sprintf(`package clientlinks

import (
	"time"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/vantage"
)

// New dials each mapped link from inside its calling component's Pods.
func New() sdk.Detector {
	d, err := vantage.NewClientLinksDetector(sdk.DetectorSpec{
		%s
		Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
		Interval: %d * time.Second,
	}, []vantage.ClientLink{
%s	}, %d*time.Second)
	if err != nil {
		panic(err)
	}
	return d
}
`, genericSpecWithSeverity(ClientLinksDetectorID, "Warning"), int(vantage.MinInterval.Seconds()), b.String(), int(vantage.MinInterval.Seconds()))
}
