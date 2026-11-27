package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

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

// ClientLinkLoad is the vantage load Go's client dials may use, in probe runs
// per vantage.MinInterval: each link is one vantage probe, and vantage probes
// share the vantage.MaxProbes load budget with the designer's own, so Go takes
// half of it. Every mapped link is dialed; more links mean longer intervals
// (readMultiples), not fewer links.
const ClientLinkLoad = vantage.MaxProbes / 2

// MaxClientLinks bounds the client dials Go declares: half of
// vantage.MaxDeclaredProbes. Only past it does ranking decide which exist.
const MaxClientLinks = vantage.MaxDeclaredProbes / 2

// plannedClientLinks lists the links of the link-probe workloads whose
// calling Service belongs to a map component with a workload: links whose two
// ends a probed user operation depends on first, then links from entry
// components, then by edge. Their intervals fit ClientLinkLoad with the
// probed links dialed most often. The prober dials those links from its own
// Pod; these dial them from the client, so a loss or partition on the
// client's side of an edge is seen too.
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
	covered := func(service string) appmap.ComponentCoverage { return coverage.Components[owner[service].Name] }
	probed := func(l vantage.ClientLink) bool { return covered(l.Link.From).Probed() && covered(l.Link.To).Probed() }
	callerEntry := func(l vantage.ClientLink) bool { return covered(l.Link.From).Entry }
	sort.SliceStable(links, func(i, j int) bool {
		if a, b := probed(links[i]), probed(links[j]); a != b {
			return a
		}
		if a, b := callerEntry(links[i]), callerEntry(links[j]); a != b {
			return a
		}
		return links[i].ID < links[j].ID
	})
	if len(links) > MaxClientLinks {
		links = links[:MaxClientLinks]
	}
	onPath := 0
	for _, l := range links {
		if probed(l) {
			onPath++
		}
	}
	kp, ku := readMultiples(onPath, len(links)-onPath, ClientLinkLoad)
	for i := range links {
		links[i].Interval = time.Duration(ku) * vantage.MinInterval
		if probed(links[i]) {
			links[i].Interval = time.Duration(kp) * vantage.MinInterval
		}
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
		fmt.Fprintf(&b, "\t\t{ID: %q, From: podread.Target{Workload: %q}, Link: traffic.Link{From: %q, To: %q, Port: %d, Protocol: traffic.LinkProtocol(%q), HTTP: %s}, Interval: %d * time.Second},\n",
			l.ID, l.From.Workload, l.Link.From, l.Link.To, l.Link.Port, string(l.Link.Protocol), httpOptions, int(l.Interval.Seconds()))
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
