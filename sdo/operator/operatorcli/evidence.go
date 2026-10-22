package operatorcli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sdo.dev/operator/apihistory"
)

type observedState struct {
	Unknown        bool            `json:"unknown"`
	Stale          bool            `json:"stale"`
	Reason         string          `json:"reason"`
	RequestedAt    time.Time       `json:"requested_at,omitempty"`
	ObservedAt     time.Time       `json:"observed_at,omitempty"`
	Origin         string          `json:"origin"`
	Complete       bool            `json:"complete"`
	Dropped        uint64          `json:"dropped"`
	Event          uint64          `json:"event,omitempty"`
	ObservedAbsent bool            `json:"observed_absent,omitempty"`
	LastObserved   json.RawMessage `json:"last_observed,omitempty"`
}

// RunEvidence is an offline reader. It has no cluster client or writer capability.
func RunEvidence(args []string, getenv func(string) string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("evidence requires history, show, or state <Kind/namespace/name> --at <RFC3339|before-first-mutation|after-fix>")
	}
	command := args[0]
	args = args[1:]
	object := ""
	if command == "state" {
		if len(args) == 0 {
			return errors.New("state requires Kind/namespace/name")
		}
		object = args[0]
		args = args[1:]
	}
	flags := flag.NewFlagSet("evidence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	directory := flags.String("directory", getenv("SDO_API_HISTORY_DIR"), "read-only history directory")
	incident := flags.String("incident", "", "expected incident identity")
	number := flags.Uint64("event", 0, "event sequence")
	offset := flags.Int("offset", 0, "history offset")
	limit := flags.Int("limit", 100, "history page size, at most 1000")
	at := flags.String("at", "", "observation cut")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected evidence arguments")
	}
	if *directory == "" {
		return errors.New("SDO_API_HISTORY_DIR or --directory required")
	}
	if *offset < 0 || *limit < 1 || *limit > 1000 {
		return errors.New("offset must be nonnegative and limit 1..1000")
	}
	enc := json.NewEncoder(out)
	h, err := apihistory.Read(*directory)
	if err != nil {
		return enc.Encode(observedState{Unknown: true, Stale: true, Origin: "responder_api_proxy", Reason: "history unavailable"})
	}
	if *incident != "" && *incident != h.Manifest.Incident {
		return errors.New("history incident mismatch")
	}
	switch command {
	case "history":
		start := min(*offset, len(h.Events))
		end := min(start+*limit, len(h.Events))
		return enc.Encode(struct {
			Manifest apihistory.Manifest `json:"manifest"`
			Events   []apihistory.Event  `json:"events"`
			Next     int                 `json:"next_offset"`
		}{h.Manifest, h.Events[start:end], end})
	case "show":
		for _, e := range h.Events {
			if e.Sequence == *number {
				body := h.Body(e)
				var projected json.RawMessage
				var tail string
				if e.BodyFormat == "projected_json" {
					projected = body
				} else if e.BodyFormat == "log_tail" {
					tail = string(body)
				}
				return enc.Encode(struct {
					Event           apihistory.Event    `json:"event"`
					Body            json.RawMessage     `json:"body,omitempty"`
					LogTail         string              `json:"log_tail,omitempty"`
					BodyUnavailable bool                `json:"body_unavailable"`
					Manifest        apihistory.Manifest `json:"manifest"`
				}{e, projected, tail, e.BodyHash != "" && len(body) == 0, h.Manifest})
			}
		}
		return fmt.Errorf("Unknown: event %d was not recorded", *number)
	case "state":
		return enc.Encode(stateAt(h, *directory, object, *at))
	default:
		return fmt.Errorf("unknown evidence command %q", command)
	}
}
func stateAt(h apihistory.History, directory, object, at string) observedState {
	s := observedState{Unknown: true, Stale: true, Origin: "responder_api_proxy", Complete: h.Manifest.Complete && !h.Manifest.Open, Dropped: h.Manifest.Dropped, Reason: "last observed only; watches are metadata-only, continuity is Unknown"}
	parts := strings.Split(object, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		s.Reason = "object must be Kind/namespace/name"
		return s
	}
	var cut, after time.Time
	switch at {
	case "before-first-mutation":
		if !h.Manifest.Complete || h.Manifest.Open {
			s.Reason = "Unknown: mutation boundary incomplete"
			return s
		}
		for _, e := range h.Events {
			if e.Phase == "start" && !e.DryRun && isMutation(e) && (cut.IsZero() || e.Started.Before(cut)) {
				cut = e.Started
			}
		}
		if cut.IsZero() {
			s.Reason = "Unknown: no first mutation boundary recorded"
			return s
		}
	case "after-fix":
		var marker struct {
			VerifiedAt time.Time `json:"verified_at"`
		}
		b, err := os.ReadFile(filepath.Join(directory, "recovery.json"))
		if err != nil || json.Unmarshal(b, &marker) != nil || marker.VerifiedAt.IsZero() {
			s.Reason = "Unknown: independently verified recovery marker unavailable"
			return s
		}
		after = marker.VerifiedAt
		for _, e := range h.Events {
			if e.Finished.After(cut) {
				cut = e.Finished
			}
		}
		if cut.Before(after) {
			s.Reason = "Unknown: no post-recovery observation"
			return s
		}
	default:
		var err error
		cut, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			s.Reason = "Unknown: --at requires RFC3339, before-first-mutation, or after-fix"
			return s
		}
	}
	s.RequestedAt = cut
	for _, e := range h.Events {
		if e.Phase != "end" || e.Finished.IsZero() || e.Finished.After(cut) || (!after.IsZero() && e.Started.Before(after)) || e.Finished.Before(s.ObservedAt) || e.Subresource != "" || e.DryRun {
			continue
		}
		if e.Namespace != parts[1] {
			continue
		}
		if e.Status == 404 && e.Name == parts[2] && resourceMatches(parts[0], e.Resource) && e.Verb == "get" {
			s.ObservedAt = e.Finished
			s.ObservedAbsent = true
			s.Event = e.Sequence
			s.LastObserved = nil
			continue
		}
		if e.Status < 200 || e.Status >= 300 || e.BodyFormat != "projected_json" {
			continue
		}
		body := h.Body(e)
		if len(body) == 0 {
			continue
		}
		var o struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
				Continue  string `json:"continue"`
			} `json:"metadata"`
			Items []json.RawMessage `json:"items"`
		}
		if json.Unmarshal(body, &o) != nil {
			continue
		}
		var match json.RawMessage
		if o.Metadata.Name == parts[2] && o.Metadata.Namespace == parts[1] && (strings.EqualFold(parts[0], o.Kind) || resourceMatches(parts[0], e.Resource)) {
			match = body
		}
		if strings.HasSuffix(o.Kind, "List") {
			if !strings.EqualFold(parts[0], strings.TrimSuffix(o.Kind, "List")) && !resourceMatches(parts[0], e.Resource) {
				continue
			}
			for _, item := range o.Items {
				var itemMeta struct {
					Metadata struct {
						Name      string `json:"name"`
						Namespace string `json:"namespace"`
					} `json:"metadata"`
				}
				if json.Unmarshal(item, &itemMeta) == nil && itemMeta.Metadata.Name == parts[2] && itemMeta.Metadata.Namespace == parts[1] {
					match = item
					break
				}
			}
			// Only a complete, unfiltered namespace list can observe absence.
			if match == nil && e.Verb == "list" && !e.Partial && o.Metadata.Continue == "" {
				s.ObservedAt = e.Finished
				s.ObservedAbsent = true
				s.LastObserved = nil
				s.Event = e.Sequence
				continue
			}
		}
		if match != nil {
			s.ObservedAt = e.Finished
			s.ObservedAbsent = false
			s.LastObserved = match
			s.Event = e.Sequence
		}
	}
	if s.ObservedAt.IsZero() {
		s.Reason = "Unknown: no compatible observation at this cut"
	}
	return s
}
func resourceMatches(kind, resource string) bool {
	return strings.EqualFold(kind, resource) || strings.EqualFold(kind+"s", resource) || strings.EqualFold(strings.TrimSuffix(kind, "y")+"ies", resource)
}
func isMutation(e apihistory.Event) bool {
	switch e.Verb {
	case "post", "put", "patch", "delete":
		return true
	}
	return e.Subresource == "exec" || e.Subresource == "attach" || e.Subresource == "portforward"
}
