package bench

import (
	"encoding/json"
	"fmt"
	"strings"
)

type faultKind int

const (
	singleFault faultKind = iota + 1
	compositeFault
)

// faultCatalog reads the ID indexes emitted by fuzzops. Only fuzzops can
// compute canonical IDs because parameter defaults come from its templates.
func faultCatalog(path string) (map[string]faultKind, error) {
	var f struct {
		IDs          []string          `json:"ids"`
		CompositeIDs []string          `json:"composite_ids"`
		Composites   []json.RawMessage `json:"composites"`
	}
	if err := readJSON(path, &f); err != nil {
		return nil, err
	}
	if len(f.CompositeIDs) != len(f.Composites) {
		return nil, fmt.Errorf("composites require a matching composite_ids index; regenerate with fuzzops composites expand")
	}
	index := make(map[string]faultKind, len(f.IDs)+len(f.CompositeIDs))
	for _, id := range f.IDs {
		index[id] = singleFault
	}
	for _, id := range f.CompositeIDs {
		switch index[id] {
		case singleFault:
			return nil, fmt.Errorf("fault ID %q appears in both ids and composite_ids", id)
		case compositeFault:
			return nil, fmt.Errorf("duplicate composite ID %q", id)
		}
		index[id] = compositeFault
	}
	return index, nil
}

func checkIDs(path string, ids []string) error {
	index, err := faultCatalog(path)
	if err != nil {
		return err
	}
	var missing []string
	for _, id := range ids {
		if id != Healthy && index[id] == 0 {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("fault IDs not in the instances file: %s", strings.Join(missing, ", "))
	}
	return nil
}

func (rs RunSpec) faultFlags() ([]string, error) {
	if rs.Fault == Healthy {
		return []string{"--fault", "none"}, nil
	}
	index, err := faultCatalog(rs.Instances)
	if err != nil {
		return nil, err
	}
	switch index[rs.Fault] {
	case singleFault:
		return []string{"--instance-file", rs.Instances, "--instance", rs.Fault}, nil
	case compositeFault:
		return []string{"--composite-file", rs.Instances, "--composite", rs.Fault}, nil
	default:
		return nil, fmt.Errorf("fault ID %q is absent from %s", rs.Fault, rs.Instances)
	}
}
