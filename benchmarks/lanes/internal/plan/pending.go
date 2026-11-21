package plan

// PendingSpecs are the rendered specs of the nodes of the plan in dir that still have to be released: waiting, whether
// blocked on an incoming edge or sent back to waiting for a retry. The platform reads them to know which hosts and lanes
// the plan will want, so a lane is not deleted while a node that could use it is only waiting for its turn. A plan that
// is complete or removed wants nothing. A paused plan still wants its lanes (it can resume). Artifacts an edge has not
// delivered yet render as empty strings; only the fields that place the node (hosts, selector, shape) are read.
func PendingSpecs(dir string) ([][]byte, error) {
	e, err := Open(dir, nil, nil)
	if err != nil {
		return nil, err
	}
	st, err := LoadState(dir)
	if err != nil {
		return nil, err
	}
	ctl, err := LoadControl(dir)
	if err != nil {
		return nil, err
	}
	if st.Complete || ctl.Removed || ctl.Purge {
		return nil, nil
	}
	var out [][]byte
	for _, n := range st.Nodes {
		if n.Status != NodeWaiting {
			continue
		}
		arts := map[string]string{}
		for _, ed := range e.incoming(n.Arm) {
			if ed.Artifact != "" {
				arts[ed.Artifact] = ""
			}
		}
		spec, _, err := e.Render(Node{ID: n.ID, Arm: n.Arm, Item: n.Item, Rep: n.Rep, Index: n.Index}, n.Attempt+1, arts)
		if err != nil {
			continue
		}
		out = append(out, spec)
	}
	return out, nil
}
