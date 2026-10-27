package runtime

// namedPlanLists recognizes only unambiguous named lists. Other arrays retain
// positional matching; duplicate or missing names never establish ownership.
func namedPlanLists(before, after, live []any) (map[string]any, map[string]any, map[string]any, bool) {
	index := func(list []any) (map[string]any, bool) {
		items := map[string]any{}
		for _, value := range list {
			item, ok := value.(map[string]any)
			if !ok {
				return nil, false
			}
			name, ok := item["name"].(string)
			if !ok || name == "" {
				return nil, false
			}
			if _, duplicate := items[name]; duplicate {
				return nil, false
			}
			items[name] = value
		}
		return items, true
	}
	b, bok := index(before)
	a, aok := index(after)
	l, lok := index(live)
	return b, a, l, bok && aok && lok
}
