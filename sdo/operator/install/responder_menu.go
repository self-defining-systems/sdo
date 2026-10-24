package install

func boolMenuValue(enabled bool) int {
	if enabled {
		return 1
	}
	return 0
}
