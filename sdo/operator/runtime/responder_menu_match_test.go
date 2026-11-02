package runtime

import (

)

// matchOptions lists the program options; the menu also carries generic actions.
func matchOptions(response ToolResponse, programID string) []string {
	var ids []string
	for _, option := range response.Menu.GetOptions() {
		if option.ProgramId == programID {
			ids = append(ids, option.OptionId)
		}
	}
	return ids
}

