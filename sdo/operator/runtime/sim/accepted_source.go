package sim

import "strings"

func resources2kind(plural string) string { return resources[plural] }

func readAll(body interface{ Read([]byte) (int, error) }) []byte {
	var out strings.Builder
	buffer := make([]byte, 4096)
	for {
		n, err := body.Read(buffer)
		out.Write(buffer[:n])
		if err != nil {
			return []byte(out.String())
		}
	}
}
