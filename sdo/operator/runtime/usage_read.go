package runtime

import (
	"fmt"
	"io"
)

func readUsageBounded(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxUsageBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxUsageBytes {
		return nil, fmt.Errorf("kubelet usage response exceeds %d bytes", maxUsageBytes)
	}
	return data, nil
}
