package runtime

// lockedLines is the existing race-safe runtime output fixture. Its string
// writer avoids exposing a concurrently mutated bytes.Buffer to the driver.
type restartClassOutput struct{ lines *lockedLines }

func (w restartClassOutput) Write(p []byte) (int, error) {
	w.lines.mu.Lock()
	defer w.lines.mu.Unlock()
	w.lines.lines = append(w.lines.lines, string(p))
	return len(p), nil
}
func newRestartClassOutput() (*lockedLines, restartClassOutput) {
	lines := &lockedLines{}
	return lines, restartClassOutput{lines: lines}
}
