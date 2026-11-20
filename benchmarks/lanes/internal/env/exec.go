package env

import "context"

// Exec runs a local command (go build, docker, a deploy script) through the
// Env runner so tests can fake it.
func (e Env) Exec(ctx context.Context, name string, args ...string) ([]byte, error) {
	return e.exec(ctx, nil, name, args...)
}

// NodeExecStdin is NodeExec with stdin (docker exec -i), used to push a file
// into a kind node: NodeExecStdin(ctx, node, data, "sh", "-c", "cat > /path").
func (e Env) NodeExecStdin(ctx context.Context, node string, stdin []byte, args ...string) ([]byte, error) {
	if !e.HasNodes() {
		return nil, ErrNoNodes
	}
	if stdin == nil {
		stdin = []byte{}
	}
	return e.exec(ctx, stdin, "docker", append([]string{"exec", "-i", node}, args...)...)
}
