"""JSON-lines control pipe for Go tests using the existing fastloop HTTP fake."""

import json
import sys

from benchmarks.sregym.fastloop.fake_kube import FakeKubernetesAPIServer


def main() -> None:
    snapshot = json.loads(sys.stdin.readline())
    with FakeKubernetesAPIServer(snapshot) as server:
        host, port = server._http.server_address[:2]  # test bridge only
        print(json.dumps({"url": f"http://{host}:{port}"}), flush=True)
        for line in sys.stdin:
            request = json.loads(line)
            if "plural" in request:
                server.set_object(request["plural"], request["object"])
            else:
                server.set_kubelet(request["node"], request["summary"], request.get("cadvisor", ""))
            print("{}", flush=True)


if __name__ == "__main__":
    main()
