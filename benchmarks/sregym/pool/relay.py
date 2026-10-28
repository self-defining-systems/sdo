"""Pool-owned systemd TCP relays; Docker configuration and live lanes stay online."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class RelayConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(pattern=r"^[a-z][a-z0-9-]{0,40}$")
    listen_host: str = "127.0.0.1"
    listen_port: int = Field(ge=1024, le=65535)
    upstream_host: str
    upstream_port: int = Field(ge=1, le=65535)

    @property
    def unit(self) -> str:
        return f"sdo-pool-registry-{self.name}.service"


def ensure_relay(config: RelayConfig, code_root: Path, state: Path) -> None:
    """Install an owned restartable service, then check the complete registry path."""
    state.mkdir(parents=True, exist_ok=True)
    spec = state / f"{config.name}.json"
    ready = state / f"{config.name}.ready.json"
    spec.write_text(config.model_dump_json(indent=2) + "\n")
    command = [sys.executable, "-m", "benchmarks.sregym.pool", "relay", "--spec", str(spec), "--ready", str(ready)]
    # systemd has its own quoting and treats '%' as a specifier.
    argv = " ".join('"' + part.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%") + '"' for part in command)
    unit = (
        "[Unit]\nDescription=SDO pool shared app registry relay\nAfter=network-online.target\n"
        f"[Service]\nWorkingDirectory={code_root}\nExecStart={argv}\n"
        "Restart=always\nRestartSec=2\nNice=10\n[Install]\nWantedBy=default.target\n"
    )
    destination = Path.home() / ".config/systemd/user" / config.unit
    destination.parent.mkdir(parents=True, exist_ok=True)
    changed = not destination.exists() or destination.read_text() != unit
    if changed:
        destination.write_text(unit)
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "--user", "enable", config.unit], check=True, capture_output=True)
    subprocess.run(["systemctl", "--user", "restart" if changed else "start", config.unit], check=True)
    host = "127.0.0.1" if config.listen_host == "0.0.0.0" else config.listen_host
    # Retry startup and restart a service that stayed alive with a broken transport.
    import time

    for attempt in range(10):
        try:
            state_result = subprocess.run(
                ["systemctl", "--user", "show", config.unit, "--property=ActiveState,MainPID"],
                check=True,
                capture_output=True,
                text=True,
            )
            fields = dict(line.split("=", 1) for line in state_result.stdout.splitlines() if "=" in line)
            if fields.get("ActiveState") != "active" or int(fields.get("MainPID", "0")) <= 0:
                raise OSError("owned relay unit is not running")
            if not ready.is_file() or json.loads(ready.read_text()).get("pid") != int(fields["MainPID"]):
                raise OSError("owned relay has not bound its listening socket")
            with urllib.request.urlopen(f"http://{host}:{config.listen_port}/v2/", timeout=2) as response:
                if response.status == 200:
                    # Bind failures can be asynchronous immediately after systemctl start.
                    time.sleep(0.2)
                    if subprocess.run(["systemctl", "--user", "is-active", "--quiet", config.unit]).returncode:
                        raise OSError("owned relay exited during health check")
                    return
        except OSError:
            if attempt == 4:
                subprocess.run(["systemctl", "--user", "restart", config.unit], check=True)
        time.sleep(1)
    raise RuntimeError(f"pool: registry relay {config.unit} has no healthy upstream")


async def serve(config: RelayConfig, ready: Path | None = None) -> None:
    async def connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream = None
        try:
            incoming, upstream = await asyncio.wait_for(
                asyncio.open_connection(config.upstream_host, config.upstream_port), timeout=10
            )

            async def copy(source: asyncio.StreamReader, target: asyncio.StreamWriter) -> None:
                while chunk := await source.read(256 * 1024):
                    target.write(chunk)
                    await target.drain()
                if target.can_write_eof():
                    target.write_eof()

            await asyncio.gather(copy(reader, upstream), copy(incoming, writer))
        except (OSError, TimeoutError, ConnectionError):
            pass
        finally:
            writer.close()
            if upstream is not None:
                upstream.close()

    server = await asyncio.start_server(connection, config.listen_host, config.listen_port)
    if ready is not None:
        ready.write_text(json.dumps({"pid": os.getpid()}) + "\n")
    async with server:
        await server.serve_forever()
