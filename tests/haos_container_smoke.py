"""Black-box real-image checks; synthetic children never reach real devices."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import time

IMAGE = "casambi-haos:smoke"
ROOT = Path(__file__).resolve().parents[1]


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args], text=True, capture_output=True, timeout=240, check=check
    )


def python_in(name: str, code: str, *, check: bool = True):
    return docker("exec", name, "python", "-c", code, check=check)


def wait_python(name: str, code: str, timeout: float = 40) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if python_in(name, code, check=False).returncode == 0:
            return
        time.sleep(0.5)
    raise AssertionError(
        f"Timed out waiting for {name}: {docker('logs', name, check=False).stdout}"
    )


LIVENESS = (
    "import urllib.request;from pathlib import Path; "
    'assert urllib.request.urlopen("http://127.0.0.1:8099/supervisor-health",timeout=2).status==200; '
    'assert any(line.split()[1]==f"00000000:{8099:04X}" and line.split()[3]=="0A" '
    'for line in Path("/proc/net/tcp").read_text().splitlines()[1:])'
)
NO_CHILD = (
    "from pathlib import Path; "
    'assert not any(b"/app/server.py" in p.read_bytes().split(b"\\0") '
    'for p in Path("/proc").glob("[0-9]*/cmdline") if p.exists())'
)
LATCHED = (
    "import json;from pathlib import Path; "
    's=json.loads(Path("/data/recovery/state.json").read_text());assert s["latched"]'
)


def main() -> None:
    names = []
    with tempfile.TemporaryDirectory(prefix="casambi-image-test-") as directory:
        root = Path(directory)
        try:

            def start(name: str, data: Path, fixture: Path | None = None) -> None:
                data.mkdir()
                args = [
                    "run",
                    "--detach",
                    "--name",
                    name,
                    "--network",
                    "none",
                    "--volume",
                    f"{data}:/data",
                ]
                if fixture:
                    args.extend(["--volume", f"{fixture}:/app/server.py:ro"])
                args.append(IMAGE)
                docker(*args)
                names.append(name)

            # Disabled outer controller stays alive; no radio/broker/bridge child.
            name = "casambi-disabled-smoke"
            start(name, root / "disabled")
            wait_python(name, LIVENESS)
            python_in(name, NO_CHILD)
            docker("stop", "--time", "25", name)
            assert (
                docker(
                    "inspect", "--format", "{{.State.ExitCode}}", name
                ).stdout.strip()
                == "0"
            )
            print(
                "Persistent disabled controller: liveness OK, zero bridge children, clean SIGTERM"
            )

            # Invalid private config latches; native container restart cannot bypass it.
            name = "casambi-invalid-smoke"
            data = root / "invalid"
            start(name, data)
            docker("stop", "--time", "25", name)
            (data / "options.json").write_text(
                json.dumps(
                    {"mode": "bridge", "env": "", "recovery_reset_generation": 0}
                )
            )
            docker("start", name)
            wait_python(name, LIVENESS)
            wait_python(name, LATCHED)
            python_in(name, NO_CHILD)
            python_in(name, LATCHED + ';assert s["attempts"]==0')
            docker("restart", "--time", "25", name)
            wait_python(name, LIVENESS)
            python_in(name, LATCHED + ';assert s["attempts"]==0')
            python_in(name, NO_CHILD)
            print(
                "Missing configuration: durable latch, zero child launches, safe after container restart"
            )

            # Explicit synthetic fixture: just append a marker and exit75, never import BLE.
            fixture = root / "synthetic_exit75.py"
            fixture.write_text(
                "from pathlib import Path\nimport os\n"
                'with Path("/data/synthetic-launches").open("a") as f:\n'
                ' f.write("synthetic child\\n");f.flush();os.fsync(f.fileno())\n'
                "raise SystemExit(75)\n"
            )
            data = root / "failures"
            data.mkdir()
            env = "\n".join(
                [
                    "MQTT_BROKER=127.0.0.1",
                    "MQTT_PORT=1883",
                    "MQTT_USERNAME=synthetic",
                    "MQTT_PASSWORD=synthetic",
                    "CASAMBI_NETWORK_ADDRESS=00:11:22:33:44:55",
                    "CASAMBI_NETWORK_PASSWORD=synthetic",
                    "CASAMBI_NETWORK_NAME=synthetic",
                ]
            )
            (data / "options.json").write_text(
                json.dumps(
                    {"mode": "bridge", "env": env, "recovery_reset_generation": 0}
                )
            )
            name = "casambi-three-failure-smoke"
            docker(
                "run",
                "--detach",
                "--name",
                name,
                "--network",
                "none",
                "--volume",
                f"{data}:/data",
                "--volume",
                f"{fixture}:/app/server.py:ro",
                IMAGE,
            )
            names.append(name)
            wait_python(name, LIVENESS)
            wait_python(name, LATCHED + ';assert s["attempts"]==3', timeout=180)
            python_in(name, NO_CHILD)
            python_in(
                name,
                "from pathlib import Path; "
                'assert len(Path("/data/synthetic-launches").read_text().splitlines()) == 4',
            )
            docker("restart", "--time", "25", name)
            wait_python(name, LIVENESS)
            time.sleep(5)
            python_in(name, LATCHED + ';assert s["attempts"]==3')
            python_in(name, NO_CHILD)
            python_in(
                name,
                "from pathlib import Path; "
                'assert len(Path("/data/synthetic-launches").read_text().splitlines()) == 4',
            )
            print(
                "Synthetic exit75: original+exactly3 replacements, no fifth launch after container restart"
            )
        finally:
            for name in names:
                docker("rm", "--force", name, check=False)
            # Recovery state is intentionally root-owned0700/0600 in the image.
            # Remove only this test's synthetic temporary data before host cleanup.
            docker(
                "run",
                "--rm",
                "--network",
                "none",
                "--entrypoint",
                "python",
                "--volume",
                f"{root}:/cleanup",
                IMAGE,
                "-c",
                "import shutil;from pathlib import Path; "
                "[shutil.rmtree(p) if p.is_dir() else p.unlink() "
                'for p in Path("/cleanup").iterdir()]',
                check=False,
            )


if __name__ == "__main__":
    main()
