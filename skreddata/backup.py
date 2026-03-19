from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import BinaryIO

from .database import Database


DEFAULT_CONTAINER_RUNTIME = "podman"


def _resolve_container(container: str | None) -> str | None:
    if container not in (None, ""):
        return container

    value = os.environ.get("SKREDDATA_MONGO_CONTAINER")
    return value or None


def _mongo_tool_command(tool: str, db: Database, *, container: str | None) -> list[str]:
    resolved_container = _resolve_container(container)
    if resolved_container is not None:
        runtime = os.environ.get("SKREDDATA_CONTAINER_RUNTIME", DEFAULT_CONTAINER_RUNTIME)
        return [runtime, "exec", "-i", resolved_container, tool]

    tool_path = shutil.which(tool)
    if tool_path is None:
        raise FileNotFoundError(
            f"{tool} is not available on PATH. Install MongoDB database tools or set SKREDDATA_MONGO_CONTAINER."
        )

    return [
        tool_path,
        "--host",
        str(db.host),
        "--port",
        str(db.port),
    ]


def _run_command(command: list[str], *, stdin: BinaryIO | None = None, stdout: BinaryIO | None = None) -> None:
    try:
        subprocess.run(
            command,
            check=True,
            stdin=stdin,
            stdout=stdout,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as exc:
        message = exc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(message or f"Command failed: {' '.join(command)}") from exc


def backup_database(
    archive_file: str | Path,
    db: Database,
    *,
    container: str | None = None,
) -> Path:
    archive_file = Path(archive_file)
    archive_file.parent.mkdir(parents=True, exist_ok=True)

    command = _mongo_tool_command("mongodump", db, container=container)
    command.extend(
        [
            "--db",
            str(db.database),
            "--collection",
            str(db.collection),
            "--archive",
            "--gzip",
        ]
    )

    with archive_file.open("wb") as handle:
        _run_command(command, stdout=handle)

    return archive_file


def restore_backup(
    archive_file: str | Path,
    db: Database,
    *,
    container: str | None = None,
    drop: bool = False,
) -> None:
    archive_file = Path(archive_file)

    command = _mongo_tool_command("mongorestore", db, container=container)
    command.extend(
        [
            "--db",
            str(db.database),
            "--collection",
            str(db.collection),
            "--archive",
            "--gzip",
        ]
    )
    if drop:
        command.append("--drop")

    with archive_file.open("rb") as handle:
        _run_command(command, stdin=handle)
