from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import typer

from skreddata import Database, Item, backup_database, restore_backup


app = typer.Typer(
    name="skreddata",
    no_args_is_help=True,
    help="CLI for the skreddata Mongo database.",
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=True,
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)


def _make_database(
    host: str | None,
    port: int | None,
    database: str | None,
    collection: str | None,
) -> Database:
    return Database(host=host, port=port, database=database, collection=collection)


def _json_default(value: object) -> str:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _echo_json(value: dict | list) -> None:
    typer.echo(json.dumps(value, indent=2, sort_keys=True, default=_json_default))


def _item_payload(item: Item) -> dict:
    return item.asdict()


def _validate_count_filters(label: int | None, labeled: bool, unlabeled: bool) -> None:
    if labeled and unlabeled:
        raise typer.BadParameter("Use at most one of --labeled and --unlabeled.")
    if label is not None and (labeled or unlabeled):
        raise typer.BadParameter("Use --label by itself.")


def _validate_list_filters(label: int | None, unlabeled: bool) -> None:
    if label is not None and unlabeled:
        raise typer.BadParameter("Use either --label or --unlabeled, not both.")


@app.command()
def ping(
    host: str | None = typer.Option(None, "--host", envvar="SKREDDATA_MONGO_HOST", help="Mongo host."),
    port: int | None = typer.Option(None, "--port", envvar="SKREDDATA_MONGO_PORT", help="Mongo port."),
    database: str | None = typer.Option(
        None,
        "--database",
        envvar="SKREDDATA_MONGO_DATABASE",
        help="Mongo database name.",
    ),
    collection: str | None = typer.Option(
        None,
        "--collection",
        envvar="SKREDDATA_MONGO_COLLECTION",
        help="Mongo collection name.",
    ),
) -> None:
    db = _make_database(host, port, database, collection)
    _echo_json(db.ping())


@app.command()
def count(
    label: int | None = typer.Option(None, "--label", help="Count items with a specific label."),
    labeled: bool = typer.Option(False, "--labeled", help="Count labeled items only."),
    unlabeled: bool = typer.Option(False, "--unlabeled", help="Count unlabeled items only."),
    host: str | None = typer.Option(None, "--host", envvar="SKREDDATA_MONGO_HOST", help="Mongo host."),
    port: int | None = typer.Option(None, "--port", envvar="SKREDDATA_MONGO_PORT", help="Mongo port."),
    database: str | None = typer.Option(
        None,
        "--database",
        envvar="SKREDDATA_MONGO_DATABASE",
        help="Mongo database name.",
    ),
    collection: str | None = typer.Option(
        None,
        "--collection",
        envvar="SKREDDATA_MONGO_COLLECTION",
        help="Mongo collection name.",
    ),
) -> None:
    _validate_count_filters(label, labeled, unlabeled)
    db = _make_database(host, port, database, collection)

    if label is not None:
        result = db.get_length_with_label(label)
    elif labeled:
        result = db.get_length_labeled()
    elif unlabeled:
        result = db.get_length_unlabeled()
    else:
        result = db.get_length()

    typer.echo(result)


@app.command()
def get(
    uuid: str = typer.Argument(..., help="Item UUID."),
    host: str | None = typer.Option(None, "--host", envvar="SKREDDATA_MONGO_HOST", help="Mongo host."),
    port: int | None = typer.Option(None, "--port", envvar="SKREDDATA_MONGO_PORT", help="Mongo port."),
    database: str | None = typer.Option(
        None,
        "--database",
        envvar="SKREDDATA_MONGO_DATABASE",
        help="Mongo database name.",
    ),
    collection: str | None = typer.Option(
        None,
        "--collection",
        envvar="SKREDDATA_MONGO_COLLECTION",
        help="Mongo collection name.",
    ),
) -> None:
    db = _make_database(host, port, database, collection)
    item = db.get_by_uuid(uuid)
    if item is None:
        typer.echo(f"UUID not found: {uuid}", err=True)
        raise typer.Exit(code=1)

    _echo_json(_item_payload(item))


@app.command(name="list")
def list_items(
    limit: int = typer.Option(20, "--limit", min=1, help="Maximum number of items to print."),
    label: int | None = typer.Option(None, "--label", help="Filter to a specific label."),
    unlabeled: bool = typer.Option(False, "--unlabeled", help="Show unlabeled items only."),
    host: str | None = typer.Option(None, "--host", envvar="SKREDDATA_MONGO_HOST", help="Mongo host."),
    port: int | None = typer.Option(None, "--port", envvar="SKREDDATA_MONGO_PORT", help="Mongo port."),
    database: str | None = typer.Option(
        None,
        "--database",
        envvar="SKREDDATA_MONGO_DATABASE",
        help="Mongo database name.",
    ),
    collection: str | None = typer.Option(
        None,
        "--collection",
        envvar="SKREDDATA_MONGO_COLLECTION",
        help="Mongo collection name.",
    ),
) -> None:
    _validate_list_filters(label, unlabeled)
    db = _make_database(host, port, database, collection)

    if unlabeled:
        items = db.get_all_unlabeled()
    elif label is not None:
        items = db.get_by_label(label)
    else:
        items = db.get_all()

    payload = [_item_payload(item) for item in items[:limit]]
    _echo_json(payload)


@app.command()
def backup(
    archive_file: Path = typer.Argument(..., exists=False, file_okay=True, dir_okay=False, help="Archive file to create."),
    container: str | None = typer.Option(
        None,
        "--container",
        envvar="SKREDDATA_MONGO_CONTAINER",
        help="Container running MongoDB tools, used when mongodump is not installed on the host.",
    ),
    host: str | None = typer.Option(None, "--host", envvar="SKREDDATA_MONGO_HOST", help="Mongo host."),
    port: int | None = typer.Option(None, "--port", envvar="SKREDDATA_MONGO_PORT", help="Mongo port."),
    database: str | None = typer.Option(
        None,
        "--database",
        envvar="SKREDDATA_MONGO_DATABASE",
        help="Mongo database name.",
    ),
    collection: str | None = typer.Option(
        None,
        "--collection",
        envvar="SKREDDATA_MONGO_COLLECTION",
        help="Mongo collection name.",
    ),
) -> None:
    db = _make_database(host, port, database, collection)
    created_file = backup_database(archive_file, db, container=container)
    _echo_json({"archive_file": created_file})


@app.command()
def restore(
    backup_file: Path = typer.Argument(..., exists=True, dir_okay=False, file_okay=True, help="Backup archive to restore."),
    drop: bool = typer.Option(False, "--drop", help="Drop the target collection before restoring."),
    container: str | None = typer.Option(
        None,
        "--container",
        envvar="SKREDDATA_MONGO_CONTAINER",
        help="Container running MongoDB tools, used when mongorestore is not installed on the host.",
    ),
    host: str | None = typer.Option(None, "--host", envvar="SKREDDATA_MONGO_HOST", help="Mongo host."),
    port: int | None = typer.Option(None, "--port", envvar="SKREDDATA_MONGO_PORT", help="Mongo port."),
    database: str | None = typer.Option(
        None,
        "--database",
        envvar="SKREDDATA_MONGO_DATABASE",
        help="Mongo database name.",
    ),
    collection: str | None = typer.Option(
        None,
        "--collection",
        envvar="SKREDDATA_MONGO_COLLECTION",
        help="Mongo collection name.",
    ),
) -> None:
    db = _make_database(host, port, database, collection)
    restore_backup(backup_file, db, container=container, drop=drop)
    _echo_json({"archive_file": backup_file, "restored": True})


def main() -> None:
    app()
