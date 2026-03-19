#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from pymongo import ReplaceOne
from skreddata import Database, Item


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import an avalanche SQLite database into a skreddata MongoDB collection."
    )
    parser.add_argument("sqlite_file", type=Path, help="Path to the SQLite file.")
    parser.add_argument("--host", default=None, help="Mongo host.")
    parser.add_argument("--port", type=int, default=None, help="Mongo port.")
    parser.add_argument("--database", default=None, help="Mongo database name.")
    parser.add_argument("--collection", default=None, help="Mongo collection name.")
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop the target collection before importing.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of rows to buffer before writing to Mongo.",
    )
    return parser.parse_args()


def iter_items(sqlite_file: Path):
    connection = sqlite3.connect(sqlite_file)
    connection.row_factory = sqlite3.Row
    try:
        query = """
            SELECT uuid, geometry, t_0, t_1, label, comment, type, certainty, source, json
            FROM avalanches
        """
        for row in connection.execute(query):
            yield Item(**dict(row))
    finally:
        connection.close()


def item_document(item: Item) -> dict:
    document = item.asdict()
    if document.get("_id") is None:
        document.pop("_id", None)
    return document


def main() -> None:
    args = parse_args()
    db = Database(
        host=args.host,
        port=args.port,
        database=args.database,
        collection=args.collection,
    )

    if args.drop:
        db.mongo_collection.drop()

    db.mongo_collection.create_index("uuid", unique=True)

    batch = []
    imported = 0
    for item in iter_items(args.sqlite_file):
        document = item_document(item)
        batch.append(
            ReplaceOne(
                {"uuid": document["uuid"]},
                document,
                upsert=True,
            )
        )
        if len(batch) >= args.batch_size:
            db.mongo_collection.bulk_write(batch, ordered=False)
            imported += len(batch)
            print(f"imported {imported}")
            batch.clear()

    if batch:
        db.mongo_collection.bulk_write(batch, ordered=False)
        imported += len(batch)
        print(f"imported {imported}")

    print(
        f"done: imported {imported} documents into "
        f"{db.database}.{db.collection} on {db.host}:{db.port}"
    )


if __name__ == "__main__":
    main()
