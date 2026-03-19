from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
import re
from typing import Any

from dateutil import parser
from pymongo import MongoClient
from shapely.geometry.base import BaseGeometry
import shapely.wkt


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 27017
DEFAULT_DATABASE = "skreddata"
DEFAULT_COLLECTION = "avl-v20230607"


LABEL_DESCRIPTION = {
    0: "Avalanche(s) absent",
    1: "Avalanche(s) present",
    2: "Unsure",
    3: "Defected",
}


def _as_datetime(value: dt.datetime | str) -> dt.datetime:
    return value if isinstance(value, dt.datetime) else parser.parse(value)


def _resolve_setting(name: str, explicit: Any, default: Any, cast: type | None = None) -> Any:
    if explicit not in (None, ""):
        return cast(explicit) if cast is not None else explicit

    raw = os.environ.get(name)
    if raw in (None, ""):
        return default

    return cast(raw) if cast is not None else raw


def _query_unlabeled() -> dict[str, Any]:
    return {"$or": [{"label": {"$exists": False}}, {"label": None}]}


def _query_labeled() -> dict[str, Any]:
    return {"label": {"$exists": True, "$ne": None}}


def _match_query(document: dict[str, Any], query: dict[str, Any] | None) -> bool:
    if not query:
        return True

    if "$and" in query:
        return all(_match_query(document, subquery) for subquery in query["$and"])

    if "$or" in query:
        return any(_match_query(document, subquery) for subquery in query["$or"])

    for key, expected in query.items():
        if key in {"$and", "$or"}:
            continue

        value = document.get(key)
        if isinstance(expected, dict):
            for operator, operand in expected.items():
                if operator == "$exists":
                    if (key in document) != operand:
                        return False
                elif operator == "$ne":
                    if value == operand:
                        return False
                elif operator == "$lte":
                    if value is None or value > operand:
                        return False
                elif operator == "$gte":
                    if value is None or value < operand:
                        return False
                elif operator == "$regex":
                    if value is None or re.search(str(operand), str(value)) is None:
                        return False
                else:
                    raise NotImplementedError(f"Unsupported DummyDatabase operator: {operator}")
        elif value != expected:
            return False

    return True


@dataclasses.dataclass(frozen=True)
class Item:
    uuid: str
    geometry: str | BaseGeometry
    t_0: dt.datetime | str
    t_1: dt.datetime | str
    label: int | None = None
    comment: str | None = None
    type: str | None = None
    certainty: int | None = None
    source: str | None = None
    json: str | dict[str, Any] | list[Any] | None = None
    _id: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.geometry, BaseGeometry):
            super().__setattr__("geometry", shapely.wkt.dumps(self.geometry))

        super().__setattr__("t_0", _as_datetime(self.t_0))
        super().__setattr__("t_1", _as_datetime(self.t_1))

        if self.json is not None and not isinstance(self.json, str):
            super().__setattr__("json", json.dumps(self.json))

        if self._id is not None:
            super().__setattr__("_id", str(self._id))

    @classmethod
    def from_value(cls, value: Item | dict[str, Any]) -> Item:
        if isinstance(value, cls):
            return value
        if isinstance(value, dict):
            return cls(**value)
        raise TypeError("Expected Item or dict")

    def asdict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    def to_document(self) -> dict[str, Any]:
        document = self.asdict()
        if document["_id"] is None:
            document.pop("_id")
        return document


class _DatabaseQueries:
    def find(self, *args: Any, **kwargs: Any) -> list[Item]:
        raise NotImplementedError

    def find_one(self, *args: Any, **kwargs: Any) -> Item | None:
        raise NotImplementedError

    def _count_documents(self, query: dict[str, Any]) -> int:
        raise NotImplementedError

    def get_by_uuid(self, uuid: str) -> Item | None:
        return self.find_one({"uuid": uuid})

    def get_by_label(self, label: int | None) -> list[Item]:
        return self.find({"label": label})

    def get_by_time(self, time: dt.datetime | str) -> list[Item]:
        instant = _as_datetime(time)
        return self.find({"$and": [{"t_0": {"$lte": instant}}, {"t_1": {"$gte": instant}}]})

    def get_by_uuid_contains(self, part: str) -> list[Item]:
        return self.find({"uuid": {"$regex": part}})

    def get_by_comment_contains(self, part: str) -> list[Item]:
        return self.find({"comment": {"$regex": part}})

    def get_all(self) -> list[Item]:
        return self.find({})

    def get_all_unlabeled(self) -> list[Item]:
        return self.find(_query_unlabeled())

    def get_all_labeled(self) -> list[Item]:
        return self.find(_query_labeled())

    def get_length(self) -> int:
        return self._count_documents({})

    def get_length_unlabeled(self) -> int:
        return self._count_documents(_query_unlabeled())

    def get_length_labeled(self) -> int:
        return self._count_documents(_query_labeled())

    def get_length_with_label(self, label: int | None) -> int:
        return self._count_documents({"label": label})


class Database(_DatabaseQueries):
    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        collection: str | None = None,
    ) -> None:
        self.host = _resolve_setting("SKREDDATA_MONGO_HOST", host, DEFAULT_HOST)
        self.port = _resolve_setting("SKREDDATA_MONGO_PORT", port, DEFAULT_PORT, int)
        self.database = _resolve_setting("SKREDDATA_MONGO_DATABASE", database, DEFAULT_DATABASE)
        self.collection = _resolve_setting("SKREDDATA_MONGO_COLLECTION", collection, DEFAULT_COLLECTION)

        self.client = MongoClient(self.host, self.port)
        self.db = self.client[self.database]
        self.mongo_collection = self.db[self.collection]

    def ping(self) -> dict[str, Any]:
        response = self.client.admin.command("ping")
        response.update(
            {
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "collection": self.collection,
            }
        )
        return response

    def find(self, *args: Any, **kwargs: Any) -> list[Item]:
        return [Item.from_value(item) for item in self.mongo_collection.find(*args, **kwargs)]

    def find_one(self, *args: Any, **kwargs: Any) -> Item | None:
        item = self.mongo_collection.find_one(*args, **kwargs)
        return None if item is None else Item.from_value(item)

    def insert(self, item: Item | dict[str, Any]) -> None:
        self.mongo_collection.insert_one(Item.from_value(item).to_document())

    def replace(self, item: Item | dict[str, Any]) -> None:
        document = Item.from_value(item).to_document()
        self.mongo_collection.replace_one({"uuid": document["uuid"]}, document)

    def remove_by_uuid(self, uuid: str) -> None:
        self.mongo_collection.delete_one({"uuid": uuid})

    def _count_documents(self, query: dict[str, Any]) -> int:
        return self.mongo_collection.count_documents(query)


class DummyDatabase(_DatabaseQueries):
    def __init__(
        self,
        storage: dict[str, Item | dict[str, Any]] | None = None,
        *,
        host: str = "dummy",
        port: int = 0,
        database: str = "dummy",
        collection: str = "dummy",
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.collection = collection
        self.storage = {
            uuid: Item.from_value(item)
            for uuid, item in (storage or {}).items()
        }

    def ping(self) -> dict[str, Any]:
        return {
            "ok": 1,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "collection": self.collection,
        }

    def find(self, query: dict[str, Any] | None = None, *args: Any, **kwargs: Any) -> list[Item]:
        if args or kwargs:
            raise NotImplementedError("DummyDatabase.find only supports a single query argument")
        return [item for item in self.storage.values() if _match_query(item.asdict(), query)]

    def find_one(self, query: dict[str, Any] | None = None, *args: Any, **kwargs: Any) -> Item | None:
        matches = self.find(query, *args, **kwargs)
        return matches[0] if matches else None

    def insert(self, item: Item | dict[str, Any]) -> None:
        normalized = Item.from_value(item)
        self.storage[normalized.uuid] = normalized

    def replace(self, item: Item | dict[str, Any]) -> None:
        self.insert(item)

    def remove_by_uuid(self, uuid: str) -> None:
        self.storage.pop(uuid, None)

    def _count_documents(self, query: dict[str, Any]) -> int:
        return sum(1 for item in self.storage.values() if _match_query(item.asdict(), query))
