import datetime as dt
import os
import unittest
from unittest import mock

from shapely.geometry import Point

from skreddata import Database, DummyDatabase, Item


class FakeMongoCollection:
    def find(self, *args, **kwargs):
        return []

    def find_one(self, *args, **kwargs):
        return None

    def count_documents(self, query):
        return 0


class FakeMongoDatabase:
    def __getitem__(self, name):
        return FakeMongoCollection()


class FakeMongoAdmin:
    def command(self, name):
        return {"ok": 1, "command": name}


class FakeMongoClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.admin = FakeMongoAdmin()

    def __getitem__(self, name):
        return FakeMongoDatabase()


class TestItem(unittest.TestCase):
    def test_normalizes_geometry_timestamps_and_json(self):
        item = Item(
            uuid="abc",
            geometry=Point(10, 60),
            t_0="2024-01-01T00:00:00",
            t_1="2024-01-02T12:00:00",
            json={"source": "manual"},
        )

        self.assertEqual(item.geometry, "POINT (10.0000000000000000 60.0000000000000000)")
        self.assertEqual(item.t_0, dt.datetime(2024, 1, 1, 0, 0))
        self.assertEqual(item.t_1, dt.datetime(2024, 1, 2, 12, 0))
        self.assertEqual(item.json, '{"source": "manual"}')

    def test_preserves_missing_object_id(self):
        item = Item(
            uuid="abc",
            geometry="POINT (10 60)",
            t_0="2024-01-01T00:00:00",
            t_1="2024-01-02T00:00:00",
        )

        self.assertIsNone(item._id)


class TestDummyDatabase(unittest.TestCase):
    def setUp(self):
        self.db = DummyDatabase()
        self.first = Item(
            uuid="first",
            geometry="POINT (10 60)",
            t_0="2024-01-01T00:00:00",
            t_1="2024-01-02T00:00:00",
            label=1,
            comment="first sample",
        )
        self.second = Item(
            uuid="second",
            geometry="POINT (11 61)",
            t_0="2024-01-03T00:00:00",
            t_1="2024-01-04T00:00:00",
            comment="second sample",
        )
        self.db.insert(self.first)
        self.db.insert(self.second)

    def test_returns_items_from_queries(self):
        item = self.db.get_by_uuid("first")

        self.assertIsInstance(item, Item)
        self.assertEqual(item.uuid, "first")

    def test_length_and_label_queries_match_database_contract(self):
        self.assertEqual(self.db.get_length(), 2)
        self.assertEqual(self.db.get_length_labeled(), 1)
        self.assertEqual(self.db.get_length_unlabeled(), 1)
        self.assertEqual(self.db.get_length_with_label(1), 1)

    def test_search_helpers_work(self):
        self.assertEqual([item.uuid for item in self.db.get_by_uuid_contains("ir")], ["first"])
        self.assertEqual([item.uuid for item in self.db.get_by_comment_contains("second")], ["second"])
        self.assertEqual([item.uuid for item in self.db.get_by_time("2024-01-01T12:00:00")], ["first"])


class TestDatabaseConfig(unittest.TestCase):
    def setUp(self):
        self.env = {
            "SKREDDATA_MONGO_HOST": "env-host",
            "SKREDDATA_MONGO_PORT": "28017",
            "SKREDDATA_MONGO_DATABASE": "env-db",
            "SKREDDATA_MONGO_COLLECTION": "env-collection",
        }

    def test_constructor_args_override_environment(self):
        with mock.patch("skreddata.database.MongoClient", FakeMongoClient):
            with mock.patch.dict(os.environ, self.env, clear=False):
                db = Database(
                    host="arg-host",
                    port=29017,
                    database="arg-db",
                    collection="arg-collection",
                )

        self.assertEqual(db.host, "arg-host")
        self.assertEqual(db.port, 29017)
        self.assertEqual(db.database, "arg-db")
        self.assertEqual(db.collection, "arg-collection")

    def test_environment_overrides_defaults(self):
        with mock.patch("skreddata.database.MongoClient", FakeMongoClient):
            with mock.patch.dict(os.environ, self.env, clear=False):
                db = Database()

        self.assertEqual(db.host, "env-host")
        self.assertEqual(db.port, 28017)
        self.assertEqual(db.database, "env-db")
        self.assertEqual(db.collection, "env-collection")

    def test_defaults_apply_when_environment_is_missing(self):
        with mock.patch("skreddata.database.MongoClient", FakeMongoClient):
            with mock.patch.dict(os.environ, {}, clear=True):
                db = Database()

        self.assertEqual(db.host, "localhost")
        self.assertEqual(db.port, 27017)
        self.assertEqual(db.database, "skreddata")
        self.assertEqual(db.collection, "avl-v20230607")
