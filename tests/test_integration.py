import os
import unittest
import uuid

from skreddata import Database, Item


@unittest.skipUnless(os.environ.get("SKREDDATA_TEST_MONGO") == "1", "requires SKREDDATA_TEST_MONGO=1")
class TestMongoIntegration(unittest.TestCase):
    def setUp(self):
        collection_prefix = os.environ.get("SKREDDATA_MONGO_COLLECTION", "avl-v20230607")
        self.collection = f"{collection_prefix}_test_{uuid.uuid4().hex[:8]}"
        self.db = Database(collection=self.collection)

    def tearDown(self):
        self.db.db.drop_collection(self.collection)

    def test_insert_and_fetch_item(self):
        item = Item(
            uuid="integration-item",
            geometry="POINT (10 60)",
            t_0="2024-01-01T00:00:00",
            t_1="2024-01-02T00:00:00",
            label=1,
        )

        self.db.insert(item)
        fetched = self.db.get_by_uuid("integration-item")

        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.uuid, "integration-item")
        self.assertEqual(self.db.get_length(), 1)
