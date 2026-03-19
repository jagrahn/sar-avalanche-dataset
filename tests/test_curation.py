import json
import tempfile
import unittest
from pathlib import Path

from skreddata import DummyDatabase, Item
from skreddata.curation import (
    build_curated_dataset,
    canonical_uuid_for_source_id,
    inventory_backup,
    verify_curated_dataset,
)


class TestDatasetCuration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.backup_root = self.root / "backup"
        self.build_root = self.root / "build"

        input_root = self.backup_root / "data" / "avalanche_input"
        mask_root = self.backup_root / "data" / "avalanche_masks"
        uuid_dir = input_root / "00081C51-D7A1-47A4-AE52-6DB2C9919F6A"
        uuid_dir.mkdir(parents=True)
        (uuid_dir / "sample_rcs.tif").write_bytes(b"rcs")
        (uuid_dir / "sample_dem.tif").write_bytes(b"dem")
        (uuid_dir / "sample_input.geojson").write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
        (uuid_dir / "sample_epsg3857_rgb_bpol.geojson").write_text(
            '{"type":"FeatureCollection","features":[]}',
            encoding="utf-8",
        )
        (uuid_dir / "sample_regobs.geojson").write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
        (uuid_dir / "sample_products.json").write_text("[]", encoding="utf-8")
        (uuid_dir / "sample_met.json").write_text('{"time":[]}', encoding="utf-8")

        ignored_dir = input_root / "not-a-uuid"
        ignored_dir.mkdir(parents=True)
        (ignored_dir / "ignored_rcs.tif").write_bytes(b"ignored")

        non_uuid_id = "Ch_Tamok_20141017_168_00000_00512"
        non_uuid_dir = input_root / non_uuid_id
        non_uuid_dir.mkdir(parents=True)
        (non_uuid_dir / f"{non_uuid_id}_rcs.tif").write_bytes(b"rcs")
        (non_uuid_dir / f"{non_uuid_id}_bpol.geojson").write_text(
            '{"type":"FeatureCollection","features":[]}',
            encoding="utf-8",
        )

        mask_root.mkdir(parents=True)
        (mask_root / "00081C51-D7A1-47A4-AE52-6DB2C9919F6A_mask.png").write_bytes(
            b"\x89PNG\r\n\x1a\npayload"
        )
        (mask_root / f"{non_uuid_id}_mask.png").write_bytes(b"\x89PNG\r\n\x1a\npayload")

        self.db = DummyDatabase()
        self.db.insert(
            Item(
                uuid="00081C51-D7A1-47A4-AE52-6DB2C9919F6A",
                geometry="POINT (10 60)",
                t_0="2024-01-01T00:00:00",
                t_1="2024-01-02T00:00:00",
            )
        )
        self.db.insert(
            Item(
                uuid="000A7F06-4753-47CD-8C94-0D772F05623C",
                geometry="POINT (11 61)",
                t_0="2024-01-03T00:00:00",
                t_1="2024-01-04T00:00:00",
            )
        )
        self.db.insert(
            Item(
                uuid=non_uuid_id,
                geometry="POINT (12 62)",
                t_0="2024-01-05T00:00:00",
                t_1="2024-01-06T00:00:00",
            )
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_inventory_build_and_verify_dataset(self):
        inventory_rows, missing_rows, summary = inventory_backup(
            backup_root=self.backup_root,
            build_root=self.build_root,
            db=self.db,
        )

        self.assertEqual(len(inventory_rows), 2)
        self.assertEqual(len(missing_rows), 1)
        self.assertEqual(summary["migratable_count"], 2)
        self.assertEqual(summary["missing_count"], 1)
        self.assertEqual(summary["generated_uuid_count"], 1)

        build_summary = build_curated_dataset(
            backup_root=self.backup_root,
            build_root=self.build_root,
            db=self.db,
        )
        self.assertEqual(build_summary["built_patch_count"], 2)
        self.assertEqual(build_summary["built_mask_count"], 2)

        patch_root = (
            self.build_root
            / "dataset"
            / "patches"
            / "00"
            / "00081C51-D7A1-47A4-AE52-6DB2C9919F6A"
        )
        self.assertTrue((patch_root / "rcs.tif").exists())
        self.assertTrue((patch_root / "dem.tif").exists())
        self.assertTrue((patch_root / "meta.json").exists())
        self.assertFalse(any(path.name.endswith("rgb.tif") for path in patch_root.iterdir()))

        with (patch_root / "meta.json").open("r", encoding="utf-8") as handle:
            meta = json.load(handle)
        self.assertEqual(meta["uuid"], "00081C51-D7A1-47A4-AE52-6DB2C9919F6A")
        self.assertEqual(meta["assets"]["rcs"], "rcs.tif")
        self.assertEqual(meta["assets"]["dem"], "dem.tif")
        self.assertEqual(meta["source"]["original_id"], "00081C51-D7A1-47A4-AE52-6DB2C9919F6A")

        non_uuid_canonical = canonical_uuid_for_source_id("Ch_Tamok_20141017_168_00000_00512")
        non_uuid_patch_root = (
            self.build_root
            / "dataset"
            / "patches"
            / non_uuid_canonical.replace("-", "")[:2]
            / non_uuid_canonical
        )
        self.assertTrue((non_uuid_patch_root / "rcs.tif").exists())
        with (non_uuid_patch_root / "meta.json").open("r", encoding="utf-8") as handle:
            non_uuid_meta = json.load(handle)
        self.assertEqual(non_uuid_meta["uuid"], non_uuid_canonical)
        self.assertEqual(non_uuid_meta["source"]["original_id"], "Ch_Tamok_20141017_168_00000_00512")
        self.assertTrue(non_uuid_meta["source"]["canonical_uuid_generated"])

        verify_result = verify_curated_dataset(self.build_root)
        self.assertTrue(verify_result["valid"], verify_result["errors"])
