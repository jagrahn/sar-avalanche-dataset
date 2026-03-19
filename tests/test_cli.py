import unittest
from unittest import mock
import tempfile
from pathlib import Path

from typer.testing import CliRunner

from skreddata import DummyDatabase, Item
from skreddata.cli.app import app


class TestCli(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.db = DummyDatabase()
        self.db.insert(
            Item(
                uuid="first",
                geometry="POINT (10 60)",
                t_0="2024-01-01T00:00:00",
                t_1="2024-01-02T00:00:00",
                label=1,
                comment="first sample",
            )
        )
        self.db.insert(
            Item(
                uuid="second",
                geometry="POINT (11 61)",
                t_0="2024-01-03T00:00:00",
                t_1="2024-01-04T00:00:00",
            )
        )

    def _run(self, args):
        with mock.patch("skreddata.cli.app.Database", return_value=self.db):
            return self.runner.invoke(app, args)

    def test_ping(self):
        result = self._run(["ping"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn('"ok": 1', result.stdout)

    def test_count_filters(self):
        self.assertEqual(self._run(["count"]).stdout.strip(), "2")
        self.assertEqual(self._run(["count", "--labeled"]).stdout.strip(), "1")
        self.assertEqual(self._run(["count", "--unlabeled"]).stdout.strip(), "1")
        self.assertEqual(self._run(["count", "--label", "1"]).stdout.strip(), "1")

    def test_get_found_and_missing(self):
        found = self._run(["get", "first"])
        missing = self._run(["get", "missing"])

        self.assertEqual(found.exit_code, 0)
        self.assertIn('"uuid": "first"', found.stdout)
        self.assertEqual(missing.exit_code, 1)
        self.assertIn("UUID not found: missing", missing.stderr)

    def test_list_filters(self):
        listed = self._run(["list", "--limit", "1"])
        unlabeled = self._run(["list", "--unlabeled"])
        labeled = self._run(["list", "--label", "1"])

        self.assertEqual(listed.exit_code, 0)
        self.assertEqual(listed.stdout.count('"uuid"'), 1)
        self.assertIn('"uuid": "second"', unlabeled.stdout)
        self.assertIn('"uuid": "first"', labeled.stdout)

    def test_backup_command(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            archive_file = Path(tmp_dir) / "sample.archive.gz"
            with mock.patch("skreddata.cli.app.backup_database", return_value=archive_file):
                result = self._run(["backup", str(archive_file)])

        self.assertEqual(result.exit_code, 0)
        self.assertIn(str(archive_file), result.stdout)

    def test_restore_command(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            backup_file = Path(tmp_dir) / "sample.archive.gz"
            backup_file.write_bytes(b"archive")

            with mock.patch("skreddata.cli.app.Database", return_value=self.db):
                with mock.patch("skreddata.cli.app.restore_backup", return_value=None):
                    result = self.runner.invoke(app, ["restore", str(backup_file), "--drop"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn('"restored": true', result.stdout)
