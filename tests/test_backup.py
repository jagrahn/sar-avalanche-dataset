import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from skreddata import backup_database, restore_backup


class TestMongoToolBackups(unittest.TestCase):
    def setUp(self):
        self.db = SimpleNamespace(
            host="localhost",
            port=27017,
            database="skreddata",
            collection="avl-v20230607",
        )

    def test_backup_database_runs_mongodump(self):
        commands = []

        def fake_run(command, **kwargs):
            commands.append(command)
            kwargs["stdout"].write(b"archive")
            return mock.Mock()

        with tempfile.TemporaryDirectory() as tmp_dir:
            archive_file = Path(tmp_dir) / "backup.archive.gz"
            with mock.patch("skreddata.backup.shutil.which", return_value="/usr/bin/mongodump"):
                with mock.patch("skreddata.backup.subprocess.run", side_effect=fake_run):
                    result = backup_database(archive_file, self.db)

            self.assertTrue(archive_file.exists())

        self.assertEqual(result, archive_file)
        self.assertEqual(
            commands[0],
            [
                "/usr/bin/mongodump",
                "--host",
                "localhost",
                "--port",
                "27017",
                "--db",
                "skreddata",
                "--collection",
                "avl-v20230607",
                "--archive",
                "--gzip",
            ],
        )

    def test_backup_database_uses_container_when_requested(self):
        commands = []

        def fake_run(command, **kwargs):
            commands.append(command)
            kwargs["stdout"].write(b"archive")
            return mock.Mock()

        with tempfile.TemporaryDirectory() as tmp_dir:
            archive_file = Path(tmp_dir) / "backup.archive.gz"
            with mock.patch("skreddata.backup.subprocess.run", side_effect=fake_run):
                backup_database(archive_file, self.db, container="skreddata-mongo")

        self.assertEqual(
            commands[0],
            [
                "podman",
                "exec",
                "-i",
                "skreddata-mongo",
                "mongodump",
                "--db",
                "skreddata",
                "--collection",
                "avl-v20230607",
                "--archive",
                "--gzip",
            ],
        )

    def test_restore_backup_runs_mongorestore(self):
        commands = []

        def fake_run(command, **kwargs):
            commands.append(command)
            self.assertEqual(kwargs["stdin"].read(), b"archive")
            return mock.Mock()

        with tempfile.TemporaryDirectory() as tmp_dir:
            archive_file = Path(tmp_dir) / "backup.archive.gz"
            archive_file.write_bytes(b"archive")

            with mock.patch("skreddata.backup.shutil.which", return_value="/usr/bin/mongorestore"):
                with mock.patch("skreddata.backup.subprocess.run", side_effect=fake_run):
                    restore_backup(archive_file, self.db, drop=True)

        self.assertEqual(
            commands[0],
            [
                "/usr/bin/mongorestore",
                "--host",
                "localhost",
                "--port",
                "27017",
                "--db",
                "skreddata",
                "--collection",
                "avl-v20230607",
                "--archive",
                "--gzip",
                "--drop",
            ],
        )
