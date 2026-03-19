from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import uuid as uuidlib
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from functools import partial
from pathlib import Path
from typing import Any, Iterable

from skreddata import Database, Item


DEFAULT_BACKUP_ROOT = Path("/NORCE/Data/600/60090/long_lived_JGRA/from_lysorgel/skreddata")
DEFAULT_BUILD_ROOT = Path("/localscratch/work/jgra/skreddata-build")
DEFAULT_NFS_TARGET = Path("/NORCE/Data/600/60090/long_lived_JGRA/skreddata")
UUID_PATTERN = re.compile(r"^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$")

INVENTORY_FILENAME = "inventory.jsonl"
MISSING_FILENAME = "missing.jsonl"
SUMMARY_FILENAME = "build-summary.json"
PATCHES_FILENAME = "patches.jsonl"


@dataclass(frozen=True)
class SourceEntry:
    original_id: str
    source_item_dir: Path
    rcs_path: Path | None
    dem_path: Path | None
    input_geojson_path: Path | None
    overlay_bounds_path: Path | None
    regobs_path: Path | None
    products_path: Path | None
    meteorology_path: Path | None

    @property
    def source_item_dir_relative(self) -> str:
        return f"avalanche_input/{self.source_item_dir.name}"


@dataclass(frozen=True)
class InventoryEntry:
    uuid: str
    original_id: str
    source_item_dir: str
    rcs_path: str
    dem_path: str | None
    input_geojson_path: str | None
    overlay_bounds_path: str | None
    regobs_path: str | None
    products_path: str | None
    meteorology_path: str | None
    mask_source_path: str | None


@dataclass(frozen=True)
class MissingEntry:
    uuid: str
    original_id: str
    reason: str
    source_item_dir: str | None


def _fanout(uuid: str) -> str:
    normalized = uuid.replace("-", "").upper()
    return normalized[:2]


def _as_path(value: str | None) -> Path | None:
    return None if value is None else Path(value)


def canonical_uuid_for_source_id(source_id: str) -> str:
    if UUID_PATTERN.match(source_id):
        return source_id.upper()
    return str(uuidlib.uuid5(uuidlib.NAMESPACE_URL, f"skreddata:source-id:{source_id}")).upper()


def _dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _load_json(path: Path | None) -> Any | None:
    if path is None or not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _pick_file(files: list[Path], *suffixes: str) -> Path | None:
    for suffix in suffixes:
        for file_path in files:
            if file_path.name.endswith(suffix):
                return file_path
    return None


def _source_input_root(backup_root: Path) -> Path:
    return backup_root / "data" / "avalanche_input"


def _source_mask_root(backup_root: Path) -> Path:
    return backup_root / "data" / "avalanche_masks"


def _manifest_root(build_root: Path) -> Path:
    return build_root / "dataset" / "manifests"


def _inventory_path(build_root: Path) -> Path:
    return build_root / INVENTORY_FILENAME


def _missing_path(build_root: Path) -> Path:
    return _manifest_root(build_root) / MISSING_FILENAME


def _summary_path(build_root: Path) -> Path:
    return _manifest_root(build_root) / SUMMARY_FILENAME


def _patches_path(build_root: Path) -> Path:
    return _manifest_root(build_root) / PATCHES_FILENAME


def _patch_root(build_root: Path, uuid: str) -> Path:
    return build_root / "dataset" / "patches" / _fanout(uuid) / uuid


def _mask_path(build_root: Path, uuid: str) -> Path:
    return build_root / "dataset" / "masks" / _fanout(uuid) / f"{uuid}.png"


def _mongo_uuids(db: Database) -> list[str]:
    return sorted(item.uuid for item in db.get_all())


def _scan_source_entries(backup_root: Path) -> tuple[dict[str, SourceEntry], Counter[str]]:
    input_root = _source_input_root(backup_root)
    summary: Counter[str] = Counter()
    entries: dict[str, SourceEntry] = {}

    if not input_root.exists():
        raise FileNotFoundError(f"Backup input root does not exist: {input_root}")

    with os.scandir(input_root) as root_entries:
        for root_entry in root_entries:
            if not root_entry.is_dir():
                continue

            summary["source_dir_count"] += 1
            if UUID_PATTERN.match(root_entry.name):
                summary["uuid_named_source_dir_count"] += 1
            else:
                summary["non_uuid_source_dir_count"] += 1
            files: list[Path] = []
            with os.scandir(root_entry.path) as item_entries:
                for item_entry in item_entries:
                    if item_entry.is_file():
                        files.append(Path(item_entry.path))

            entries[root_entry.name] = SourceEntry(
                original_id=root_entry.name,
                source_item_dir=Path(root_entry.path),
                rcs_path=_pick_file(files, "_epsg32633_rcs.tif", "_rcs.tif"),
                dem_path=_pick_file(files, "_epsg32633_dem.tif", "_dem.tif"),
                input_geojson_path=_pick_file(files, "_input.geojson"),
                overlay_bounds_path=_pick_file(files, "_epsg3857_rgb_bpol.geojson", "_rgb_bpol.geojson", "_bpol.geojson"),
                regobs_path=_pick_file(files, "_regobs.geojson"),
                products_path=_pick_file(files, "_products.json"),
                meteorology_path=_pick_file(files, "_met.json"),
            )

    return entries, summary


def inventory_backup(
    backup_root: Path = DEFAULT_BACKUP_ROOT,
    build_root: Path = DEFAULT_BUILD_ROOT,
    db: Database | None = None,
) -> tuple[list[InventoryEntry], list[MissingEntry], dict[str, Any]]:
    db = db or Database()
    source_entries, summary_counter = _scan_source_entries(backup_root)
    mongo_uuids = _mongo_uuids(db)
    mask_root = _source_mask_root(backup_root)
    missing_reason_counts: Counter[str] = Counter()
    inventory_rows: list[InventoryEntry] = []
    missing_rows: list[MissingEntry] = []

    summary_counter["mongo_uuid_count"] = len(mongo_uuids)

    for uuid in mongo_uuids:
        canonical_uuid = canonical_uuid_for_source_id(uuid)
        source = source_entries.get(uuid)
        if source is None:
            reason = "source_dir_not_found"
            missing_rows.append(MissingEntry(uuid=canonical_uuid, original_id=uuid, reason=reason, source_item_dir=None))
            missing_reason_counts[reason] += 1
            continue

        if source.rcs_path is None:
            reason = "epsg32633_rcs_missing"
            missing_rows.append(
                MissingEntry(
                    uuid=canonical_uuid,
                    original_id=uuid,
                    reason=reason,
                    source_item_dir=source.source_item_dir_relative,
                )
            )
            missing_reason_counts[reason] += 1
            continue

        mask_source_path = mask_root / f"{uuid}_mask.png"
        inventory_rows.append(
            InventoryEntry(
                uuid=canonical_uuid,
                original_id=uuid,
                source_item_dir=source.source_item_dir_relative,
                rcs_path=str(source.rcs_path),
                dem_path=None if source.dem_path is None else str(source.dem_path),
                input_geojson_path=None if source.input_geojson_path is None else str(source.input_geojson_path),
                overlay_bounds_path=None if source.overlay_bounds_path is None else str(source.overlay_bounds_path),
                regobs_path=None if source.regobs_path is None else str(source.regobs_path),
                products_path=None if source.products_path is None else str(source.products_path),
                meteorology_path=None if source.meteorology_path is None else str(source.meteorology_path),
                mask_source_path=str(mask_source_path) if mask_source_path.exists() else None,
            )
        )

    summary = {
        "backup_root": str(backup_root),
        "build_root": str(build_root),
        "mongo_uuid_count": len(mongo_uuids),
        "source_dir_count": summary_counter["source_dir_count"],
        "uuid_named_source_dir_count": summary_counter["uuid_named_source_dir_count"],
        "non_uuid_source_dir_count": summary_counter["non_uuid_source_dir_count"],
        "migratable_count": len(inventory_rows),
        "missing_count": len(missing_rows),
        "mask_count": sum(1 for row in inventory_rows if row.mask_source_path is not None),
        "generated_uuid_count": sum(1 for row in inventory_rows if row.original_id != row.uuid),
        "missing_by_reason": dict(sorted(missing_reason_counts.items())),
    }

    _write_jsonl(_inventory_path(build_root), (asdict(row) for row in inventory_rows))
    _write_jsonl(_missing_path(build_root), (asdict(row) for row in missing_rows))
    _dump_json(_summary_path(build_root), summary)
    return inventory_rows, missing_rows, summary


def _load_inventory_rows(build_root: Path) -> list[InventoryEntry]:
    rows = []
    for row in _load_jsonl(_inventory_path(build_root)):
        rows.append(InventoryEntry(**row))
    return rows


def _meta_payload(entry: InventoryEntry, backup_root: Path) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "uuid": entry.uuid,
        "projection": "EPSG:32633",
        "source": {
            "backup_root": str(backup_root / "data"),
            "item_dir": entry.source_item_dir,
            "original_id": entry.original_id,
            "canonical_uuid_generated": entry.original_id != entry.uuid,
        },
        "assets": {
            "rcs": "rcs.tif",
            "dem": "dem.tif" if entry.dem_path is not None else None,
        },
        "input_geojson": _load_json(_as_path(entry.input_geojson_path)),
        "overlay_bounds_geojson": _load_json(_as_path(entry.overlay_bounds_path)),
        "regobs_geojson": _load_json(_as_path(entry.regobs_path)),
        "products": _load_json(_as_path(entry.products_path)),
        "meteorology": _load_json(_as_path(entry.meteorology_path)),
    }


def _materialize_entry(entry: InventoryEntry, backup_root: Path, build_root: Path) -> dict[str, Any]:
    patch_root = _patch_root(build_root, entry.uuid)
    patch_root.mkdir(parents=True, exist_ok=True)

    rcs_target = patch_root / "rcs.tif"
    if not rcs_target.exists():
        shutil.copy2(entry.rcs_path, rcs_target)

    dem_target = patch_root / "dem.tif"
    if entry.dem_path is not None:
        if not dem_target.exists():
            shutil.copy2(entry.dem_path, dem_target)
    else:
        dem_target.unlink(missing_ok=True)

    _dump_json(patch_root / "meta.json", _meta_payload(entry, backup_root))

    mask_path: str | None = None
    target_mask = _mask_path(build_root, entry.uuid)
    if entry.mask_source_path is not None:
        target_mask.parent.mkdir(parents=True, exist_ok=True)
        if not target_mask.exists():
            shutil.copy2(entry.mask_source_path, target_mask)
        mask_path = str(target_mask.relative_to(build_root / "dataset"))
    else:
        target_mask.unlink(missing_ok=True)

    return {
        "uuid": entry.uuid,
        "original_id": entry.original_id,
        "patch_dir": str(patch_root.relative_to(build_root / "dataset")),
        "mask_path": mask_path,
        "has_dem": entry.dem_path is not None,
        "has_overlay_bounds": entry.overlay_bounds_path is not None,
        "has_regobs": entry.regobs_path is not None,
        "has_products": entry.products_path is not None,
        "has_meteorology": entry.meteorology_path is not None,
        "source_item_dir": entry.source_item_dir,
        "projection": "EPSG:32633",
        "status": "migrated",
    }


def build_curated_dataset(
    backup_root: Path = DEFAULT_BACKUP_ROOT,
    build_root: Path = DEFAULT_BUILD_ROOT,
    db: Database | None = None,
    clean: bool = False,
    workers: int = 8,
) -> dict[str, Any]:
    db = db or Database()
    if clean and build_root.exists():
        shutil.rmtree(build_root)

    if build_root.exists() and any(build_root.iterdir()):
        inventory_rows = _load_inventory_rows(build_root)
        if not inventory_rows:
            raise RuntimeError(f"Build root is not empty and has no inventory file: {build_root}")
        with _summary_path(build_root).open("r", encoding="utf-8") as handle:
            summary = json.load(handle)
    else:
        inventory_rows, _, summary = inventory_backup(backup_root=backup_root, build_root=build_root, db=db)

    patches_root = build_root / "dataset" / "patches"
    masks_root = build_root / "dataset" / "masks"
    manifests_root = _manifest_root(build_root)
    patches_root.mkdir(parents=True, exist_ok=True)
    masks_root.mkdir(parents=True, exist_ok=True)
    manifests_root.mkdir(parents=True, exist_ok=True)

    if workers <= 1:
        migrated_rows = [_materialize_entry(entry, backup_root, build_root) for entry in inventory_rows]
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            migrated_rows = list(executor.map(partial(_materialize_entry, backup_root=backup_root, build_root=build_root), inventory_rows))

    _write_jsonl(_patches_path(build_root), migrated_rows)
    summary = dict(summary)
    summary["built_patch_count"] = len(migrated_rows)
    summary["built_mask_count"] = sum(1 for row in migrated_rows if row["mask_path"] is not None)
    _dump_json(_summary_path(build_root), summary)
    return summary


def verify_curated_dataset(build_root: Path = DEFAULT_BUILD_ROOT) -> dict[str, Any]:
    dataset_root = build_root / "dataset"
    manifests_root = _manifest_root(build_root)
    patches_manifest = _load_jsonl(_patches_path(build_root))
    missing_manifest = _load_jsonl(_missing_path(build_root))

    errors: list[str] = []
    patch_count = 0
    mask_count = 0
    png_signature = b"\x89PNG\r\n\x1a\n"

    for row in patches_manifest:
        patch_dir = dataset_root / row["patch_dir"]
        patch_count += 1
        if not patch_dir.exists():
            errors.append(f"Missing patch directory: {patch_dir}")
            continue

        rcs_path = patch_dir / "rcs.tif"
        meta_path = patch_dir / "meta.json"
        if not rcs_path.exists():
            errors.append(f"Missing rcs.tif: {rcs_path}")
        if not meta_path.exists():
            errors.append(f"Missing meta.json: {meta_path}")
        else:
            try:
                with meta_path.open("r", encoding="utf-8") as handle:
                    json.load(handle)
            except json.JSONDecodeError as exc:
                errors.append(f"Invalid meta.json {meta_path}: {exc}")

        if row["mask_path"] is not None:
            mask_path = dataset_root / row["mask_path"]
            if not mask_path.exists():
                errors.append(f"Missing mask: {mask_path}")
            else:
                with mask_path.open("rb") as handle:
                    if handle.read(8) != png_signature:
                        errors.append(f"Mask is not a PNG: {mask_path}")
                mask_count += 1

    for path in dataset_root.rglob("*"):
        if not path.is_file():
            continue
        if path.name.endswith(".complete"):
            errors.append(f"Unexpected .complete file: {path}")
        if path.name.endswith("rgb.tif"):
            errors.append(f"Unexpected rgb.tif file: {path}")

    with _summary_path(build_root).open("r", encoding="utf-8") as handle:
        summary = json.load(handle)

    if patch_count != summary.get("built_patch_count"):
        errors.append(f"Patch manifest count {patch_count} != build summary {summary.get('built_patch_count')}")
    if mask_count != summary.get("built_mask_count"):
        errors.append(f"Mask manifest count {mask_count} != build summary {summary.get('built_mask_count')}")
    if len(missing_manifest) != summary.get("missing_count"):
        errors.append(f"Missing manifest count {len(missing_manifest)} != build summary {summary.get('missing_count')}")

    result = {
        "dataset_root": str(dataset_root),
        "patch_count": patch_count,
        "mask_count": mask_count,
        "missing_count": len(missing_manifest),
        "errors": errors,
        "valid": not errors,
    }
    return result


def _common_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--build-root", type=Path, default=DEFAULT_BUILD_ROOT)
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument("--collection", default=None)
    return parser


def _make_database(args: argparse.Namespace) -> Database:
    return Database(host=args.host, port=args.port, database=args.database, collection=args.collection)


def inventory_main() -> int:
    parser = _common_parser("Inventory Mongo-backed UUIDs against the raw backup tree.")
    args = parser.parse_args()
    _, _, summary = inventory_backup(backup_root=args.backup_root, build_root=args.build_root, db=_make_database(args))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def build_main() -> int:
    parser = _common_parser("Build the curated skreddata dataset locally.")
    parser.add_argument("--clean", action="store_true", help="Remove the build root before rebuilding.")
    parser.add_argument("--workers", type=int, default=8, help="Number of worker threads for local materialization.")
    args = parser.parse_args()
    summary = build_curated_dataset(
        backup_root=args.backup_root,
        build_root=args.build_root,
        db=_make_database(args),
        clean=args.clean,
        workers=args.workers,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def verify_main() -> int:
    parser = argparse.ArgumentParser(description="Verify the curated skreddata dataset.")
    parser.add_argument("--build-root", type=Path, default=DEFAULT_BUILD_ROOT)
    args = parser.parse_args()
    result = verify_curated_dataset(build_root=args.build_root)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["valid"] else 1
