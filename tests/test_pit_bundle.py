"""Strict integrity contracts for point-in-time universe bundles."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import cast

import pytest

from src.core import pit_bundle
from src.core.pit_bundle import PitBundleValidationError, validate_pit_bundle


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _composite(hashes: dict[str, str]) -> str:
    payload = "".join(f"{path}  {digest}\n" for path, digest in sorted(hashes.items()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _make_bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "pit-bundle"
    sources = bundle / "sources"
    sources.mkdir(parents=True)
    source = sources / "universe-2025-01-02.csv"
    source.write_text("ticker\n000001.SZ\n000002.SZ\n", encoding="utf-8")
    schedule = bundle / "universe_schedule.csv"
    schedule.write_text(
        "as_of_date,ticker,listed_on_or_before,delisted_after,source_file,source_sha256\n"
        f"2025-01-02,000002.SZ,2020-01-01,,sources/{source.name},{_sha256(source)}\n"
        f"2025-01-02,000001.SZ,2020-01-01,,sources/{source.name},{_sha256(source)}\n",
        encoding="utf-8",
    )
    hashes = {
        "sources/universe-2025-01-02.csv": _sha256(source),
        "universe_schedule.csv": _sha256(schedule),
    }
    manifest = {
        "bundle_id": "pit-test-bundle",
        "created_at": "2026-08-09T00:00:00Z",
        "pit_grade": True,
        "as_of_dates": ["2025-01-02"],
        "hashes": hashes,
        "composite_sha256": _composite(hashes),
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return bundle


def _rewrite_manifest(bundle: Path, mutate) -> None:
    path = bundle / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    mutate(manifest)
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _replace_schedule(bundle: Path, rows: list[str]) -> None:
    schedule = bundle / "universe_schedule.csv"
    schedule.write_text(
        "as_of_date,ticker,listed_on_or_before,delisted_after,source_file,source_sha256\n"
        + "\n".join(rows)
        + "\n",
        encoding="utf-8",
    )
    _rewrite_manifest(
        bundle,
        lambda manifest: manifest["hashes"].update({"universe_schedule.csv": _sha256(schedule)}),
    )
    _rewrite_manifest(bundle, lambda manifest: manifest.update(composite_sha256=_composite(manifest["hashes"])))


def test_validate_pit_bundle_loads_hash_valid_schedule_sorted_by_date_and_ticker(tmp_path):
    bundle = _make_bundle(tmp_path)

    loaded = validate_pit_bundle(bundle)

    assert loaded.bundle_id == "pit-test-bundle"
    assert loaded.pit_grade is True
    assert loaded.schedule == (
        {
            "as_of_date": "2025-01-02",
            "ticker": "000001.SZ",
            "listed_on_or_before": "2020-01-01",
            "delisted_after": "",
            "source_file": "sources/universe-2025-01-02.csv",
            "source_sha256": _sha256(bundle / "sources/universe-2025-01-02.csv"),
        },
        {
            "as_of_date": "2025-01-02",
            "ticker": "000002.SZ",
            "listed_on_or_before": "2020-01-01",
            "delisted_after": "",
            "source_file": "sources/universe-2025-01-02.csv",
            "source_sha256": _sha256(bundle / "sources/universe-2025-01-02.csv"),
        },
    )
    with pytest.raises(TypeError):
        loaded.schedule[0]["ticker"] = "mutated"


def test_validate_pit_bundle_rejects_invalid_utf8_manifest(tmp_path):
    bundle = _make_bundle(tmp_path)
    (bundle / "manifest.json").write_bytes(b"\xff")

    with pytest.raises(PitBundleValidationError, match="manifest is unparseable"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_normalizes_deeply_nested_manifest_json_recursion_error(tmp_path):
    bundle = _make_bundle(tmp_path)
    (bundle / "manifest.json").write_text("{" + "\"extra\": " + "[" * 2_000 + "0" + "]" * 2_000 + "}", encoding="utf-8")

    with pytest.raises(PitBundleValidationError, match="manifest is unparseable"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_unvalidated_extra_key_beyond_freeze_depth_limit(tmp_path):
    bundle = _make_bundle(tmp_path)
    extra: object = "leaf"
    for _ in range(101):
        extra = {"nested": extra}
    _rewrite_manifest(bundle, lambda manifest: manifest.update(extra=extra))

    with pytest.raises(PitBundleValidationError, match="depth"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_invalid_utf8_schedule_with_valid_hash(tmp_path):
    bundle = _make_bundle(tmp_path)
    schedule = bundle / "universe_schedule.csv"
    schedule.write_bytes(b"\xff")
    _rewrite_manifest(
        bundle,
        lambda manifest: manifest["hashes"].update({"universe_schedule.csv": _sha256(schedule)}),
    )
    _rewrite_manifest(bundle, lambda manifest: manifest.update(composite_sha256=_composite(manifest["hashes"])))

    with pytest.raises(PitBundleValidationError, match="unparseable universe_schedule.csv"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_csv_field_size_error(tmp_path):
    bundle = _make_bundle(tmp_path)
    schedule = bundle / "universe_schedule.csv"
    schedule.write_text("x" * (csv.field_size_limit() + 1), encoding="utf-8")
    _rewrite_manifest(
        bundle,
        lambda manifest: manifest["hashes"].update({"universe_schedule.csv": _sha256(schedule)}),
    )
    _rewrite_manifest(bundle, lambda manifest: manifest.update(composite_sha256=_composite(manifest["hashes"])))

    with pytest.raises(PitBundleValidationError, match="unparseable universe_schedule.csv"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_hash_read_os_error(tmp_path, monkeypatch):
    bundle = _make_bundle(tmp_path)
    unreadable = bundle / "sources" / "universe-2025-01-02.csv"
    original_sha256_file = pit_bundle.sha256_file

    def raise_for_unreadable(path):
        if Path(path) == unreadable:
            raise OSError("simulated read failure")
        return original_sha256_file(path)

    monkeypatch.setattr(pit_bundle, "sha256_file", raise_for_unreadable)

    with pytest.raises(PitBundleValidationError, match="unable to hash manifest-listed file"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_false_pit_grade(tmp_path):
    bundle = _make_bundle(tmp_path)
    _rewrite_manifest(bundle, lambda manifest: manifest.update(pit_grade=False))

    with pytest.raises(PitBundleValidationError, match="pit_grade"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_unhashed_raw_source(tmp_path):
    bundle = _make_bundle(tmp_path)
    (bundle / "sources" / "unlisted.csv").write_text("ticker\n000003.SZ\n", encoding="utf-8")

    with pytest.raises(PitBundleValidationError, match="unhashed raw source"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_unhashed_nested_manifest_named_source(tmp_path):
    bundle = _make_bundle(tmp_path)
    (bundle / "sources" / "manifest.json").write_text('{"unexpected": true}\n', encoding="utf-8")

    with pytest.raises(PitBundleValidationError, match="unhashed raw source: sources/manifest.json"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_hash_listed_nested_manifest_source(tmp_path):
    bundle = _make_bundle(tmp_path)
    original_source = bundle / "sources" / "universe-2025-01-02.csv"
    nested_manifest = bundle / "sources" / "manifest.json"
    original_source.unlink()
    nested_manifest.write_text('{"unexpected": true}\n', encoding="utf-8")
    nested_manifest_hash = _sha256(nested_manifest)
    _replace_schedule(
        bundle,
        [f"2025-01-02,000001.SZ,2020-01-01,,sources/manifest.json,{nested_manifest_hash}"],
    )

    def replace_source_hash(manifest):
        manifest["hashes"].pop("sources/universe-2025-01-02.csv")
        manifest["hashes"]["sources/manifest.json"] = nested_manifest_hash
        manifest["composite_sha256"] = _composite(manifest["hashes"])

    _rewrite_manifest(bundle, replace_source_hash)

    with pytest.raises(PitBundleValidationError, match="manifest.json"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_unreferenced_manifest_source_hash(tmp_path):
    bundle = _make_bundle(tmp_path)
    unreferenced = bundle / "sources" / "unreferenced.csv"
    unreferenced.write_text("ticker\n000003.SZ\n", encoding="utf-8")

    def add_unreferenced_hash(manifest):
        manifest["hashes"]["sources/unreferenced.csv"] = _sha256(unreferenced)
        manifest["composite_sha256"] = _composite(manifest["hashes"])

    _rewrite_manifest(bundle, add_unreferenced_hash)

    with pytest.raises(PitBundleValidationError, match="unreferenced manifest source"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_whitespace_in_manifest_hash_path(tmp_path):
    bundle = _make_bundle(tmp_path)

    def add_control_character_path(manifest):
        manifest["hashes"]["sources/control\ncharacter.csv"] = "0" * 64
        manifest["composite_sha256"] = _composite(manifest["hashes"])

    _rewrite_manifest(bundle, add_control_character_path)

    with pytest.raises(PitBundleValidationError, match="invalid manifest hash path"):
        validate_pit_bundle(bundle)


@pytest.mark.parametrize("character", ["\x7f", "\x80", "\u202e", "\u00a0"], ids=["del", "c1", "rtl-override", "nbsp"])
def test_validate_pit_bundle_rejects_unicode_controls_in_manifest_hash_path(tmp_path, character):
    bundle = _make_bundle(tmp_path)
    unsafe_path = f"sources/control{character}character.csv"

    def add_unsafe_path(manifest):
        manifest["hashes"][unsafe_path] = "0" * 64
        manifest["composite_sha256"] = _composite(manifest["hashes"])

    _rewrite_manifest(bundle, add_unsafe_path)

    with pytest.raises(PitBundleValidationError, match="invalid manifest hash path"):
        validate_pit_bundle(bundle)


@pytest.mark.parametrize("character", ["\x7f", "\x80", "\u202e", "\u00a0"], ids=["del", "c1", "rtl-override", "nbsp"])
def test_validate_pit_bundle_rejects_unicode_controls_in_schedule_source_path(tmp_path, character):
    bundle = _make_bundle(tmp_path)
    original_source = bundle / "sources" / "universe-2025-01-02.csv"
    unsafe_source = bundle / "sources" / f"control{character}character.csv"
    original_source.rename(unsafe_source)
    unsafe_path = f"sources/{unsafe_source.name}"
    unsafe_hash = _sha256(unsafe_source)
    _replace_schedule(
        bundle,
        [f"2025-01-02,000001.SZ,2020-01-01,,{unsafe_path},{unsafe_hash}"],
    )

    def replace_source_hash(manifest):
        manifest["hashes"].pop("sources/universe-2025-01-02.csv")
        manifest["hashes"][unsafe_path] = unsafe_hash
        manifest["composite_sha256"] = _composite(manifest["hashes"])

    _rewrite_manifest(bundle, replace_source_hash)

    with pytest.raises(PitBundleValidationError, match="schedule source is not under sources"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_schedule_date_outside_manifest_dates(tmp_path):
    bundle = _make_bundle(tmp_path)
    source_hash = _sha256(bundle / "sources/universe-2025-01-02.csv")
    _replace_schedule(
        bundle,
        [f"2025-01-03,000001.SZ,2020-01-01,,sources/universe-2025-01-02.csv,{source_hash}"],
    )

    with pytest.raises(PitBundleValidationError, match="outside manifest.as_of_dates"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_duplicate_schedule_identity(tmp_path):
    bundle = _make_bundle(tmp_path)
    source_hash = _sha256(bundle / "sources/universe-2025-01-02.csv")
    _replace_schedule(
        bundle,
        [
            f"2025-01-02,000001.SZ,2020-01-01,,sources/universe-2025-01-02.csv,{source_hash}",
            f"2025-01-02,000001.SZ,2020-01-01,,sources/universe-2025-01-02.csv,{source_hash}",
        ],
    )

    with pytest.raises(PitBundleValidationError, match="duplicate schedule identity"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_schedule_source_missing_manifest_hash(tmp_path):
    bundle = _make_bundle(tmp_path)
    source = bundle / "sources" / "other.csv"
    source.write_text("ticker\n000003.SZ\n", encoding="utf-8")
    source_hash = _sha256(source)
    _replace_schedule(
        bundle,
        [f"2025-01-02,000003.SZ,2020-01-01,,sources/other.csv,{source_hash}"],
    )

    with pytest.raises(PitBundleValidationError, match="missing manifest hash"):
        validate_pit_bundle(bundle)


@pytest.mark.parametrize(
    ("as_of_date", "listed_on_or_before", "delisted_after", "message"),
    [
        ("2025/01/02", "2020-01-01", "", "ISO YYYY-MM-DD"),
        ("2025-01-02", "", "", "blank required fields"),
        ("2025-01-02", "2025-01-03", "", "not listed"),
        ("2025-01-02", "2020-01-01", "2025-01-02", "already delisted"),
    ],
)
def test_validate_pit_bundle_rejects_invalid_listing_schedule_dates(
    tmp_path, as_of_date, listed_on_or_before, delisted_after, message
):
    bundle = _make_bundle(tmp_path)
    source_hash = _sha256(bundle / "sources/universe-2025-01-02.csv")
    _replace_schedule(
        bundle,
        [
            f"{as_of_date},000001.SZ,{listed_on_or_before},{delisted_after},"
            f"sources/universe-2025-01-02.csv,{source_hash}"
        ],
    )

    with pytest.raises(PitBundleValidationError, match=message):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_empty_schedule(tmp_path):
    bundle = _make_bundle(tmp_path)
    _replace_schedule(bundle, [])

    with pytest.raises(PitBundleValidationError, match="schedule must not be empty"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_requires_every_manifest_date_in_schedule(tmp_path):
    bundle = _make_bundle(tmp_path)
    _rewrite_manifest(bundle, lambda manifest: manifest.update(as_of_dates=["2025-01-02", "2025-01-03"]))

    with pytest.raises(PitBundleValidationError, match="missing schedule dates"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_symlinked_bundle_root(tmp_path):
    bundle = _make_bundle(tmp_path)
    alias = tmp_path / "pit-bundle-alias"
    alias.symlink_to(bundle, target_is_directory=True)

    with pytest.raises(PitBundleValidationError, match="symlink"):
        validate_pit_bundle(alias)


@pytest.mark.parametrize("symlink_directory", [False, True], ids=["source-file", "sources-directory"])
def test_validate_pit_bundle_rejects_external_source_symlinks(tmp_path, symlink_directory):
    bundle = _make_bundle(tmp_path)
    external = tmp_path / "external"
    external.mkdir()
    external_source = external / "universe-2025-01-02.csv"
    external_source.write_text("ticker\n000001.SZ\n000002.SZ\n", encoding="utf-8")
    source = bundle / "sources" / external_source.name
    source.unlink()
    if symlink_directory:
        (bundle / "sources").rmdir()
        (bundle / "sources").symlink_to(external, target_is_directory=True)
    else:
        source.symlink_to(external_source)

    with pytest.raises(PitBundleValidationError, match="symlink|outside bundle root"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_legacy_source_path_schema(tmp_path):
    bundle = _make_bundle(tmp_path)
    schedule = bundle / "universe_schedule.csv"
    schedule.write_text(
        "as_of_date,ticker,listed_on_or_before,delisted_after,source_path,source_sha256\n",
        encoding="utf-8",
    )
    _rewrite_manifest(
        bundle,
        lambda manifest: manifest["hashes"].update({"universe_schedule.csv": _sha256(schedule)}),
    )
    _rewrite_manifest(bundle, lambda manifest: manifest.update(composite_sha256=_composite(manifest["hashes"])))

    with pytest.raises(PitBundleValidationError, match="missing required columns"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_unsafe_source_file_path(tmp_path):
    bundle = _make_bundle(tmp_path)
    source_hash = _sha256(bundle / "sources/universe-2025-01-02.csv")
    _replace_schedule(
        bundle,
        [f"2025-01-02,000001.SZ,2020-01-01,,sources/../universe_schedule.csv,{source_hash}"],
    )

    with pytest.raises(PitBundleValidationError, match="source is not under sources/"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_tampered_source_hash_and_composite(tmp_path):
    bundle = _make_bundle(tmp_path)
    source = bundle / "sources/universe-2025-01-02.csv"
    source.write_text("tampered", encoding="utf-8")
    _rewrite_manifest(bundle, lambda manifest: manifest.update(composite_sha256="0" * 64))

    with pytest.raises(PitBundleValidationError, match=r"(?s)hash mismatch.*composite hash mismatch"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_manifest_hash_path_traversal(tmp_path):
    bundle = _make_bundle(tmp_path)

    def add_traversal_path(manifest):
        manifest["hashes"]["sources/../escape.csv"] = "0" * 64
        manifest["composite_sha256"] = _composite(manifest["hashes"])

    _rewrite_manifest(bundle, add_traversal_path)

    with pytest.raises(PitBundleValidationError, match="invalid manifest hash path"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_dot_component_in_manifest_hash_path(tmp_path):
    bundle = _make_bundle(tmp_path)
    source = bundle / "sources" / "universe-2025-01-02.csv"

    def add_dot_component_alias(manifest):
        manifest["hashes"]["sources/./universe-2025-01-02.csv"] = _sha256(source)
        manifest["composite_sha256"] = _composite(manifest["hashes"])

    _rewrite_manifest(bundle, add_dot_component_alias)

    with pytest.raises(PitBundleValidationError, match="invalid manifest hash path"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_dot_component_in_schedule_source_path(tmp_path):
    bundle = _make_bundle(tmp_path)
    source_hash = _sha256(bundle / "sources/universe-2025-01-02.csv")
    _replace_schedule(
        bundle,
        [f"2025-01-02,000001.SZ,2020-01-01,,sources/./universe-2025-01-02.csv,{source_hash}"],
    )

    with pytest.raises(PitBundleValidationError, match="schedule source is not under sources"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_non_source_manifest_hash_file(tmp_path):
    bundle = _make_bundle(tmp_path)
    note = bundle / "note.txt"
    note.write_text("unexpected payload\n", encoding="utf-8")

    def add_non_source_file(manifest):
        manifest["hashes"]["note.txt"] = _sha256(note)
        manifest["composite_sha256"] = _composite(manifest["hashes"])

    _rewrite_manifest(bundle, add_non_source_file)

    with pytest.raises(PitBundleValidationError, match="may contain only"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_symlinked_root_manifest(tmp_path):
    bundle = _make_bundle(tmp_path)
    manifest = bundle / "manifest.json"
    external_manifest = tmp_path / "external-manifest.json"
    external_manifest.write_bytes(manifest.read_bytes())
    manifest.unlink()
    manifest.symlink_to(external_manifest)

    with pytest.raises(PitBundleValidationError, match="manifest must not be a symlink"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_duplicate_manifest_dates(tmp_path):
    bundle = _make_bundle(tmp_path)
    _rewrite_manifest(bundle, lambda manifest: manifest.update(as_of_dates=["2025-01-02", "2025-01-02"]))

    with pytest.raises(PitBundleValidationError, match="must not contain duplicates"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_truthy_non_boolean_pit_grade(tmp_path):
    bundle = _make_bundle(tmp_path)
    _rewrite_manifest(bundle, lambda manifest: manifest.update(pit_grade=1))

    with pytest.raises(PitBundleValidationError, match="literal true"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_rejects_standalone_composite_mismatch(tmp_path):
    bundle = _make_bundle(tmp_path)
    _rewrite_manifest(bundle, lambda manifest: manifest.update(composite_sha256="0" * 64))

    with pytest.raises(PitBundleValidationError, match="composite hash mismatch"):
        validate_pit_bundle(bundle)


def test_validate_pit_bundle_returns_immutable_manifest(tmp_path):
    bundle = _make_bundle(tmp_path)

    loaded = validate_pit_bundle(bundle)

    manifest = cast(dict[str, object], loaded.manifest)
    hashes = cast(dict[str, str], loaded.manifest["hashes"])
    with pytest.raises(TypeError):
        manifest["bundle_id"] = "mutated"
    with pytest.raises(TypeError):
        hashes["universe_schedule.csv"] = "0" * 64
