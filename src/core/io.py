"""
I/O and Output Management

Handles dated output directories, file saving, and run metadata.
"""

from __future__ import annotations
import os
import json
import hashlib
from pathlib import Path
from datetime import date, datetime
from typing import Any, Optional
import pandas as pd

# Import NY timezone helper
from .helpers import get_ny_date
from src.utils.time import utc_now_iso_z


def get_run_dir(base_date: Optional[date] = None, root_dir: str = "outputs") -> Path:
    """
    Get or create dated output directory.
    
    Uses trading dates only (weekdays, excludes weekends).
    If base_date is a weekend, uses the previous Friday.
    If base_date is before market close, uses previous trading day.
    
    Args:
        base_date: Date for directory name (defaults to last trading day in NY timezone)
        root_dir: Root output directory (defaults to "outputs")
    
    Returns:
        Path to dated output directory (YYYY-MM-DD format)
    """
    from .helpers import get_trading_date
    
    if base_date is None:
        base_date = get_trading_date()
    else:
        # Ensure we use a trading date even if user provided a weekend date
        base_date = get_trading_date(base_date)
    
    date_str = base_date.strftime("%Y-%m-%d")
    run_dir = Path(root_dir) / date_str
    run_dir.mkdir(parents=True, exist_ok=True)
    
    return run_dir


def save_csv(df: pd.DataFrame, path: Path | str, index: bool = False) -> Path:
    """
    Save DataFrame to CSV file.
    
    Args:
        df: DataFrame to save
        path: File path (Path or str)
        index: Whether to include index in CSV
    
    Returns:
        Path to saved file
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=index)
    return path


def save_json(obj: Any, path: Path | str, indent: int = 2) -> Path:
    """
    Save object to JSON file.
    
    Args:
        obj: Object to save (must be JSON serializable)
        path: File path (Path or str)
        indent: JSON indentation (default: 2)
    
    Returns:
        Path to saved file
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=indent, default=str, ensure_ascii=False)
    
    return path


def save_run_metadata(
    run_dir: Path,
    method_version: str,
    config: Optional[dict] = None,
    **additional_metadata: Any,
) -> Path:
    """
    Save run metadata JSON file.
    
    Args:
        run_dir: Output directory
        method_version: Version string (e.g., "v3.0")
        config: Optional config dict (used to compute hash)
        **additional_metadata: Additional metadata fields
    
    Returns:
        Path to metadata file
    """
    metadata = {
        "method_version": method_version,
        "run_timestamp_utc": utc_now_iso_z(),
        "run_timestamp_et": pd.Timestamp.now(tz="America/New_York").isoformat(),
    }
    
    # Add config hash if provided
    if config:
        try:
            config_str = json.dumps(config, sort_keys=True, default=str)
            config_hash = hashlib.sha256(config_str.encode("utf-8")).hexdigest()[:12]
            metadata["config_hash"] = config_hash
        except Exception:
            pass
    
    # Add additional metadata
    metadata.update(additional_metadata)
    
    metadata_file = run_dir / "run_metadata.json"
    return save_json(metadata, metadata_file)



def read_reconciliation(date: str, *, root_dir: str = "outputs") -> Optional[dict]:
    """Load the day's cross-source health check, or None when it did not run.

    Returns None rather than raising: the check is non-binding, so its absence
    must degrade the alert to "unverified" and never block or crash the morning
    message that carries the actual picks.
    """
    path = Path(root_dir) / date / f"source_reconciliation_{date}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None
