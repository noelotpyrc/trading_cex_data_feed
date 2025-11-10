from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    reason: str
    validated_rows: int


def _is_strictly_hourly(df: pd.DataFrame) -> bool:
    if df.empty:
        return True
    diffs = df["timestamp"].diff().dropna().dt.total_seconds()
    return (diffs == 3600).all()


def validate_window(api_df: pd.DataFrame, db_df: pd.DataFrame, target_hour: pd.Timestamp, tolerance: float = 1e-8) -> ValidationResult:
    """Validate that API window [t-N+1, t-1] matches DB last N-1 rows.

    - Ensures timestamps match exactly and are hourly spaced.
    - Compares OHLCV values with absolute tolerance.
    - api_df must include at least one row at t (target_hour); that row is not part of validation set.
    """
    if api_df.empty:
        return ValidationResult(False, "api_df empty", 0)

    if api_df.iloc[-1]["timestamp"] != target_hour:
        return ValidationResult(False, "api last row is not target_hour", 0)

    # Validation window excludes the last row (t)
    api_hist = api_df.iloc[:-1].copy()
    if api_hist.empty:
        # Nothing to validate, allow append of t
        return ValidationResult(True, "no historical window to validate", 0)

    if not _is_strictly_hourly(api_df[["timestamp"]].copy().assign(dummy=0)):
        # Recompute hourly spacing on timestamps only
        ts = api_df["timestamp"].sort_values().reset_index(drop=True)
        diffs = ts.diff().dropna().dt.total_seconds()
        if not (diffs == 3600).all():
            return ValidationResult(False, "api window not strictly hourly", 0)

    # DB rows must match length of api_hist to compare fully
    if len(db_df) < len(api_hist):
        return ValidationResult(False, "db has fewer rows than validation window", 0)

    # Align the last len(api_hist) rows of DB to compare
    db_tail = db_df.tail(len(api_hist)).reset_index(drop=True)
    api_hist = api_hist.reset_index(drop=True)

    # Timestamps must match exactly
    if not (db_tail["timestamp"].tolist() == api_hist["timestamp"].tolist()):
        return ValidationResult(False, "timestamp mismatch between api and db", 0)

    # Compare values within tolerance
    for col in ["open", "high", "low", "close", "volume"]:
        diff = (db_tail[col] - api_hist[col]).abs().max()
        if pd.isna(diff):
            return ValidationResult(False, f"NaN in column {col}", 0)
        if diff > tolerance:
            return ValidationResult(False, f"mismatch in {col} (max diff {diff})", 0)

    return ValidationResult(True, "validated", len(api_hist))

