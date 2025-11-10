from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class PersistConfig:
    root_dir: Path
    dataset_slug: str

    def dataset_dir(self) -> Path:
        d = self.root_dir / self.dataset_slug
        d.mkdir(parents=True, exist_ok=True)
        return d


def now_utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def write_raw_snapshot(cfg: PersistConfig, run_id: str, df: pd.DataFrame) -> Path:
    out = cfg.dataset_dir() / f"{run_id}_api_pull.csv"
    # Ensure column order
    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    df_to_write = df.loc[:, cols].copy()
    # Persist as CSV with ISO-like timestamp
    df_to_write.to_csv(out, index=False)
    return out

