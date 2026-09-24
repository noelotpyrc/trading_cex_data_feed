"""Causal consumer of frozen artifacts. No fitting, labels, costs or order placement."""
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd
from .contract import MINUTE, digest, expiry_ms, release_ms
from .historical_features import build_features, RETAINED_ARRAYS, BAR_COLUMNS


class Unavailable(ValueError):
    pass


def f32(value):
    return float(np.float32(value))


def released_inputs(store, t, asof):
    values = {name: np.nan for name in RETAINED_ARRAYS}
    used = []
    for source in ("mark", "premium", "funding", "coinbase", "deribit"):
        lookback = (481 if source == "funding" else 20) * MINUTE
        rows = store.rows(source, t - lookback, t, asof)
        eligible = [r for r in rows if release_ms(source, r.t) <= t < expiry_ms(source, r.t)]
        if not eligible:
            raise Unavailable(f"missing_or_stale:{source}")
        row = eligible[-1]
        used.append(row.fingerprint)
        if source in ("mark", "premium"):
            prefix = "mark_price" if source == "mark" else "premium_index"
            values.update({f"{prefix}_{k}": f32(v) for k, v in row.values.items()})
        elif source == "funding":
            values["funding_last_funding_rate"] = f32(row.values["rate"])
        else:
            previous = next((r for r in rows if r.t == row.t - 5 * MINUTE), None)
            if previous is None:
                raise Unavailable(f"missing_previous:{source}")
            used.append(previous.fingerprint)
            v = row.values
            values[f"{source}_ret5"] = f32(np.log(v["close"]) - np.log(v["open"]))
            values[f"{source}_prev_ret5"] = f32(np.log(v["close"] / previous.values["close"]))
            values[f"{source}_log_volume5"] = f32(np.log1p(v["volume"]))
    values["cross_deribit_coinbase_ret_diff"] = f32(values["deribit_ret5"] - values["coinbase_ret5"])
    values["cross_deribit_coinbase_prev_ret_diff"] = f32(values["deribit_prev_ret5"] - values["coinbase_prev_ret5"])
    index = pd.to_datetime([t], unit="ms", utc=True)
    return pd.DataFrame([values], index=index), used


def rebuild_ema(store, seed, last_bar, cutoff):
    """Exact recurrence from the fixed seed, bounded memory, current as-of versions."""
    ema = seed["ema"]
    alpha = 2 / 1441
    start = seed["last_bar_ms"] + MINUTE
    while start <= last_bar:
        end = min(last_bar + MINUTE, start + 2048 * MINUTE)
        rows = store.rows("binance", start, end, cutoff)
        if [r.t for r in rows] != list(range(start, end, MINUTE)):
            raise Unavailable("missing_revised_ema_history")
        for row in rows:
            ema = alpha * row.values["close"] + (1 - alpha) * ema
        start = end
    return ema


class Bundle:
    """Load trusted local joblib models only after checksum and temporal validation."""
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.data = json.loads(self.path.read_text())
        if self.data.get("schema_version") != 1:
            raise ValueError("Unknown bundle schema")
        self.models = {}
        if len(self.data["columns"]) != 40 or len(set(self.data["columns"])) != 40:
            raise ValueError("Expected 40 unique ordered feature names")
        contract = json.loads(Path(__file__).with_name("feature_contract.json").read_text())
        if self.data["columns"] != contract["columns"]:
            raise ValueError("Feature order differs from historical contract")
        if hashlib.sha256(Path(__file__).with_name("historical_features.py").read_bytes()).hexdigest() != contract["historical_features_sha256"]:
            raise ValueError("Historical feature implementation changed")
        for key in ("quarters", "months"):
            items = sorted(self.data[key], key=lambda r: r["start_ms"])
            if any(a["end_ms"] > b["start_ms"] for a, b in zip(items, items[1:])):
                raise ValueError("Overlapping artifact validity intervals")
            for row in items:
                if row["end_ms"] <= row["start_ms"] or row["calibration_end_ms"] > row["start_ms"]:
                    raise ValueError("Invalid artifact timing")
                cuts = [row[k] for k in (("q75", "q90") if key == "quarters" else ("q10", "q95"))]
                if not np.isfinite(cuts).all() or cuts[0] > cuts[1]:
                    raise ValueError("Invalid cutoffs")
        seed = self.data["bootstrap"]
        if seed["last_bar_ms"] % MINUTE or seed["ema"] <= 0 or not np.isfinite(seed["ema"]):
            raise ValueError("Invalid exact EMA bootstrap")

    def active(self, key, t):
        rows = [r for r in self.data[key] if r["start_ms"] <= t < r["end_ms"]]
        if len(rows) != 1:
            raise Unavailable(f"missing_or_expired:{key}")
        return rows[0]

    def predict(self, quarter, x):
        sha = quarter["model_sha256"]
        if sha not in self.models:
            import joblib
            p = (self.path.parent / quarter["model_file"]).resolve()
            if not p.is_relative_to(self.path.parent):
                raise ValueError("Model must reside inside bundle directory")
            if hashlib.sha256(p.read_bytes()).hexdigest() != sha:
                raise ValueError("Model checksum mismatch")
            self.models[sha] = joblib.load(p)
        return float(self.models[sha].predict(np.asarray(x, dtype=np.float64).reshape(1, -1))[0])


class Engine:
    def __init__(self, store, bundle, run="live", deadline_ms=30_000):
        self.store, self.bundle, self.run, self.deadline_ms = store, bundle, run, deadline_ms

    def evaluate(self, t, emitted_ms, *, replay=False):
        if t % MINUTE or emitted_ms < t:
            raise ValueError("Decision needs UTC minute and nonfuture knowledge time")
        old = self.store.decision(self.run, t)
        if old is not None:
            return old
        seed = self.bundle.data["bootstrap"]
        seed_hash = digest(seed)
        state = self.store.checkpoint(self.run) or {**seed, "seed_hash": seed_hash}
        expected_previous = state.get("last_decision_ms", -1)
        if state.get("seed_hash") != seed_hash:
            raise ValueError("Bootstrap changed; use a new run identifier")
        if t <= state.get("last_decision_ms", -1):
            raise ValueError("Decisions must be strictly ordered")
        cutoff = min(emitted_ms, t + self.deadline_ms)
        decision = dict(t=t, emitted_ms=emitted_ms, knowledge_cutoff_ms=cutoff,
                        mode="replay" if replay else "live", status="unavailable", reasons=[],
                        fires={}, bundle_sha256=digest(self.bundle.data))
        try:
            if not replay and emitted_ms > t + self.deadline_ms:
                raise Unavailable("decision_deadline_missed")
            if t <= seed["last_bar_ms"]:
                raise Unavailable("bootstrap_is_in_the_future")
            quarter = self.bundle.active("quarters", t)
            month = self.bundle.active("months", t)
            if state.get("revision_blocked"):
                raise Unavailable("history_revision_requires_rebootstrap")
            if "knowledge_cutoff_ms" in state:
                changed_times = self.store.db.execute('''SELECT DISTINCT t FROM observations WHERE source='binance'
                    AND t<=? AND received_ms>? AND received_ms<=?''',
                    (state["last_bar_ms"], state["knowledge_cutoff_ms"], cutoff)).fetchall()
                # Only close revisions invalidate recursive EMA state. Activity/OHLC
                # revisions remain versioned and feed the next fresh RV prediction;
                # previously issued scores/decisions remain immutable.
                close_revision = False
                for changed in changed_times:
                    old_rows = self.store.rows("binance", changed[0], changed[0] + MINUTE, state["knowledge_cutoff_ms"])
                    new_rows = self.store.rows("binance", changed[0], changed[0] + MINUTE, cutoff)
                    if not old_rows or not new_rows or old_rows[0].values["close"] != new_rows[0].values["close"]:
                        if changed[0] <= seed["last_bar_ms"]:
                            state["revision_blocked"] = True
                            raise Unavailable("history_revision_requires_rebootstrap")
                        close_revision = True
                if close_revision:
                    state["ema_rebuild_required"] = True
            if state.get("ema_rebuild_required"):
                # Persist a pending rebuild if required history is missing. A later
                # repair can recover, without changing any previously issued row.
                state["ema"] = rebuild_ema(self.store, seed, state["last_bar_ms"], cutoff)
                state.pop("ema_rebuild_required")
                decision["ema_history_rebuilt"] = True
            updates = self.store.rows("binance", state["last_bar_ms"] + MINUTE, t, cutoff)
            expected = list(range(state["last_bar_ms"] + MINUTE, t, MINUTE))
            if [r.t for r in updates] != expected:
                raise Unavailable("missing_ema_history")
            alpha = 2 / 1441
            for row in updates:
                state["ema"] = alpha * row.values["close"] + (1 - alpha) * state["ema"]
                state["last_bar_ms"] = row.t
            window = 10081 if t % (15 * MINUTE) == 0 else 1441
            bars = self.store.rows("binance", t - window * MINUTE, t, cutoff)
            if [r.t for r in bars] != list(range(t - window * MINUTE, t, MINUTE)):
                raise Unavailable("missing_contiguous_lookback")
            close = np.array([r.values["close"] for r in bars[-1441:]], dtype=float)
            ret = np.diff(np.log(close))
            rv = float(np.sqrt(np.sum(ret * ret)))
            if rv <= 0:
                raise Unavailable("zero_rv")
            x = float(np.log(close[-1] / state["ema"]) / rv)
            anchor = t // (15 * MINUTE) * (15 * MINUTE)
            if t == anchor:
                released, auxiliary_hashes = released_inputs(self.store, t, cutoff)
                frame = pd.DataFrame([r.values for r in bars], index=pd.to_datetime([r.t for r in bars], unit="ms", utc=True))
                features = build_features(frame, released, anchors=released.index)[self.bundle.data["columns"]].iloc[0].to_numpy()
                if not np.isfinite(features).all():
                    raise Unavailable("incomplete_features")
                score = self.bundle.predict(quarter, features)
                if not np.isfinite(score):
                    raise Unavailable("nonfinite_score")
                state.update(score=score, score_t=t, model_sha256=quarter["model_sha256"])
                decision.update(features=features.tolist(), auxiliary_hashes=auxiliary_hashes)
            if state.get("score_t") != anchor or state.get("model_sha256") != quarter["model_sha256"]:
                raise Unavailable("missing_current_rv_score")
            score = state["score"]
            fires = {}
            for q in ("q75", "q90"):
                high = score > quarter[q]
                fires[q] = {"regime_high": bool(high), "low10_long": bool(high and x <= month["q10"]),
                            "top5_short": bool(high and x > month["q95"])}
            eligible = anchor >= quarter["start_ms"] + 10081 * MINUTE and t + 1440 * MINUTE < quarter["end_ms"]
            decision.update(status="ok", score=score, score_t=anchor, ema_dist=x, ema=state["ema"],
                            thresholds={k:quarter[k] for k in ("q75","q90")} | {k:month[k] for k in ("q10","q95")},
                            fires=fires, report_eligible=eligible, model_sha256=quarter["model_sha256"],
                            bar_input_sha256=digest([r.fingerprint for r in bars]))
        except Unavailable as error:
            decision["reasons"].append(str(error))
        state.update(last_decision_ms=t, knowledge_cutoff_ms=cutoff)
        return self.store.commit_decision(self.run, t, decision, state, expected_previous=expected_previous)
