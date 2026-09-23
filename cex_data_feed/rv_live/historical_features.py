from __future__ import annotations

from collections.abc import Mapping
import numpy as np
import pandas as pd

BAR_COLUMNS = ("open", "high", "low", "close", "volume", "quote_asset_volume", "num_trades", "taker_buy_base_volume")
RETAINED_ARRAYS = (
    "coinbase_log_volume5", "coinbase_prev_ret5", "coinbase_ret5",
    "deribit_log_volume5", "deribit_prev_ret5", "deribit_ret5",
    "cross_deribit_coinbase_prev_ret_diff", "cross_deribit_coinbase_ret_diff",
    "funding_last_funding_rate", "mark_price_open", "mark_price_high", "mark_price_low", "mark_price_close",
    "premium_index_open", "premium_index_high", "premium_index_low", "premium_index_close",
    "dvol_level", "dvol_prev_ret1h", "dvol_ret1h", "metrics_sum_open_interest",
    "metrics_sum_open_interest_value",
)
CONSUMPTION = {
    "coinbase_log_volume5": ("X04_CB_LOGVOL5",), "coinbase_prev_ret5": ("X06_CB_PREV_RET5",),
    "coinbase_ret5": ("X01_CB_RET5",), "deribit_log_volume5": ("X05_DER_LOGVOL5",),
    "deribit_prev_ret5": ("X07_DER_PREV_RET5",), "deribit_ret5": ("X02_DER_RET5",),
    "cross_deribit_coinbase_prev_ret_diff": ("X08_DER_CB_PREV_RET_DIFF",),
    "cross_deribit_coinbase_ret_diff": ("X03_DER_CB_RET_DIFF",),
    "funding_last_funding_rate": ("D01_FUNDING",), "mark_price_open": ("D04_MARK_BODY",),
    "mark_price_high": ("D03_MARK_RANGE",), "mark_price_low": ("D03_MARK_RANGE",),
    "mark_price_close": ("D02_MARK_BASIS", "D04_MARK_BODY"), "premium_index_open": ("D07_PREMIUM_BODY",),
    "premium_index_high": ("D06_PREMIUM_RANGE",), "premium_index_low": ("D06_PREMIUM_RANGE",),
    "premium_index_close": ("D05_PREMIUM_CLOSE", "D07_PREMIUM_BODY"),
    "dvol_level": ("D08_DVOL_LOG",), "dvol_prev_ret1h": ("D09_DVOL_PREV_RET1H",),
    "dvol_ret1h": ("D10_DVOL_RET1H",), "metrics_sum_open_interest": ("D11_OI_LOG",),
    "metrics_sum_open_interest_value": ("D12_OI_VALUE_LOG",),
}


def _utc_index(frame: pd.DataFrame, name: str) -> pd.DatetimeIndex:
    if not isinstance(frame.index, pd.DatetimeIndex):
        raise TypeError(f"{name} must have a DatetimeIndex")
    idx = frame.index
    if idx.tz is None:
        raise ValueError(f"{name} index must be timezone-aware UTC")
    idx = idx.tz_convert("UTC")
    if not idx.is_monotonic_increasing or idx.has_duplicates:
        raise ValueError(f"{name} index must be strictly increasing and unique")
    return idx


def _contiguous_end_mask(index: pd.DatetimeIndex, window: int) -> np.ndarray:
    n = len(index)
    good = np.zeros(n, dtype=bool)
    if n >= window:
        ns = index.as_unit("ns").asi8
        positions = np.arange(window - 1, n)
        good[positions] = ns[positions] - ns[positions - window + 1] == (window - 1) * 60_000_000_000
    return good


def _roll(series: pd.Series, window: int, op: str) -> pd.Series:
    roller = series.rolling(window=window, min_periods=window)
    if op == "sum": out = roller.sum()
    elif op == "min": out = roller.min()
    elif op == "max": out = roller.max()
    else: raise ValueError(op)
    return out.where(_contiguous_end_mask(series.index, window))


def _safe_log_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    valid = (numerator > 0) & (denominator > 0) & np.isfinite(numerator) & np.isfinite(denominator)
    return pd.Series(np.where(valid, np.log(numerator / denominator), np.nan), index=numerator.index, dtype="float64")


def build_features(bars: pd.DataFrame, released: pd.DataFrame, *, anchors: pd.DatetimeIndex | None = None) -> pd.DataFrame:
    """Build all 45 registered columns at decision minute d using bars only through d-1.

    ``released`` must already contain the causal Phase 0 values at d. Values are matched exactly by
    timestamp. This function never fills, carries, or imputes them.
    """
    idx = _utc_index(bars, "bars")
    missing = sorted(set(BAR_COLUMNS) - set(bars.columns))
    if missing: raise ValueError(f"missing bar columns: {missing}")
    ridx = _utc_index(released, "released")
    missing = sorted(set(RETAINED_ARRAYS) - set(released.columns))
    if missing: raise ValueError(f"missing retained arrays: {missing}")
    if anchors is None:
        anchors = idx[(idx.minute % 15 == 0) & (idx.second == 0) & (idx.microsecond == 0)]
    else:
        anchors = pd.DatetimeIndex(anchors)
        if anchors.tz is None: raise ValueError("anchors must be timezone-aware UTC")
        anchors = anchors.tz_convert("UTC")
    if not anchors.is_monotonic_increasing or anchors.has_duplicates or not np.all(anchors.minute % 15 == 0):
        raise ValueError("anchors must be unique, ordered, and on the 15-minute UTC grid")
    b = bars.astype({c: "float64" for c in BAR_COLUMNS})
    close = b["close"]
    consecutive = idx.to_series().diff().eq(pd.Timedelta(minutes=1)).to_numpy()
    ret = np.log(close / close.shift(1)).where(consecutive & (close > 0) & (close.shift(1) > 0))
    cache: dict[tuple[str, int], pd.Series] = {}
    def roll(s: pd.Series, w: int, op: str = "sum") -> pd.Series:
        key = (f"{s.name}:{op}", w)
        if key not in cache: cache[key] = _roll(s, w, op)
        return cache[key]
    R = {w: roll(ret.rename("ret"), w) for w in (60, 240, 1440, 10080)}
    RV = {w: np.sqrt(roll(ret.pow(2).rename("ret2"), w)) for w in (15, 60, 240, 1440, 10080)}
    hmax = {w: roll(b["high"], w, "max") for w in (60, 240, 1440)}
    lmin = {w: roll(b["low"], w, "min") for w in (60, 240)}
    RG = {w: _safe_log_ratio(hmax[w], lmin[w]) for w in (60, 240)}
    loc_den = hmax[240] - lmin[240]
    loc = ((close - lmin[240]) / loc_den).where(loc_den != 0, 0.5)
    dd = _safe_log_ratio(close, hmax[1440])
    qsum = {w: roll(b["quote_asset_volume"], w) for w in (60, 240)}
    nsum = {w: roll(b["num_trades"], w) for w in (60, 240)}
    imb_num = (2 * b["taker_buy_base_volume"] - b["volume"]).rename("imb_num")
    imb = {}
    for w in (15, 60):
        den = roll(b["volume"], w)
        num = roll(imb_num, w)
        imb[w] = (num / den).where(den != 0, 0.0)
    illiq = np.log1p(1e12 * roll(ret.abs().rename("absret"), 60) / (1 + qsum[60]))
    end = anchors - pd.Timedelta(minutes=1)
    def at_anchor(s: pd.Series) -> np.ndarray:
        return s.reindex(end).to_numpy(dtype="float64")
    out: dict[str, np.ndarray] = {
        "P01_R60": at_anchor(R[60]), "P02_R240": at_anchor(R[240]), "P03_R1440": at_anchor(R[1440]),
        "P04_R10080": at_anchor(R[10080]), "P05_LOC240": at_anchor(loc), "P06_DD1440": at_anchor(dd),
        "V01_RV15": at_anchor(RV[15]), "V02_RV60": at_anchor(RV[60]), "V03_RV240": at_anchor(RV[240]),
        "V04_RV1440": at_anchor(RV[1440]), "V05_RG60": at_anchor(RG[60]), "V06_RG240": at_anchor(RG[240]),
        "V07_LOG_RV_1440_10080": at_anchor(np.log((RV[1440] + 1e-12) / (RV[10080] + 1e-12))),
        "A01_Q60": at_anchor(np.log1p(qsum[60])), "A02_Q240": at_anchor(np.log1p(qsum[240])),
        "A03_N60": at_anchor(np.log1p(nsum[60])), "A04_N240": at_anchor(np.log1p(nsum[240])),
        "A05_ILLIQ60": at_anchor(illiq), "T01_IMB15": at_anchor(imb[15]), "T02_IMB60": at_anchor(imb[60]),
        "T03_PRICE_FLOW60": at_anchor(R[60] * imb[60]),
    }
    rel = released.reindex(anchors)
    prior_close = pd.Series(at_anchor(close), index=anchors)
    def rcol(name: str) -> pd.Series: return pd.to_numeric(rel[name], errors="coerce").astype("float64")
    out.update({
        "D01_FUNDING": rcol("funding_last_funding_rate").to_numpy(),
        "D02_MARK_BASIS": _safe_log_ratio(rcol("mark_price_close"), prior_close).to_numpy(),
        "D03_MARK_RANGE": _safe_log_ratio(rcol("mark_price_high"), rcol("mark_price_low")).to_numpy(),
        "D04_MARK_BODY": _safe_log_ratio(rcol("mark_price_close"), rcol("mark_price_open")).to_numpy(),
        "D05_PREMIUM_CLOSE": rcol("premium_index_close").to_numpy(),
        "D06_PREMIUM_RANGE": (rcol("premium_index_high") - rcol("premium_index_low")).to_numpy(),
        "D07_PREMIUM_BODY": (rcol("premium_index_close") - rcol("premium_index_open")).to_numpy(),
    })
    for fid, source, logged in (
        ("D08_DVOL_LOG", "dvol_level", True), ("D09_DVOL_PREV_RET1H", "dvol_prev_ret1h", False),
        ("D10_DVOL_RET1H", "dvol_ret1h", False), ("D11_OI_LOG", "metrics_sum_open_interest", True),
        ("D12_OI_VALUE_LOG", "metrics_sum_open_interest_value", True)):
        value = rcol(source)
        out[fid] = np.where((value > 0) & np.isfinite(value), np.log(value), np.nan) if logged else value.to_numpy()
    x_sources = {
        "X01_CB_RET5": "coinbase_ret5", "X02_DER_RET5": "deribit_ret5",
        "X03_DER_CB_RET_DIFF": "cross_deribit_coinbase_ret_diff", "X04_CB_LOGVOL5": "coinbase_log_volume5",
        "X05_DER_LOGVOL5": "deribit_log_volume5", "X06_CB_PREV_RET5": "coinbase_prev_ret5",
        "X07_DER_PREV_RET5": "deribit_prev_ret5", "X08_DER_CB_PREV_RET_DIFF": "cross_deribit_coinbase_prev_ret_diff",
    }
    out.update({fid: rcol(source).to_numpy() for fid, source in x_sources.items()})
    minute = anchors.hour * 60 + anchors.minute
    dow = anchors.dayofweek
    out.update({"C01_SIN_MIN": np.sin(2*np.pi*minute/1440), "C02_COS_MIN": np.cos(2*np.pi*minute/1440),
                "C03_SIN_DOW": np.sin(2*np.pi*dow/7), "C04_COS_DOW": np.cos(2*np.pi*dow/7)})
    result = pd.DataFrame(out, index=anchors, dtype="float64")
    return result


def recipe_matrix(features: pd.DataFrame, registry: Mapping[str, object], family: str, lane: str) -> pd.DataFrame:
    if lane not in ("lane_a", "lane_b"): raise ValueError("lane must be lane_a or lane_b")
    columns = registry["target_family_recipes"][family][f"{lane}_columns"]
    missing = sorted(set(columns) - set(features.columns))
    if missing: raise ValueError(f"missing registered features: {missing}")
    return features.loc[:, columns].astype("float64")


def complete_mask(matrix: pd.DataFrame) -> pd.Series:
    return pd.Series(np.isfinite(matrix.to_numpy(dtype="float64")).all(axis=1), index=matrix.index, name="complete")


def build_feature_domain_masks(bars: pd.DataFrame, released: pd.DataFrame, *, anchors: pd.DatetimeIndex) -> pd.DataFrame:
    """Builder-derived required-input domain masks for predictor-only arithmetic diagnostics.

    True means every registered raw input is present and satisfies the formula domain.  It does not
    depend on the computed feature value, so a NaN/Inf result under True is detectable preflight.
    """
    idx=_utc_index(bars,"bars"); _utc_index(released,"released"); anchors=pd.DatetimeIndex(anchors).tz_convert("UTC")
    b=bars.astype({c:"float64" for c in BAR_COLUMNS}); consecutive=idx.to_series().diff().eq(pd.Timedelta(minutes=1))
    close_ok=np.isfinite(b.close)&(b.close>0); ret_ok=close_ok & close_ok.shift(1,fill_value=False) & consecutive
    def allw(valid,w): return _roll(valid.astype("float64").rename("valid"),w,"sum").eq(w)
    def at(s): return s.reindex(anchors-pd.Timedelta(minutes=1),fill_value=False).to_numpy(dtype=bool)
    r={w:allw(ret_ok,w) for w in (15,60,240,1440,10080)}
    hl={w:allw(np.isfinite(b.high)&(b.high>0)&np.isfinite(b.low)&(b.low>0),w) for w in (60,240,1440)}
    q={w:allw(np.isfinite(b.quote_asset_volume)&(b.quote_asset_volume>=0),w) for w in (60,240)}
    n={w:allw(np.isfinite(b.num_trades)&(b.num_trades>=0),w) for w in (60,240)}
    flow={w:allw(np.isfinite(b.volume)&(b.volume>=0)&np.isfinite(b.taker_buy_base_volume),w) for w in (15,60)}
    out={"P01_R60":at(r[60]),"P02_R240":at(r[240]),"P03_R1440":at(r[1440]),"P04_R10080":at(r[10080]),
         "P05_LOC240":at(hl[240]&close_ok),"P06_DD1440":at(hl[1440]&close_ok),
         "V01_RV15":at(r[15]),"V02_RV60":at(r[60]),"V03_RV240":at(r[240]),"V04_RV1440":at(r[1440]),
         "V05_RG60":at(hl[60]),"V06_RG240":at(hl[240]),"V07_LOG_RV_1440_10080":at(r[1440]&r[10080]),
         "A01_Q60":at(q[60]),"A02_Q240":at(q[240]),"A03_N60":at(n[60]),"A04_N240":at(n[240]),
         "A05_ILLIQ60":at(r[60]&q[60]),"T01_IMB15":at(flow[15]),"T02_IMB60":at(flow[60]),
         "T03_PRICE_FLOW60":at(r[60]&flow[60])}
    rel=released.reindex(anchors)
    def finite(name): return np.isfinite(pd.to_numeric(rel[name],errors="coerce").to_numpy(dtype="float64"))
    def positive(name):
        v=pd.to_numeric(rel[name],errors="coerce").to_numpy(dtype="float64"); return np.isfinite(v)&(v>0)
    prior=close_ok.reindex(anchors-pd.Timedelta(minutes=1),fill_value=False).to_numpy(dtype=bool)
    out.update({"D01_FUNDING":finite("funding_last_funding_rate"),
                "D02_MARK_BASIS":positive("mark_price_close")&prior,
                "D03_MARK_RANGE":positive("mark_price_high")&positive("mark_price_low"),
                "D04_MARK_BODY":positive("mark_price_close")&positive("mark_price_open"),
                "D05_PREMIUM_CLOSE":finite("premium_index_close"),
                "D06_PREMIUM_RANGE":finite("premium_index_high")&finite("premium_index_low"),
                "D07_PREMIUM_BODY":finite("premium_index_close")&finite("premium_index_open"),
                "D08_DVOL_LOG":positive("dvol_level"),"D09_DVOL_PREV_RET1H":finite("dvol_prev_ret1h"),
                "D10_DVOL_RET1H":finite("dvol_ret1h"),"D11_OI_LOG":positive("metrics_sum_open_interest"),
                "D12_OI_VALUE_LOG":positive("metrics_sum_open_interest_value")})
    for fid,source in {"X01_CB_RET5":"coinbase_ret5","X02_DER_RET5":"deribit_ret5","X03_DER_CB_RET_DIFF":"cross_deribit_coinbase_ret_diff",
                       "X04_CB_LOGVOL5":"coinbase_log_volume5","X05_DER_LOGVOL5":"deribit_log_volume5","X06_CB_PREV_RET5":"coinbase_prev_ret5",
                       "X07_DER_PREV_RET5":"deribit_prev_ret5","X08_DER_CB_PREV_RET_DIFF":"cross_deribit_coinbase_prev_ret_diff"}.items(): out[fid]=finite(source)
    for c in ("C01_SIN_MIN","C02_COS_MIN","C03_SIN_DOW","C04_COS_DOW"): out[c]=np.ones(len(anchors),dtype=bool)
    return pd.DataFrame(out,index=anchors,dtype=bool)
