"""Strict same-input signal parity and separate observation reconciliation."""
import numpy as np


def compare_decisions(expected, actual):
    def keyed(rows):
        result = {}
        for row in rows:
            if row["t"] in result:
                raise ValueError("Duplicate decision minute")
            result[row["t"]] = row
        return result
    e, a = keyed(expected), keyed(actual)
    errors = []
    maxima = {}
    for t in sorted(e.keys() | a.keys()):
        if t not in e or t not in a:
            errors.append({"t": t, "field": "timestamp", "reason": "missing_or_extra_decision"})
            continue
        for key in ("status", "fires", "score_t", "report_eligible", "reasons"):
            if e[t].get(key) != a[t].get(key):
                errors.append({"t": t, "field": key, "reason": "categorical_mismatch"})
        for key in ("features", "score", "ema_dist", "ema"):
            if key not in e[t] and key not in a[t]:
                continue
            if key not in e[t] or key not in a[t]:
                errors.append({"t": t, "field": key, "reason": "missing_value"})
                continue
            x, y = np.asarray(e[t][key], dtype=float), np.asarray(a[t][key], dtype=float)
            tolerance = 1e-12 if key == "score" else 1e-10
            if x.shape != y.shape or not np.isfinite(x).all() or not np.isfinite(y).all():
                errors.append({"t": t, "field": key, "reason": "invalid_numeric"})
                continue
            maxima[key] = max(maxima.get(key, 0.), float(np.max(abs(x-y))))
            if not np.allclose(x, y, rtol=tolerance, atol=tolerance):
                errors.append({"t": t, "field": key, "reason": "numerical_mismatch"})
        if e[t].get("thresholds") != a[t].get("thresholds"):
            errors.append({"t": t, "field": "thresholds", "reason": "artifact_mismatch"})
    return {"status": "PASS" if not errors and e else "FAIL", "expected_minutes": len(e),
            "actual_minutes": len(a), "mismatch_count": len(errors), "examples": errors[:100],
            "max_absolute_error": maxima, "categorical_tolerance": 0,
            "numeric_tolerances": {"features_ema_rtol_atol": 1e-10, "score_rtol_atol": 1e-12}}


def reconcile(store, source, start, end, first_asof, final_asof):
    if final_asof < first_asof:
        raise ValueError("Final snapshot must follow first snapshot")
    a = {r.t: r for r in store.rows(source, start, end, first_asof)}
    b = {r.t: r for r in store.rows(source, start, end, final_asof)}
    revised = [t for t in a.keys() & b.keys() if a[t].fingerprint != b[t].fingerprint]
    return {"source": source, "first_asof": first_asof, "final_asof": final_asof,
            "first_rows": len(a), "final_rows": len(b), "late_added": sorted(b.keys()-a.keys()),
            "revised": sorted(revised), "removed": sorted(a.keys()-b.keys())}
