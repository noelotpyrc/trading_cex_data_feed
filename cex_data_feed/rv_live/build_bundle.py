"""Export existing research artifacts, without fitting/calibrating or reading outcomes."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path
import numpy as np
import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def export(root, output, bootstrap_through_ms):
    root, output = Path(root).resolve(), Path(output).resolve()
    if output.exists():
        raise ValueError("Output must be a new directory")
    source = root / "artifacts/rv_ema_holdout/reh001"
    read = lambda p: json.loads(p.read_text())
    quarter_rows = read(source / "regime_calibrations.json")
    q75 = {r["quarter"]: r for r in read(root / "artifacts/rv_ema_full_period/ref002/q75_calibrations.json")}
    months = read(source / "ema_calibrations.json")
    with np.load(source / "market.npz") as m:
        t, close = m["t"], m["close"]
    i = int(np.searchsorted(t * 60_000, bootstrap_through_ms))
    if i == len(t) or t[i] * 60_000 != bootstrap_through_ms:
        raise ValueError("Bootstrap minute absent")
    if np.any(np.diff(t[:i+1]) != 1) or str(np.datetime64(int(t[0]), "m")) != "2020-01-01T00:00":
        raise ValueError("EMA seed history must be contiguous from 2020-01-01")
    ema = float(pd.Series(close[:i+1]).ewm(span=1440, adjust=False).mean().iloc[-1])
    contract = read(Path(__file__).with_name("feature_contract.json"))
    result = dict(schema_version=1, columns=contract["columns"],
                  bootstrap={"last_bar_ms": int(bootstrap_through_ms), "ema": ema,
                             "close_history_sha256": hashlib.sha256(close[:i+1].tobytes()).hexdigest()},
                  quarters=[], months=[], source_hashes={})
    for row in quarter_rows:
        quarter = row["quarter"]
        model = source / "models" / f"{quarter}_outer.joblib"
        if sha(model) != row["model"]["model_sha256"] or row["model"]["last_label_maturity"] >= row["start"]:
            raise ValueError("Model identity/training maturity mismatch")
        qrow = q75[quarter]
        if qrow["q90"] != row["q90"] or qrow["last_label_maturity"] >= row["start"]:
            raise ValueError("Calibration identity/maturity mismatch")
        result["quarters"].append(dict(start_ms=row["start"]*60_000, end_ms=row["quarter_end"]*60_000,
            calibration_end_ms=row["start"]*60_000, q75=qrow["q75"], q90=row["q90"],
            model_file=model.name, model_sha256=sha(model)))
    for row in months:
        start = pd.Timestamp(row["month"], tz="UTC")
        if row["status"] != "OK" or row["quantiles"] != [.1,.95] or row["calibration_end_exclusive"]*60_000 != int(start.timestamp()*1000):
            raise ValueError("Invalid EMA calibration")
        result["months"].append(dict(start_ms=int(start.timestamp()*1000),
            end_ms=int((start+pd.offsets.MonthBegin(1)).timestamp()*1000),
            calibration_end_ms=row["calibration_end_exclusive"]*60_000, q10=row["cuts"][0],q95=row["cuts"][1]))
    for p in [source/"regime_calibrations.json",source/"ema_calibrations.json",source/"market.npz",
              root/"artifacts/rv_ema_full_period/ref002/q75_calibrations.json"]:
        result["source_hashes"][str(p.relative_to(root))] = sha(p)
    output.mkdir(parents=True)
    for row in result["quarters"]:
        shutil.copyfile(source/"models"/row["model_file"], output/row["model_file"])
    (output/"manifest.json").write_text(json.dumps(result,indent=2,allow_nan=False)+"\n")
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--research-root",type=Path,required=True)
    p.add_argument("--output",type=Path,required=True)
    p.add_argument("--bootstrap-through-ms",type=int,required=True)
    a=p.parse_args()
    result=export(a.research_root,a.output,a.bootstrap_through_ms)
    print(json.dumps({"manifest":str(a.output/"manifest.json"),"quarters":len(result["quarters"]),"months":len(result["months"])}))


if __name__=="__main__":
    main()
