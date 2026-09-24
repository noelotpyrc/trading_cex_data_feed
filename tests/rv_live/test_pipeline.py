import json
import sqlite3
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
import numpy as np
import pandas as pd
import pytest

from cex_data_feed.rv_live import signals
from cex_data_feed.rv_live.adapters import Adapter, parse, request_url, get_json, DeferredRequest
from cex_data_feed.rv_live.collect import repair
from cex_data_feed.rv_live.contract import Observation, MINUTE, release_ms
from cex_data_feed.rv_live.store import Store
from cex_data_feed.rv_live.validate import compare_decisions, reconcile

T = int(pd.Timestamp("2024-02-01", tz="UTC").timestamp() * 1000)
FIELDS = dict(open=100., high=102., low=99., close=101., volume=10., quote_asset_volume=1000.,
              num_trades=20, taker_buy_base_volume=4., taker_buy_quote_volume=400.)


@pytest.fixture(autouse=True)
def no_network():
    with patch("socket.socket.connect", side_effect=AssertionError("Tests must stay offline")):
        yield


@pytest.fixture
def store(tmp_path):
    result = Store(tmp_path / "live.sqlite")
    yield result
    result.close()


def bar(t, received=None, **changes):
    return Observation("binance", t, FIELDS | changes, received or t + MINUTE)


def aux(t):
    rows = []
    for source in ("mark", "premium"):
        v = dict(open=100., high=102., low=99., close=101.) if source == "mark" else dict(open=-.001, high=.001, low=-.002, close=0.)
        s = t - 2 * MINUTE
        rows.append(Observation(source, s, v, s+MINUTE))
    rows.append(Observation("funding", t-60*MINUTE, {"rate": .0001}, t-59*MINUTE))
    for source in ("coinbase", "deribit"):
        for offset in (15, 10):
            s = t-offset*MINUTE
            rows.append(Observation(source, s, dict(open=100.,high=102.,low=99.,close=101.,volume=5.), s+5*MINUTE))
    return rows


class FakeBundle:
    def __init__(self):
        cols = json.loads(Path(signals.__file__).with_name("feature_contract.json").read_text())["columns"]
        self.data = {"columns": cols, "bootstrap": {"last_bar_ms": T-10082*MINUTE, "ema":100.}}
        self.quarter = dict(start_ms=T-31*1440*MINUTE, end_ms=T+60*1440*MINUTE, q75=1.,q90=1.5,model_sha256="synthetic")
        self.month = dict(q10=-.1,q95=.1)

    def active(self,key,t):
        return self.quarter if key == "quarters" else self.month

    def predict(self, quarter, x):
        return 2.


@pytest.fixture
def warmed(store):
    rows = []
    for i,t in enumerate(range(T-10081*MINUTE,T+31*MINUTE,MINUTE)):
        c = 100 + .1*np.sin(i/15)
        rows.append(bar(t, open=float(c),close=float(c)))
    store.ingest(rows + aux(T) + aux(T+15*MINUTE) + aux(T+30*MINUTE))
    return store, FakeBundle()


@pytest.mark.parametrize("changes", [dict(close=float("nan")),dict(num_trades=2.5),dict(volume=-1.),dict(taker_buy_base_volume=100.),dict(high=99.)])
def test_reject_bad_bars(changes):
    with pytest.raises(ValueError):
        bar(T,**changes).validate()


def test_no_legacy_mutation(tmp_path):
    p=tmp_path/"legacy.sqlite"
    with sqlite3.connect(p) as con:
        con.execute("CREATE TABLE old(x)")
    with pytest.raises(ValueError,match="legacy"):
        Store(p)
    with sqlite3.connect(p) as con:
        assert con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()==[("old",)]


def test_revision_asof_and_revert(store):
    original=bar(T,T+MINUTE)
    store.ingest([original,original])
    store.ingest([bar(T,T+2*MINUTE,close=100.5)])
    store.ingest([bar(T,T+3*MINUTE)])
    assert store.rows("binance",T,T+MINUTE,T+MINUTE)[0].values["close"]==101.
    assert store.rows("binance",T,T+MINUTE,T+2*MINUTE)[0].values["close"]==100.5
    assert store.rows("binance",T,T+MINUTE,T+3*MINUTE)[0].values["close"]==101.
    assert reconcile(store,"binance",T,T+MINUTE,T+MINUTE,T+2*MINUTE)["revised"]==[T]


def test_out_of_order_receipt(store):
    store.ingest([bar(T,T+3*MINUTE),bar(T,T+MINUTE)])
    assert len(store.rows("binance",T,T+MINUTE,T+MINUTE))==1


@pytest.mark.parametrize("source", ["binance","mark","premium"])
def test_binance_adapter_closed_only(source):
    r=[T,"100","102","99","101","10",T+MINUTE-1,"1000",20,"4","400",0]
    forming=[T+MINUTE,*r[1:6],T+2*MINUTE-1,*r[7:]]
    result=parse(source,[forming,r],T+MINUTE+5,T,T+2*MINUTE)
    assert len(result)==1 and result[0].t==T


def test_venue_parsers():
    cb=parse("coinbase",[[T//1000,99,102,100,101,5]],T+5*MINUTE,T,T+5*MINUTE)[0]
    der=parse("deribit",{"result":{"status":"ok","ticks":[T],"open":[100],"high":[102],"low":[99],"close":[101],"volume":[5],"cost":[500]}},T+5*MINUTE,T,T+5*MINUTE)[0]
    assert cb.values==der.values
    f=parse("funding",[{"fundingTime":T,"fundingRate":"0.0001"}],T+MINUTE,T,T+MINUTE)[0]
    assert f.values=={"rate":.0001}
    assert "granularity=300" in request_url("coinbase",T,T+MINUTE)


def test_repair_pages_and_missing(store):
    class Stub:
        def fetch(self,start,end):
            return [bar(t,T+500*MINUTE) for t in range(start,end,MINUTE) if t!=T+3*MINUTE],[],T+500*MINUTE,"fixture"
    result=repair(store,"binance",T,T+201*MINUTE,adapter=Stub())
    assert result["pages"]==2 and result["first_missing"]==T+3*MINUTE and not result["complete"]


def test_budget_is_incomplete(store):
    class Stub:
        def fetch(self,start,end):
            return [bar(t,T+500*MINUTE) for t in range(start,end,MINUTE)],[],T+500*MINUTE,"fixture"
    result=repair(store,"binance",T,T+201*MINUTE,adapter=Stub(),max_pages=1)
    assert not result["complete"] and result["fetched_through"]==T+200*MINUTE


def test_release_and_expiry(store):
    store.ingest(aux(T))
    released,_=signals.released_inputs(store,T,T)
    assert released.iloc[0].funding_last_funding_rate==float(np.float32(.0001))
    with pytest.raises(signals.Unavailable,match="mark"):
        signals.released_inputs(store,T+MINUTE,T+MINUTE)


def test_late_receipt(store):
    rows=aux(T)
    rows[0]=Observation(rows[0].source,rows[0].t,rows[0].values,T+31_000)
    store.ingest(rows)
    with pytest.raises(signals.Unavailable,match="mark"):
        signals.released_inputs(store,T,T+30_000)


def test_restart_and_duplicate(warmed):
    store,bundle=warmed
    first=signals.Engine(store,bundle).evaluate(T,T)
    assert first["status"]=="ok" and first["fires"]["q90"]["regime_high"]
    assert signals.Engine(store,bundle).evaluate(T,T+1000)==first
    second=signals.Engine(store,bundle).evaluate(T+MINUTE,T+MINUTE)
    assert second["status"]=="ok" and second["score_t"]==T
    assert store.db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]==2


def test_stale_rv_not_carried(warmed):
    store,bundle=warmed
    engine=signals.Engine(store,bundle)
    assert engine.evaluate(T,T)["status"]=="ok"
    result=engine.evaluate(T+16*MINUTE,T+16*MINUTE)
    assert result["reasons"]==["missing_current_rv_score"]


def test_revised_history_requires_new_bootstrap(warmed):
    store,bundle=warmed
    engine=signals.Engine(store,bundle)
    engine.evaluate(T,T)
    store.ingest([bar(T-MINUTE,T+MINUTE,close=100.9)])
    result=engine.evaluate(T+MINUTE,T+MINUTE)
    assert result["reasons"]==["history_revision_requires_rebootstrap"]


def test_deadline_expired_artifact_and_future_mutation(warmed):
    store,bundle=warmed
    result=signals.Engine(store,bundle,"late").evaluate(T,T+31_000)
    assert result["reasons"]==["decision_deadline_missed"]
    with patch.object(bundle,"active",side_effect=signals.Unavailable("missing_or_expired:months")):
        result=signals.Engine(store,bundle,"expired").evaluate(T,T)
    assert result["status"]=="unavailable"
    before=signals.Engine(store,bundle,"before").evaluate(T,T)
    store.ingest([bar(T+MINUTE,T+2*MINUTE,close=101.9)])
    after=signals.Engine(store,bundle,"after").evaluate(T,T)
    assert compare_decisions([before],[after])["status"]=="PASS"


def test_strict_compare():
    a={"t":T,"status":"ok","fires":{"q90":True},"score":1.,"ema_dist":.2}
    assert compare_decisions([a],[a])["status"]=="PASS"
    assert compare_decisions([a],[a|{"fires":{"q90":False}}])["status"]=="FAIL"
    assert compare_decisions([a],[])["status"]=="FAIL"
    with pytest.raises(ValueError):
        compare_decisions([a,a],[a])


def test_rate_limit_honors_long_backoff():
    def opener(*args,**kwargs):
        raise HTTPError('https://fixture',429,'limited',{'Retry-After':'120'},None)
    with pytest.raises(DeferredRequest) as caught:
        get_json('https://fixture',opener=opener,sleeper=lambda _: pytest.fail('Should defer long backoff'))
    assert caught.value.retry_at_ms > 0


def test_concurrent_checkpoint_rejected(store):
    store.commit_decision('x',T,{'t':T},{'last_decision_ms':T})
    with pytest.raises(RuntimeError,match='Concurrent'):
        store.commit_decision('x',T+MINUTE,{'t':T+MINUTE},{'last_decision_ms':T+MINUTE})
    assert store.checkpoint('x')['last_decision_ms']==T


def test_threshold_ties(warmed):
    store,bundle=warmed
    original=signals.Engine(store,bundle,'original').evaluate(T,T)
    bundle.month={'q10':original['ema_dist'],'q95':original['ema_dist']}
    bundle.quarter['q90']=2.
    result=signals.Engine(store,bundle,'ties').evaluate(T,T)
    assert result['fires']['q75']['low10_long']
    assert not result['fires']['q75']['top5_short']
    assert not result['fires']['q90']['regime_high']
    bundle.quarter['q90']=1.5
    bundle.month={'q10':original['ema_dist']-1,'q95':original['ema_dist']-1}
    result=signals.Engine(store,bundle,'upper').evaluate(T,T)
    assert result['fires']['q90']['top5_short'] and not result['fires']['q90']['low10_long']


def test_failed_boundary_cannot_reuse_previous_score(warmed):
    store,bundle=warmed
    engine=signals.Engine(store,bundle)
    engine.evaluate(T,T)
    with patch.object(signals,'released_inputs',side_effect=signals.Unavailable('missing_or_stale:coinbase')):
        assert engine.evaluate(T+15*MINUTE,T+15*MINUTE)['status']=='unavailable'
    assert engine.evaluate(T+16*MINUTE,T+16*MINUTE)['reasons']==['missing_current_rv_score']


def test_poll_keeps_acquiring_past_old_gap(store):
    from cex_data_feed.rv_live.collect import poll
    calls=[]
    def fake_repair(store,source,start,end,**kwargs):
        calls.append((source,start,end))
        return dict(source=source,start=start,end=end,errors=[],retry_at_ms=None,
                    first_missing=start,fetched_through=min(end,start+200*MINUTE),complete=False)
    with patch('cex_data_feed.rv_live.collect.repair',side_effect=fake_repair):
        poll(store,T,T+2000*MINUTE)
    assert ('binance',T+1990*MINUTE,T+2000*MINUTE) in calls


def test_bundle_identity_and_expiry(tmp_path):
    fake=FakeBundle()
    data=dict(schema_version=1,columns=fake.data['columns'],bootstrap=fake.data['bootstrap'],
              quarters=[fake.quarter|{'calibration_end_ms':fake.quarter['start_ms']}],
              months=[dict(start_ms=T,end_ms=T+1440*MINUTE,calibration_end_ms=T,q10=-.1,q95=.1)])
    p=tmp_path/'manifest.json';p.write_text(json.dumps(data))
    bundle=signals.Bundle(p)
    assert bundle.active('months',T)['q10']==-.1
    with pytest.raises(signals.Unavailable):bundle.active('months',T+1440*MINUTE)
    with pytest.raises(signals.Unavailable):bundle.active('quarters',fake.quarter['end_ms'])
    data['columns']=list(reversed(data['columns']));p.write_text(json.dumps(data))
    with pytest.raises(ValueError,match='order'):signals.Bundle(p)


def test_restart_watcher_does_not_republish_committed_fire(warmed):
    from cex_data_feed.rv_live.__main__ import evaluate_new_minute
    store,bundle=warmed
    first=evaluate_new_minute(signals.Engine(store,bundle),T,T)
    assert first['status']=='ok'
    assert evaluate_new_minute(signals.Engine(store,bundle),T,T+1000) is None
    assert evaluate_new_minute(signals.Engine(store,bundle),T+MINUTE,T+MINUTE)['status']=='ok'


def test_activity_revision_preserves_ema_and_updates_next_rv(warmed):
    store,bundle=warmed
    engine=signals.Engine(store,bundle)
    first=engine.evaluate(T,T)
    revised_t=T-MINUTE
    old=store.rows('binance',revised_t,T,T)[0]
    changed=Observation('binance',revised_t,old.values | {'volume':50.,'quote_asset_volume':5000.,'num_trades':100},T+MINUTE)
    store.ingest([changed])
    next_minute=engine.evaluate(T+MINUTE,T+MINUTE)
    assert next_minute['status']=='ok' and next_minute['score_t']==T
    # A fresh independent calculation from the same exact seed must match EMA.
    reference=signals.Engine(store,bundle,'reference').evaluate(T,T+MINUTE,replay=True)
    alpha=2/1441
    close=store.rows('binance',T,T+MINUTE,T+MINUTE)[0].values['close']
    assert next_minute['ema']==alpha*close+(1-alpha)*reference['ema']
    assert store.decision('live',T)==first
    current=engine.evaluate(T+15*MINUTE,T+15*MINUTE)
    assert current['status']=='ok'
    # Re-evaluate with the original activity under a separate store snapshot.
    store.ingest([Observation('binance',revised_t,old.values,T+15*MINUTE+1)])
    original_activity=signals.Engine(store,bundle,'unrevised').evaluate(T+15*MINUTE,T+15*MINUTE+1)
    cols=bundle.data['columns']
    assert current['features'][cols.index('A01_Q60')] != original_activity['features'][cols.index('A01_Q60')]
    # First-seen replay stays causal even though the revision is now in the DB.
    replay=signals.Engine(store,bundle,'as-observed').evaluate(T,T)
    assert compare_decisions([first],[replay])['status']=='PASS'
