"""Bounded paginated repair and ongoing collection with explicit completeness reports."""
from .adapters import Adapter, now_ms
from .contract import SPECS, MINUTE


def repair(store, source, start, end, *, adapter=None, max_pages=100):
    interval = SPECS[source][0]
    if interval and (start % interval or end % interval):
        raise ValueError("Repair bounds must align with source interval")
    if end <= start or max_pages < 1:
        raise ValueError("Invalid repair range/budget")
    adapter = adapter or Adapter(source)
    cursor, pages, inserted, revisions = start, 0, 0, 0
    errors = []
    retry_at_ms = None
    while cursor < end and pages < max_pages:
        stop = min(end, cursor + (200 * interval if interval else 7 * 1440 * MINUTE))
        try:
            rows, raw, received, url = adapter.fetch(cursor, stop)
            stat = store.ingest(rows, source=source, request={"url": url}, raw=raw, received_ms=received)
            inserted += stat["inserted"]
            revisions += stat["revisions"]
        except Exception as error:
            store.failure(source, now_ms(), {"start": cursor, "end": stop}, str(error))
            errors.append(str(error))
            retry_at_ms = getattr(error, "retry_at_ms", None)
            break
        pages += 1
        cursor = stop
    existing = store.rows(source, start, cursor, now_ms())
    times = {r.t for r in existing}
    missing = [t for t in range(start, cursor, interval) if t not in times] if interval else []
    return dict(source=source, start=start, end=end, fetched_through=cursor,
                pages=pages, inserted=inserted, revisions=revisions, missing_count=len(missing),
                first_missing=missing[0] if missing else None,
                complete=cursor == end and not errors and not missing,
                errors=errors, retry_at_ms=retry_at_ms)


def poll(store, start, end, *, max_pages=4):
    """Cursor stops at first missing bar. Revisit a tail to discover revisions/late funding."""
    store.db.execute("CREATE TABLE IF NOT EXISTS cursors(source TEXT PRIMARY KEY,t INTEGER NOT NULL)")
    store.db.execute("CREATE TABLE IF NOT EXISTS cooldowns(source TEXT PRIMARY KEY,t INTEGER NOT NULL)")
    store.db.commit()
    results = []
    for source, (interval, _) in SPECS.items():
        cooldown = store.db.execute("SELECT t FROM cooldowns WHERE source=?", (source,)).fetchone()
        if cooldown and now_ms() < cooldown[0]:
            results.append(dict(source=source, complete=False, errors=["source_in_backoff"], retry_at_ms=cooldown[0]))
            continue
        width = interval or 15 * MINUTE
        stop = end // width * width
        row = store.db.execute("SELECT t FROM cursors WHERE source=?", (source,)).fetchone()
        if source in ("coinbase", "deribit", "funding") and row and row[0] >= stop:
            results.append(dict(source=source, complete=True, status="no_new_interval"))
            continue
        begin = max(start // width * width, (row[0] - (2 * interval if interval else 480 * MINUTE)) if row else start // width * width)
        if begin >= stop:
            continue
        result = repair(store, source, begin, stop, max_pages=max_pages)
        if result["retry_at_ms"] is not None:
            with store.db:
                store.db.execute("INSERT INTO cooldowns VALUES(?,?) ON CONFLICT(source) DO UPDATE SET t=excluded.t", (source,result["retry_at_ms"]))
        # A persistent old gap must not prevent acquisition of current observations.
        # Preserve the repair cursor at the gap, and independently collect a small live tail.
        if not result["errors"] and (result["fetched_through"] < stop or result["first_missing"] is not None):
            tail_start = max(begin, stop - (10 * interval if interval else 480 * MINUTE))
            result["recent_tail"] = repair(store, source, tail_start, stop, max_pages=1)
        if not result["errors"]:
            next_t = result["first_missing"] if result["first_missing"] is not None else result["fetched_through"]
            with store.db:
                store.db.execute("INSERT INTO cursors VALUES(?,?) ON CONFLICT(source) DO UPDATE SET t=excluded.t", (source, next_t))
        results.append(result)
    return results
