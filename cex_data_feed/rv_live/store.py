"""Append-only source versions; immutable decisions and transactional checkpoints."""
import json
import sqlite3
from pathlib import Path
from .contract import Observation, canonical


class Store:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.row_factory = sqlite3.Row
        tables = {r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables and "observations" not in tables:
            self.db.close()
            raise ValueError("Use a new rv_live database; refusing to alter a legacy database")
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS observations(
              id INTEGER PRIMARY KEY, source TEXT NOT NULL, t INTEGER NOT NULL,
              received_ms INTEGER NOT NULL, fingerprint TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS obs_lookup ON observations(source,t,received_ms,id);
            CREATE TABLE IF NOT EXISTS batches(
              id INTEGER PRIMARY KEY, source TEXT, received_ms INTEGER, request TEXT,
              raw_json TEXT, status TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS decisions(
              run TEXT NOT NULL, t INTEGER NOT NULL, payload TEXT NOT NULL,
              PRIMARY KEY(run,t));
            CREATE TABLE IF NOT EXISTS checkpoints(run TEXT PRIMARY KEY, payload TEXT NOT NULL);
        ''')

    def close(self):
        self.db.close()

    def ingest(self, observations, *, source=None, request=None, raw=None, received_ms=None):
        rows = list(observations)
        for row in rows:
            row.validate()
        inserted = revised = 0
        with self.db:
            if source is not None:
                self.db.execute("INSERT INTO batches(source,received_ms,request,raw_json,status) VALUES(?,?,?,?,?)",
                                (source, received_ms, canonical(request), canonical(raw), "OK"))
            for row in rows:
                old = self.db.execute("SELECT fingerprint,received_ms FROM observations WHERE source=? AND t=? ORDER BY received_ms DESC,id DESC LIMIT 1",
                                      (row.source, row.t)).fetchone()
                # Log every response in batches; identical normalized duplicates need no new version.
                if old and old[0] == row.fingerprint and row.received_ms >= old[1]:
                    continue
                self.db.execute("INSERT INTO observations(source,t,received_ms,fingerprint,payload) VALUES(?,?,?,?,?)",
                                (row.source, row.t, row.received_ms, row.fingerprint, canonical(row.values)))
                inserted += 1
                revised += bool(old)
        return {"inserted": inserted, "revisions": revised}

    def failure(self, source, now, request, reason, raw=None):
        with self.db:
            self.db.execute("INSERT INTO batches(source,received_ms,request,raw_json,status) VALUES(?,?,?,?,?)",
                            (source, now, canonical(request), canonical(raw), str(reason)))

    def rows(self, source, start, end, asof):
        rows = self.db.execute('''
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY t ORDER BY received_ms DESC,id DESC) AS n
            FROM observations WHERE source=? AND t>=? AND t<? AND received_ms<=?
          ) WHERE n=1 ORDER BY t
        ''', (source, int(start), int(end), int(asof))).fetchall()
        return [Observation(r["source"], r["t"], json.loads(r["payload"]), r["received_ms"]) for r in rows]

    def decision(self, run, t):
        row = self.db.execute("SELECT payload FROM decisions WHERE run=? AND t=?", (run, t)).fetchone()
        return json.loads(row[0]) if row else None

    def checkpoint(self, run):
        row = self.db.execute("SELECT payload FROM checkpoints WHERE run=?", (run,)).fetchone()
        return json.loads(row[0]) if row else None

    def commit_decision(self, run, t, decision, state, *, expected_previous=-1):
        # First decision wins, including unavailable decisions; replays use a different run.
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            existing = self.decision(run, t)
            if existing is not None:
                return existing
            checkpoint = self.checkpoint(run)
            previous = checkpoint.get("last_decision_ms", -1) if checkpoint else -1
            if previous != expected_previous:
                raise RuntimeError("Concurrent evaluator changed checkpoint; retry from fresh state")
            self.db.execute("INSERT INTO decisions VALUES(?,?,?)", (run, t, canonical(decision)))
            self.db.execute("INSERT INTO checkpoints VALUES(?,?) ON CONFLICT(run) DO UPDATE SET payload=excluded.payload",
                            (run, canonical(state)))
        return decision
