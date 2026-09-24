"""Run with python -m cex_data_feed.rv_live --help. Public data and signals only."""
import argparse
import json
import sys
import time
from pathlib import Path
from .adapters import now_ms
from .collect import poll, repair
from .contract import Observation, SPECS, MINUTE, canonical
from .store import Store


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    for name in ("collect", "repair", "import-jsonl", "evaluate", "watch", "export", "reconcile"):
        c = sub.add_parser(name)
        c.add_argument("--db", type=Path, required=True, help="New rv_live SQLite database; keep separate from legacy DB")
        if name in ("collect", "repair", "watch", "reconcile"):
            c.add_argument("--start-ms", type=int, required=True)
        if name in ("collect", "repair", "reconcile"):
            c.add_argument("--end-ms", type=int, required=True)
        if name in ("repair", "reconcile"):
            c.add_argument("--source", choices=SPECS, required=True)
        if name in ("collect", "repair", "watch"):
            c.add_argument("--max-pages", type=int, default=4)
        if name == "import-jsonl":
            c.add_argument("--input", type=Path, required=True)
        if name in ("evaluate", "watch", "export"):
            c.add_argument("--run", required=True)
        if name in ("evaluate", "watch"):
            c.add_argument("--bundle", type=Path, required=True)
        if name == "evaluate":
            c.add_argument("--minute-ms", type=int, required=True)
            c.add_argument("--received-by-ms", type=int, required=True)
            c.add_argument("--replay", action="store_true")
        if name == "reconcile":
            c.add_argument("--first-asof-ms", type=int, required=True)
            c.add_argument("--final-asof-ms", type=int, required=True)
    c = sub.add_parser("compare")
    c.add_argument("--expected", type=Path, required=True)
    c.add_argument("--actual", type=Path, required=True)
    return p


def evaluate_new_minute(engine, t, emitted_ms):
    """A restarted watcher must not publish an already committed fire again."""
    if engine.store.decision(engine.run, t) is not None:
        return None
    return engine.evaluate(t, emitted_ms)


def main(argv=None):
    args = parser().parse_args(argv)
    if args.command == "compare":
        from .validate import compare_decisions
        read = lambda path: [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        result = compare_decisions(read(args.expected), read(args.actual))
        print(canonical(result))
        return 0 if result["status"] == "PASS" else 1
    store = Store(args.db)
    try:
        if args.command == "repair":
            result = repair(store, args.source, args.start_ms, args.end_ms, max_pages=args.max_pages)
            print(canonical(result))
            return 0 if result["complete"] else 2
        if args.command == "collect":
            result = poll(store, args.start_ms, args.end_ms, max_pages=args.max_pages)
            print(canonical(result))
            return 0 if result and all(r["complete"] for r in result) else 2
        if args.command == "import-jsonl":
            count = 0
            with args.input.open() as f:
                batch = []
                for line in f:
                    if line.strip():
                        batch.append(Observation(**json.loads(line)))
                    if len(batch) == 1000:
                        count += store.ingest(batch)["inserted"]
                        batch = []
                count += store.ingest(batch)["inserted"]
            print(canonical({"inserted": count}))
            return 0
        if args.command == "export":
            for row in store.db.execute("SELECT payload FROM decisions WHERE run=? ORDER BY t", (args.run,)):
                print(row[0])
            return 0
        if args.command == "reconcile":
            from .validate import reconcile
            print(canonical(reconcile(store, args.source, args.start_ms, args.end_ms,
                                      args.first_asof_ms, args.final_asof_ms)))
            return 0
        from .signals import Bundle, Engine
        bundle = Bundle(args.bundle)
        engine = Engine(store, bundle, args.run)
        if args.command == "evaluate":
            if not args.replay and abs(now_ms() - args.received_by_ms) > 5000:
                raise ValueError("Historical evaluation requires --replay")
            result = engine.evaluate(args.minute_ms, args.received_by_ms, replay=args.replay)
            print(canonical(result))
            return 0 if result["status"] == "ok" else 2
        # Polling shadow runner. Intentionally no autostart installation, order route or notifications.
        while True:
            t = now_ms() // MINUTE * MINUTE
            if now_ms() < t + 5000:
                time.sleep((t + 5000 - now_ms()) / 1000)
            stats = poll(store, args.start_ms, t, max_pages=args.max_pages)
            print(canonical({"collection": stats}), file=sys.stderr, flush=True)
            # Permit operator replacement of a validated manifest between decisions.
            engine.bundle = Bundle(args.bundle)
            result = evaluate_new_minute(engine, t, now_ms())
            if result is not None:
                print(canonical(result), flush=True)
            time.sleep(max(0.1, (t + MINUTE + 5000 - now_ms()) / 1000))
    finally:
        store.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
