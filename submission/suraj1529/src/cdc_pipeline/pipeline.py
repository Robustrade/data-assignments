import json
from pathlib import Path
from .cdc import build_demo_events
from .lake import LakeWriter
from .schema_guard import SchemaGuard
from .warehouse import Warehouse
from .config import CHECKPOINT_FILE, LAKE_FILE, WAREHOUSE_DB

class Pipeline:
    def __init__(self, contract):
        self.contract = contract

    def run(self, events, observed_schema):
        SchemaGuard(self.contract).validate(observed_schema)

        checkpoint = 0
        if CHECKPOINT_FILE.exists():
            checkpoint = json.loads(CHECKPOINT_FILE.read_text())["source_lsn"]

        # Lake-first ordering makes every accepted event durable before warehouse apply.
        events = [e for e in events if e["source_lsn"] > checkpoint]
        events.sort(key=lambda e: e["source_lsn"])

        LakeWriter(LAKE_FILE).append(events)

        wh = Warehouse(WAREHOUSE_DB)
        applied = []
        max_lsn = checkpoint
        try:
            for event in events:
                result = wh.apply(event)
                if result in ("applied", "duplicate", "stale"):
                    max_lsn = max(max_lsn, event["source_lsn"])
                    applied.append((event["source_lsn"], result))
            wh.con.commit()
        finally:
            wh.close()

        CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
        CHECKPOINT_FILE.write_text(json.dumps({"source_lsn": max_lsn}, indent=2))
        return applied
