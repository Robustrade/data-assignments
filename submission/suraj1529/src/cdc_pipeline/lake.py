import json

class LakeWriter:
    def __init__(self, path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, events):
        existing = set()
        if self.path.exists():
            with self.path.open() as f:
                for line in f:
                    if line.strip():
                        existing.add(json.loads(line)["event_id"])
        with self.path.open("a") as f:
            for event in events:
                if event["event_id"] not in existing:
                    f.write(json.dumps(event, sort_keys=True) + "\n")
                    existing.add(event["event_id"])
