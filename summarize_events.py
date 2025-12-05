import json
import sys
from collections import Counter
from pathlib import Path
path = Path('data/pipeline_storage/2025-11-09/events.jsonl')
if not path.exists():
    print('file not found')
    sys.exit(1)
count = 0
units = Counter()
first = None
last = None
with path.open('r', encoding='utf-8') as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        data = json.loads(line)
        count += 1
        units[data['unit_id']] += 1
        ts = data['device_ts']
        first = ts if first is None or ts < first else first
        last = ts if last is None or ts > last else last
print({'events': count, 'units': len(units), 'span_hours': round((last - first)/3600, 2) if first and last else 0})
