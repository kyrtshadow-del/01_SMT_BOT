import json
from pathlib import Path
path = Path('data/pipeline_storage/2025-11-09/events.jsonl')
with path.open('r', encoding='utf-8') as fh:
    for idx, line in enumerate(fh, 1):
        text = line.strip()
        if not text:
            print(f'blank line at {idx}')
            break
        try:
            json.loads(text)
        except Exception as exc:
            print(f'bad line {idx}: {exc}')
            break
    else:
        print('ok')
