from pathlib import Path
start=615885
end=615900
path=Path('data/pipeline_storage/2025-11-09/events.jsonl')
with path.open('r', encoding='utf-8') as fh:
    for idx,line in enumerate(fh,1):
        if idx<start:
            continue
        if idx>end:
            break
        print(f"{idx}: {line.rstrip()}" )
