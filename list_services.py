import json
with open('account_data.json', 'r', encoding='utf-8') as fh:
    data = json.load(fh)
services = data.get('settings', {}).get('plan', {}).get('services', {})
for i, name in enumerate(sorted(services)):
    print(f"{i:03d}: {name}")
