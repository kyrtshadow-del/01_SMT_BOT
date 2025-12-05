import json
with open('account_data.json', 'r', encoding='utf-8') as fh:
    data = json.load(fh)
services = data.get('settings', {}).get('plan', {}).get('services', {})
print('services count', len(services))
for name in sorted(services):
    if 'req' in name:
        print('service', name, services[name])
for key, value in data.items():
    if isinstance(value, dict):
        for sub_key in value:
            if 'req' in sub_key.lower():
                print('subkey', key, sub_key, value[sub_key])
print('done')
