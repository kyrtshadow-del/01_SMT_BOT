import json
import os
from bot_new import WialonClient

client = WialonClient(os.environ['WIALON_HOST'], os.environ['WIALON_TOKEN'])
client.login()
data = client.request('core/get_account_data', {'type': 2})
with open('account_data.json', 'w', encoding='utf-8') as fh:
    json.dump(data, fh, ensure_ascii=False, indent=2)
print('saved account_data.json, keys', list(data.keys()))
