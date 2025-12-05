import os
from bot_new import WialonClient

def main():
    host = os.environ['WIALON_HOST']
    token = os.environ['WIALON_TOKEN']
    client = WialonClient(host, token)
    client.login()
    data = client.request('core/get_account_data', {'type': 2})
    for section_name in ('plan', 'settings'):
        section = data
        for part in section_name.split('.'):  # not used but keep
            pass
    services = data.get('settings', {}).get('plan', {}).get('services')
    if not services:
        services = data.get('plan', {}).get('services')
    if not services:
        print('No services found')
        return
    for name, cfg in services.items():
        if 'request' in name:
            print(name, cfg)

if __name__ == '__main__':
    main()
