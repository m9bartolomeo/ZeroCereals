"""
Script locale per testare il trigger import direttamente da Mac
Usa: python3 trigger_import.py
"""
import urllib.request, urllib.error, json, base64, sys

API = 'https://rd.metodonove.com/api.php'
IMPORT_KEY = '831807adc8a88be29409ec9a5111dd792a55cb35268cb8820c4f7da99fbdc9e5'

# Modifica queste credenziali
HTTP_USER = 'bartolomeo'
HTTP_PASS = 'INSERISCI_QUI_LA_TUA_PASSWORD'

def call(action, params=''):
    url = f'{API}?action={action}{params}'
    creds = base64.b64encode(f'{HTTP_USER}:{HTTP_PASS}'.encode()).decode()
    req = urllib.request.Request(url, method='POST' if action=='import_table' else 'GET')
    req.add_header('X-Import-Key', IMPORT_KEY)
    req.add_header('Authorization', f'Basic {creds}')
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return {'error': e.code, 'body': e.read().decode()[:300]}
    except Exception as e:
        return {'error': str(e)}

# 1. Health check
print('Health check...')
r = call('health')
print(json.dumps(r, indent=2))

# 2. Import
print('\nImport...')
r = call('import')
print(json.dumps(r, indent=2))
