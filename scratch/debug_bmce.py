import requests
import os
import sys

# Add current dir to path to import HEADERS
sys.path.append(os.getcwd())
from app.broker import HEADERS, BASE_DETAILS_URL

lid = '32976479,102,608' # SODEP
url = f"{BASE_DETAILS_URL}{lid.replace(',', '%2C')}#Tab0"

print(f"Fetching {url}...")
try:
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    dump_path = 'c:/Users/amine/.gemini/antigravity/brain/bffb05ec-13e5-4f74-8376-4982d2189ad9/scratch/bmce_dump.html'
    with open(dump_path, 'w', encoding='utf-8') as f:
        f.write(r.text)
    print(f"Dumped {len(r.text)} bytes to {dump_path}")
except Exception as e:
    print(f"Error: {e}")
