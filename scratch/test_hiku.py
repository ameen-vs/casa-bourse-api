import logging
import json
from app.broker import get_stock_details

logging.basicConfig(level=logging.INFO)

def test_new_fields():
    print("Fetching ADH...")
    data, err = get_stock_details("ADH")
    
    if err:
        print(f"Error: {err}")
        return
        
    broker = data.get("details_broker", {})
    history = broker.get("historique", [])
    stats = broker.get("statistiques", {})
    
    print(f"\n--- History (First 5 of {len(history)} rows) ---")
    print(json.dumps(history[:5], indent=2))
    
    print(f"\n--- Statistics ---")
    print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    test_new_fields()
