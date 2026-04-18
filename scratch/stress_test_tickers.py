import requests
import json
import time

BASE_URL = "http://127.0.0.1:8000/stock/details"
TICKERS_TO_TEST = ["IAM", "ATW", "MSA", "ADH", "LHM", "SNP", "TGC", "AKT", "CSM"]

def run_stress_test():
    print(f"Starting Connectivity Stress Test for {len(TICKERS_TO_TEST)} tickers...\n")
    success_count = 0
    
    for ticker in TICKERS_TO_TEST:
        print(f"Testing {ticker}...", end=" ", flush=True)
        try:
            r = requests.get(BASE_URL, params={"ticker": ticker}, timeout=20)
            if r.status_code == 200:
                data = r.json()
                metrics = data.get("analyse_generale")
                broker = data.get("carnet_ordres")
                
                if metrics and broker:
                    print("✅ SUCCESS (Metrics + Broker)")
                    success_count += 1
                elif broker:
                    print("⚠️ PARTIAL (Broker OK, Metrics NULL)")
                else:
                    print("❌ FAILED (Empty Data)")
            else:
                print(f"❌ ERROR {r.status_code}: {r.text}")
        except Exception as e:
            print(f"💥 CRASH: {e}")
        time.sleep(1) # Be kind to the APIs

    print(f"\nFinal Result: {success_count}/{len(TICKERS_TO_TEST)} fully successful.")

if __name__ == "__main__":
    run_stress_test()
