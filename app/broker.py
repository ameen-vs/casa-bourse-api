import requests
import json
import logging
import os
from bs4 import BeautifulSoup
from app.market import get_ticker_metrics

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
}

BASE_DETAILS_URL = "https://www.bmcecapitalbourse.com/bkbbourse/details/"
INTRADAY_URL = "https://www.bmcecapitalbourse.com/bkbbourse/ajax/details/intraday"

# Load mapping
MAPPING_PATH = os.path.join(os.path.dirname(__file__), "broker_mapping.json")
try:
    with open(MAPPING_PATH, "r") as f:
        TICKER_TO_LID = json.load(f)
except Exception as e:
    logger.error("Failed to load broker mapping: %s", e)
    TICKER_TO_LID = {}

def get_stock_details(ticker: str):
    """Entry point for getting deep broker data for a ticker."""
    lid = TICKER_TO_LID.get(ticker)
    if not lid:
        # Try case-insensitive search or contains
        for k, v in TICKER_TO_LID.items():
            if ticker.lower() in k.lower():
                lid = v
                break
    
    if not lid:
        return None, f"Ticker '{ticker}' not found in broker database."

    full_url = f"{BASE_DETAILS_URL}{lid.replace(',', '%2C')}#Tab0"
    
    try:
        # 1. Fetch main page
        response = requests.get(full_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # 2. Extract Broker Data
        order_book = _extract_order_book(soup)
        transactions = _extract_transactions(soup)
        quotation = _extract_quotation_details(soup)
        
        # 3. Fetch Intraday (Charts)
        intraday = _fetch_intraday(lid)
        
        # 4. Fetch TradingView Metrics (Elite Analysis)
        metrics = get_ticker_metrics(ticker)

        return {
            "ticker": ticker,
            "lid": lid,
            "analyse_generale": metrics,
            "details_broker": {
                "graphique": intraday,
                "cotations": quotation,
                "carnet_ordres": order_book,
                "transactions": transactions
            },
            "source": "BMCE Capital Bourse + TradingView"
        }, None

    except Exception as e:
        logger.error("Broker fetch for %s failed: %s", ticker, e)
        return None, str(e)

def _extract_order_book(soup):
    """Extracts the Bid/Ask split tables with robust header detection."""
    book = {"bid": [], "ask": []}
    
    # Target the container with h2 "Carnet d'ordres"
    header = soup.find(lambda tag: tag.name == "h2" and "Carnet" in tag.text)
    container = header.find_parent("div", class_="whitebox") if header else None
    
    if not container:
        container = soup # Fallback to whole page
        
    tables = container.select("table")
    # Usually there are two tables side by side in col-6 divs
    # Table 1: ACHAT (Bid)
    # Table 2: VENTE (Ask)
    
    bid_table = None
    ask_table = None
    
    for t in tables:
        if "ACHAT" in t.text:
            bid_table = t
        if "VENTE" in t.text:
            ask_table = t
            
    # Clean and parse helper
    def p(val): return float(val.strip().replace(" ", "").replace("\xa0", "").replace(",", "."))
    def q(val): return int(val.strip().replace(" ", "").replace("\xa0", "").replace(",", "").split(".")[0])

    if bid_table:
        for row in bid_table.find_all("tr")[1:]: # Skip header
            cols = row.find_all("td")
            if len(cols) >= 3:
                try:
                    px = cols[2].text.strip()
                    qty = cols[1].text.strip()
                    if px and qty:
                        book["bid"].append({"price": p(px), "quantity": q(qty)})
                except: continue

    if ask_table:
        for row in ask_table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) >= 3:
                try:
                    # In VENTE table, the first TD is often a bar chart div, so price is index 1
                    px = cols[1].text.strip()
                    qty = cols[2].text.strip()
                    if px and qty:
                        book["ask"].append({"price": p(px), "quantity": q(qty)})
                except: continue
    
    return book

def _extract_transactions(soup):
    """Searches for the transaction history table."""
    txs = []
    # BMCE often has 'Dernières transactions' as a header
    header = soup.find(lambda tag: tag.name in ["h2", "h3"] and "transaction" in tag.text.lower())
    table = header.find_next("table") if header else None
    
    if not table:
        table = soup.select_one("table:has(th:contains('Heure')), table:has(th:contains('Date'))")

    if table:
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) >= 3:
                try:
                    txs.append({
                        "time": cols[0].text.strip(),
                        "price": float(cols[1].text.strip().replace(" ", "").replace("\xa0", "").replace(",", ".")),
                        "quantity": int(cols[2].text.strip().replace(" ", "").replace("\xa0", ""))
                    })
                except: continue
    return txs

def _extract_quotation_details(soup):
    """Extracts summary quotation info (High, Low, Open, Volume)."""
    data = {}
    # BMCE specific summary labels
    mapping = {
        "Plus haut": "high",
        "Plus bas": "low",
        "Ouverture": "open",
        "Volume": "volume",
        "Dernier": "last",
        "Variation": "change"
    }
    
    for row in soup.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if th and td:
            label = th.text.strip()
            for k, v in mapping.items():
                if k in label:
                    data[v] = td.text.strip().replace("\xa0", " ")
                    break
    return data

def _fetch_intraday(lid):
    """Calls the JSON API for intraday chart data."""
    params = {"lid": lid}
    try:
        r = requests.get(INTRADAY_URL, params=params, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

if __name__ == "__main__":
    # Test script
    logging.basicConfig(level=logging.INFO)
    data, err = get_stock_details("SODEP")
    if data:
        print(json.dumps(data, indent=2))
    else:
        print(f"Error: {err}")
