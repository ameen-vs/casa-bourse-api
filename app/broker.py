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
        
        # 3. Fetch Tab Data (Intraday, History, Statistics)
        intraday = _fetch_intraday(lid)
        historique = _fetch_historique(lid)
        statistiques = _fetch_statistiques(lid)
        
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
                "transactions": transactions,
                "historique": historique,
                "statistiques": statistiques
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
    """Calls the NEW JSON API for intraday chart data."""
    # New endpoint discovered after structural change
    # https://www.bmcecapitalbourse.com/bkbbourse/api/series/intraday
    url = "https://www.bmcecapitalbourse.com/bkbbourse/api/series/intraday"
    params = {
        "decorator": "ajax",
        "lid": lid,
        "mode": "snap",
        "period": "1m",
        "max": "1250"
    }
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            logger.error("Intraday status code %s for lid %s", r.status_code, lid)
    except Exception as e:
        logger.error("Failed to fetch intraday for %s: %s", lid, e)
    return None

def _fetch_historique(lid):
    """Scrapes historical daily data from the HIKU endpoint."""
    url = f"https://www.bmcecapitalbourse.com/bkbbourse/details/hiku/{lid}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            history = []
            table = soup.find("table")
            if table:
                # Headers: Date, Ouverture, Dernier, + Haut, + Bas, Quantité, Volume, Variation %
                for row in table.find_all("tr")[1:61]: # Extract up to ~60 days max for performance
                    cols = row.find_all("td")
                    if len(cols) >= 8:
                        try:
                            # Clean spaces and commas
                            def clean_float(t): return float(t.replace(" ", "").replace("\xa0", "").replace("\u202f", "").replace(",", "."))
                            def clean_int(t): return int(t.replace(" ", "").replace("\xa0", "").replace("\u202f", "").replace(",", "").split(".")[0])
                            
                            history.append({
                                "date": cols[0].text.strip(),
                                "open": clean_float(cols[1].text.strip()),
                                "close": clean_float(cols[2].text.strip()),
                                "high": clean_float(cols[3].text.strip()),
                                "low": clean_float(cols[4].text.strip()),
                                "quantity": clean_int(cols[5].text.strip()),
                                "volume": clean_int(cols[6].text.strip()),
                                "change_pct": cols[7].text.strip()
                            })
                        except Exception as e:
                            logger.error(f"Error parsing hiku row: {e}")
                            continue
            return history
    except Exception as e:
        logger.error("Failed to fetch historique for %s: %s", lid, e)
    return []

def _fetch_statistiques(lid):
    """Scrapes company stats and dividend history from the STATISTICS endpoint."""
    url = f"https://www.bmcecapitalbourse.com/bkbbourse/details/statistics/{lid}"
    stats_data = {"metrics": {}, "dividends": []}
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            
            # 1. Look for the general statistics rows (often strong or specific spans)
            for li in soup.find_all("li", class_="row"):
                label = li.find("strong")
                val = li.find("span", class_="valeur")
                if label and val:
                    key = label.text.strip().replace(":", "")
                    stats_data["metrics"][key] = val.text.strip().replace("\xa0", " ").replace("\u202f", " ")
                    
            # Look for table-based structures
            for tr in soup.find_all("tr"):
                th = tr.find("th")
                tds = tr.find_all("td")
                
                # Format 1: TH and TD (e.g., Capitalisation boursière, Secteur)
                if th and len(tds) == 1:
                    k = th.text.strip().replace(":", "")
                    v = tds[0].text.strip().replace("\xa0", " ").replace("\u202f", " ")
                    if k and v:
                        stats_data["metrics"][k] = v
                
                # Format 2: TD and TD (e.g., Shareholders data)
                elif len(tds) == 2 and not th:
                    k = tds[0].text.strip()
                    v = tds[1].text.strip().replace("\xa0", " ").replace("\u202f", " ")
                    if k and v and "Année" not in k:
                        stats_data["metrics"][k] = v

                # Format 3: Dividends (5 columns)
                elif len(tds) >= 5:
                    try:
                        yr=tds[0].text.strip()
                        if yr.isdigit():
                            stats_data["dividends"].append({
                                "year": yr,
                                "date_detachement": tds[1].text.strip(),
                                "date_paiement": tds[2].text.strip(),
                                "montant_brut": tds[3].text.strip().replace(",", "."),
                                "montant_net": tds[4].text.strip().replace(",", ".")
                            })
                    except: continue
                    
            return stats_data
    except Exception as e:
        logger.error("Failed to fetch statistiques for %s: %s", lid, e)
    return stats_data

if __name__ == "__main__":
    # Test script
    logging.basicConfig(level=logging.INFO)
    data, err = get_stock_details("SODEP")
    if data:
        print(json.dumps(data, indent=2))
    else:
        print(f"Error: {err}")
