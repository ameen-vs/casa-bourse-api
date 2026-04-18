"""
market.py — Bourse de Casablanca snapshot
==========================================
Data sources (in priority order):
  1. TradingView Scanner API — real-time (stocks & indices)
  2. CasablancaBourse.com scraper — indices fallback (15-min delay)

SSL fix: all requests use `verify=True` by default; if you hit cert errors on a
corporate proxy, set the env-var  REQUESTS_CA_BUNDLE  to your bundle path, or
flip VERIFY_SSL=False below (not recommended in production).

Usage:
    snapshot = build_market_snapshot(top_n=10, top_by="marketcap")

Environment variables (optional):
    REQUESTS_CA_BUNDLE — path to custom CA bundle (fixes SSL on corporate proxies)
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

# TradingView Scanner API (Unofficial)
TRADINGVIEW_SCANNER_URL = "https://scanner.tradingview.com/global/scan"

# Fallback scrape target (public, ~15-min delayed data, no key needed)
CASABOURSE_BASE = "https://www.casablancabourse.com"

# SSL: set to False only if you're behind a proxy with a self-signed cert AND you
# understand the security implications. Better: point REQUESTS_CA_BUNDLE to your bundle.
VERIFY_SSL: bool = True

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; BourseSnapshotBot/2.0; "
        "+https://github.com/your-repo)"
    ),
    "Accept": "application/json",
}

# ── HTTP session with retry + timeout ─────────────────────────────────────────

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    session.headers.update(HTTP_HEADERS)
    return session

_SESSION = _make_session()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ── TradingView integration ───────────────────────────────────────────────────

def fetch_tradingview_stocks() -> tuple[list[dict] | None, str | None]:
    """
    Fetch Casablanca Stock Exchange (CSEMA) stock data from TradingView Scanner.
    Returns (list of normalized stocks, error_string).
    """
    payload = {
        "filter": [
            {"left": "exchange", "operation": "equal", "right": "CSEMA"}
        ],
        "options": {"lang": "en"},
        "columns": [
            "name",
            "close",
            "change",
            "change_abs",
            "market_cap_basic",
            "volume",
            "description",
            "type",
            "sector",
            "RSI",
            "price_earnings_ttm",
            "dividend_yield_recent",
            "earnings_per_share_basic_ttm"
        ],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 100]
    }
    
    try:
        r = _SESSION.post(
            TRADINGVIEW_SCANNER_URL,
            json=payload,
            timeout=15,
            verify=VERIFY_SSL
        )
        r.raise_for_status()
        data = r.json()
        
        raw_items = data.get("data", [])
        stocks = []
        for item in raw_items:
            # item['d'] contains the values corresponding to 'columns' in payload
            d = item.get("d", [])
            if len(d) < 7:
                continue
            
            stocks.append({
                "ticker":            item.get("s", "").replace("CSEMA:", ""),
                "name":              d[0],
                "price":             d[1],
                "variation_percent": d[2],
                "change_abs":        d[3],
                "market_cap":        d[4],
                "volume_24h":        d[5],
                "description":       d[6],
                "type":              d[7],
                "sector":            d[8] if len(d) > 8 else None,
                "rsi":               d[9] if len(d) > 9 else None,
                "pe_ratio":          d[10] if len(d) > 10 else None,
                "dividend_yield":    d[11] if len(d) > 11 else None,
                "eps":               d[12] if len(d) > 12 else None,
                "source":            "tradingview"
            })
        
        if not stocks:
            return None, "TradingView: no stocks returned for exchange CSEMA"
            
        return stocks, None
        
    except Exception as e:
        logger.error("TradingView fetch failed: %s", e)
        return None, f"TradingView error: {str(e)}"


def fetch_tradingview_indices() -> tuple[list[dict] | None, str | None]:
    """
    Fetch Casablanca Stock Exchange indices (MASI, MSI20) from TradingView.
    """
    payload = {
        "symbols": {
            "tickers": ["CSEMA:MASI", "CSEMA:MSI20"],
            "query": {"types": []}
        },
        "columns": ["name", "close", "change", "change_abs"]
    }
    
    try:
        r = _SESSION.post(
            TRADINGVIEW_SCANNER_URL,
            json=payload,
            headers=HTTP_HEADERS,
            timeout=10,
            verify=VERIFY_SSL
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        
        indices = []
        for item in data:
            d = item.get("d", [])
            if len(d) < 4:
                continue
            
            indices.append({
                "code":           d[0],
                "name":           d[0],
                "value":          d[1],
                "change_percent": d[2],
                "change_value":   d[3],
                "updated_at":     _utc_now_iso(),
                "source":         "tradingview"
            })
        
        return indices, None
    except Exception as e:
        logger.error("TradingView indices failed: %s", e)
        return None, str(e)


def fetch_top_performers(
    period: str = "day", limit: int = 10, min_volume: int = 500
) -> tuple[list[dict] | None, str | None]:
    """
    Fetch top gainers for a specific period (day, week, month).
    Filters by minimum volume to ensure liquidity.
    """
    col_map = {
        "day":   "change",
        "week":  "Perf.W",
        "month": "Perf.1M"
    }
    sort_col = col_map.get(period, "change")
    
    payload = {
        "filter": [
            {"left": "exchange", "operation": "equal", "right": "CSEMA"},
            {"left": "volume", "operation": "greater", "right": min_volume},
            {"left": "type", "operation": "equal", "right": "stock"}
        ],
        "options": {"lang": "en"},
        "columns": [
            "name", "close", "change", "Perf.W", "Perf.1M",
            "market_cap_basic", "volume", "description", "sector", "RSI",
            "price_earnings_ttm", "dividend_yield_recent", "earnings_per_share_basic_ttm"
        ],
        "sort": {"sortBy": sort_col, "sortOrder": "desc"},
        "range": [0, limit]
    }
    
    try:
        r = _SESSION.post(
            TRADINGVIEW_SCANNER_URL,
            json=payload,
            timeout=15,
            verify=VERIFY_SSL
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        
        out = []
        for item in data:
            d = item.get("d", [])
            out.append({
                "ticker":            item.get("s", "").replace("CSEMA:", ""),
                "name":              d[0],
                "price":             d[1],
                "change_day":        d[2],
                "change_week":       d[3],
                "change_month":      d[4],
                "market_cap":        d[5],
                "volume_24h":        d[6],
                "description":       d[7],
                "sector":            d[8],
                "rsi":               d[9] if len(d) > 9 else None,
                "pe_ratio":          d[10] if len(d) > 10 else None,
                "dividend_yield":    d[11] if len(d) > 11 else None,
                "eps":               d[12] if len(d) > 12 else None,
                "source":            "tradingview"
            })
        return out, None
    except Exception as e:
        logger.error("fetch_top_performers (%s) failed: %s", period, e)
        return None, str(e)


def get_masi_performance() -> float:
    """Helper to get current MASI performance."""
    indices, _ = fetch_tradingview_indices()
    if indices:
        for idx in indices:
            if idx.get("code") == "MASI":
                return idx.get("change_percent") or 0
    return 0


def generate_market_analysis(items: list[dict], period: str, **kwargs) -> dict:
    """
    Generates a localized (French) analysis and advice based on performers.
    """
    if not items:
        return {
            "analyse": "Données insuffisantes pour une analyse détaillée.",
            "conseil": "Prudence recommandée. Attendre une confirmation des volumes."
        }
    
    avg_perf = sum(abs(i.get(f"change_{period}") or 0) for i in items) / len(items)
    
    period_label = {"day": "séance", "week": "semaine", "month": "mois"}.get(period, "période")
    
    # Heuristic analysis based on performance, technical fatigue, value and benchmarking
    masi_perf = kwargs.get("masi_perf", 0)
    overbought = [i for i in items if (i.get("rsi") or 0) > 70]
    oversold   = [i for i in items if (i.get("rsi") or 0) < 30 and (i.get("rsi") or 0) > 0]
    high_yield = [i for i in items if (i.get("dividend_yield") or 0) > 5]
    good_value = [i for i in items if (i.get("pe_ratio") or 0) > 0 and (i.get("pe_ratio") or 0) < 12]
    outperform = [i for i in items if (i.get("rel_perf") or 0) > 2] # 2% above MASI
    
    if avg_perf > 10:
        analyse = f"Forte volatilité haussière constatée sur cette {period_label}."
        if overbought:
            analyse += f" Attention : {len(overbought)} titres sont en zone de surachat (RSI > 70)."
        conseil = "Attention aux prises de bénéfices imminent."
    elif outperform and masi_perf < 0:
        analyse = f"Résilience notable : {len(outperform)} titres affichent une force relative positive malgré un MASI en baisse ({masi_perf:.2f}%)."
        conseil = "Focus sur les leaders par force relative qui résistent à la baisse du marché."
    elif high_yield:
        analyse = f"Le marché offre des opportunités de rendement stables ({len(high_yield)} valeurs avec un dividende > 5%)."
        conseil = "Idéal pour une stratégie de revenus (cash-flow) axée sur le rendement."
    elif good_value:
        analyse = f"Certaines valeurs présentent des ratios de valorisation attractifs (P/E < 12)."
        conseil = "Opportunités 'Value' à étudier pour un investissement de moyen/long terme."
    elif oversold:
        analyse = f"Signes de survente détectés ({len(oversold)} valeurs avec RSI < 30)."
        conseil = "Potentiel de rebond technique à surveiller."
    else:
        analyse = f"Marché calme ou en consolidation sur cette {period_label}."
        conseil = "Accumulation sélective possible sur les supports techniques."
        
    return {
        "analyse": analyse,
        "conseil": conseil
    }


# ── CasablancaBourse.com scraper (fallback) ───────────────────────────────────

def fetch_casabourse_indices() -> tuple[list[dict] | None, str | None]:
    """
    Scrape index summary from casablancabourse.com.
    """
    url = f"{CASABOURSE_BASE}/"
    try:
        r = _SESSION.get(url, timeout=25, verify=VERIFY_SSL,
                         headers={**HTTP_HEADERS, "Accept": "text/html"})
        r.raise_for_status()
    except requests.RequestException as e:
        return None, f"casabourse fallback request error: {e}"

    html = r.text
    import re
    indices = []
    patterns = {
        "MASI":   r"MASI\D{0,10}([\d\s,.]+)\s*([\-+]?\d+[.,]\d+)\s*%",
        "MASI20": r"MASI\s*20\D{0,10}([\d\s,.]+)\s*([\-+]?\d+[.,]\d+)\s*%",
    }
    for code, pat in patterns.items():
        m = re.search(pat, html)
        if m:
            raw_value = m.group(1).replace(" ", "").replace(",", ".")
            raw_chg   = m.group(2).replace(",", ".")
            try:
                indices.append({
                    "code":           code,
                    "name":           code,
                    "value":          float(raw_value),
                    "change_percent": float(raw_chg),
                    "change_value":   None,
                    "updated_at":     _utc_now_iso(),
                    "source":         "casabourse"
                })
            except ValueError:
                pass

    if not indices:
        return None, "casabourse fallback: could not parse index values"
    return indices, None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sort_stocks(items: list[dict], top_by: str) -> list[dict]:
    key = (top_by or "marketcap").lower()
    if key == "volume":
        sort_key = lambda x: float(x.get("volume_24h") or 0)
    elif key in ("variation", "abs_variation"):
        sort_key = lambda x: abs(float(x.get("variation_percent") or 0))
    else:
        sort_key = lambda x: float(x.get("market_cap") or 0)
    return sorted(items, key=sort_key, reverse=True)


def _normalise_stock(row: dict) -> dict:
    """Map raw stock fields to a canonical output shape."""
    return {
        "ticker":            row.get("ticker"),
        "name":              row.get("name"),
        "price":             row.get("price"),
        "variation_percent": row.get("variation_percent") or row.get("change_day"),
        "change_week":       row.get("change_week"),
        "change_month":      row.get("change_month"),
        "rsi":               row.get("rsi"),
        "pe_ratio":          row.get("pe_ratio"),
        "dividend_yield":    row.get("dividend_yield"),
        "eps":               row.get("eps"),
        "rel_perf":          row.get("rel_perf"),
        "market_cap":        row.get("market_cap"),
        "volume_24h":        row.get("volume_24h"),
        "sector":            row.get("sector"),
        "description":       row.get("description"),
    }


# ── Main snapshot builder ─────────────────────────────────────────────────────

def build_market_snapshot(top_n: int = 10, top_by: str = "marketcap") -> dict[str, Any]:
    """
    Build a market snapshot dict with:
      - indices       : MASI & MASI20 (+ all indices)
      - top_actions   : top-N stocks ranked by top_by
      - errors        : list of non-fatal errors encountered
      - partial       : True if any source failed
    """
    errors: list[str] = []
    top_n = max(1, min(int(top_n), 25))

    # 1. Indices — try TradingView first, fall back to scraper
    indices, err = fetch_tradingview_indices()
    indices_source = "tradingview"
    
    if err:
        errors.append(f"[tradingview-indices] {err}")
        logger.warning("TradingView indices failed, trying casabourse fallback…")
        indices, err_fb = fetch_casabourse_indices()
        indices_source = f"{CASABOURSE_BASE}/ (scraper fallback)"
        if err_fb:
            errors.append(f"[fallback-indices] {err_fb}")

    # 2. Stocks — Try TradingView
    raw_stocks, err = fetch_tradingview_stocks()
    stocks_source = "tradingview"
    
    if err:
        errors.append(f"[tradingview-stocks] {err}")
        raw_stocks = []

    if raw_stocks:
        ranked    = _sort_stocks(raw_stocks, top_by)
        top_stocks = [_normalise_stock(r) for r in ranked[:top_n]]
    # 3. Pull out MASI / MASI20 for convenience
    masi = masi20 = None
    masi_perf = 0
    if indices:
        for x in indices:
            c = (x.get("code") or "").upper()
            if c == "MASI":
                masi = x
                masi_perf = x.get("change_percent") or 0
            elif c in ("MASI20", "MASI 20", "MSI20"):
                masi20 = x

    # 4. Calculate relative performance for stocks
    if raw_stocks:
        for s in raw_stocks:
            stock_perf = s.get("variation_percent") or 0
            s["rel_perf"] = stock_perf - masi_perf

    if raw_stocks:
        ranked    = _sort_stocks(raw_stocks, top_by)
        top_stocks = [_normalise_stock(r) for r in ranked[:top_n]]
    else:
        top_stocks = []

    result = {
        "fetched_at": _utc_now_iso(),
        "disclaimer": (
            "Données indicatives. Cours et indices issus de TradingView (non officiel) "
            "ou du site CasablancaBourse.com (données retardées 15 min). "
            "Vérifier sur https://www.casablanca-bourse.com/fr en cas de doute. "
            "Pas un conseil en investissement."
        ),
        "indices": {
            "source":     indices_source,
            "masi":       masi,
            "masi20":     masi20,
            "all":        indices,
        },
        "top_actions": {
            "source":     stocks_source,
            "top_by":     (top_by or "marketcap").lower(),
            "count":      len(top_stocks),
            "items":      top_stocks,
        },
        "partial": bool(errors),
    }
    
    # Only include errors if they exist for a cleaner response
    if errors:
        result["errors"] = errors
        
    return result


# ── Quick CLI test ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import pprint
    logging.basicConfig(level=logging.INFO)
    snapshot = build_market_snapshot(top_n=5, top_by="marketcap")
    pprint.pprint(snapshot)

def get_ticker_metrics(ticker: str):
    """Fetches high-level RSI/PE/Yield metrics for a single ticker."""
    try:
        # Use a targeted query for just this ticker
        payload = {
            "filter": [
                {"left": "exchange", "operation": "equal", "right": "CSEMA"},
                {"left": "name", "operation": "equal", "right": ticker.upper()}
            ],
            "options": {"lang": "en"},
            "symbols": {"query": {"types": []}, "tickers": []},
            "columns": ["name", "close", "RSI", "price_earnings_ttm", "dividend_yield_recent", "earnings_per_share_basic_ttm", "change"],
            "sort": {"sortBy": "name", "sortOrder": "asc"},
            "range": [0, 1]
        }
        
        r = requests.post(TRADINGVIEW_SCANNER_URL, json=payload, headers=HTTP_HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json().get("data", [])
        
        if not data:
            return None
            
        d = data[0]["d"]
        return {
            "rsi": d[2],
            "pe": d[3],
            "yield": d[4],
            "eps": d[5],
            "change_day": d[6]
        }
    except Exception as e:
        logger.error("Failed to fetch metrics for %s: %s", ticker, e)
        return None
