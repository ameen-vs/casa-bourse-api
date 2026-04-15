"""
market.py — Bourse de Casablanca snapshot
==========================================
Data sources (in priority order):
  1. Drahmi API v1  (https://api.drahmi.app/api/v1) — structured JSON, requires API key
  2. CasablancaBourse.com scraper — public fallback for indices & stocks (15-min delay)

SSL fix: all requests use `verify=True` by default; if you hit cert errors on a
corporate proxy, set the env-var  REQUESTS_CA_BUNDLE  to your bundle path, or
flip VERIFY_SSL=False below (not recommended in production).

Usage:
    snapshot = build_market_snapshot(top_n=10, top_by="marketcap")

Environment variables (optional):
    DRAHMI_API_KEY   — your Drahmi API key (get one at https://www.drahmi.app/api)
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

DRAHMI_API_BASE = "https://api.drahmi.app/api/v1"   # versioned endpoint
DRAHMI_API_KEY  = os.getenv("DRAHMI_API_KEY", "")   # set in env; empty = public tier

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
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    session.headers.update(HTTP_HEADERS)
    if DRAHMI_API_KEY:
        session.headers["X-API-Key"] = DRAHMI_API_KEY
    return session

_SESSION = _make_session()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ── Drahmi v1 helpers ─────────────────────────────────────────────────────────

def _drahmi_get(path: str, params: dict | None = None) -> tuple[Any, str | None]:
    """GET from Drahmi v1 API. Returns (data, error_string|None)."""
    url = f"{DRAHMI_API_BASE}{path}"
    try:
        r = _SESSION.get(url, params=params, timeout=20, verify=VERIFY_SSL)
        r.raise_for_status()
        return r.json(), None
    except requests.exceptions.SSLError as e:
        msg = (
            f"SSL error hitting {url}: {e}. "
            "Fix: set env REQUESTS_CA_BUNDLE to your CA bundle, or "
            "set VERIFY_SSL=False in market.py (insecure)."
        )
        logger.error(msg)
        return None, msg
    except requests.exceptions.ConnectionError as e:
        return None, f"Connection error: {e}"
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else "?"
        if status == 401:
            return None, f"Drahmi API: 401 Unauthorized — set DRAHMI_API_KEY env var."
        if status == 429:
            return None, f"Drahmi API: 429 Rate limited — slow down or upgrade plan."
        return None, f"Drahmi API HTTP {status}: {e}"
    except (requests.RequestException, ValueError) as e:
        return None, f"Drahmi request error: {e}"


def fetch_drahmi_indices() -> tuple[list[dict] | None, str | None]:
    data, err = _drahmi_get("/indices")
    if err:
        return None, err
    if not isinstance(data, list):
        return None, f"drahmi_indices: unexpected payload type {type(data)}"

    out = []
    for row in data:
        if not isinstance(row, dict):
            continue
        out.append({
            "code":           row.get("code"),
            "name":           row.get("name"),
            "value":          row.get("value"),
            "change_percent": row.get("changePercent"),
            "change_value":   row.get("changeValue"),
            "updated_at":     row.get("updatedAt"),
        })
    return out, None


def fetch_drahmi_market_status() -> tuple[dict | None, str | None]:
    """Returns whether the MASI session is currently open."""
    data, err = _drahmi_get("/market/status")
    if err:
        return None, err
    return data, None


def fetch_drahmi_stocks(
    page: int = 1, page_size: int = 100
) -> tuple[list[dict] | None, str | None]:
    data, err = _drahmi_get("/stocks", params={"page": page, "pageSize": page_size})
    if err:
        return None, err
    items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(items, list):
        return None, f"drahmi_stocks: missing items[] in response"
    return items, None


# ── CasablancaBourse.com scraper (fallback) ───────────────────────────────────
# This site publishes 15-min delayed prices with no API key required.
# We parse the JSON-LD or embedded table data rather than full HTML parsing
# to keep the dependency footprint small (no BeautifulSoup needed).

def fetch_casabourse_indices() -> tuple[list[dict] | None, str | None]:
    """
    Scrape index summary from casablancabourse.com.
    Returns a minimal list compatible with fetch_drahmi_indices().
    """
    url = f"{CASABOURSE_BASE}/"
    try:
        r = _SESSION.get(url, timeout=25, verify=VERIFY_SSL,
                         headers={**HTTP_HEADERS, "Accept": "text/html"})
        r.raise_for_status()
    except requests.exceptions.SSLError as e:
        return None, f"SSL error (fallback): {e}"
    except requests.RequestException as e:
        return None, f"casabourse fallback request error: {e}"

    html = r.text
    # Quick extraction of MASI / MASI20 from embedded script or visible text.
    # The site renders values like: MASI 18 285.05 -0.88%
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
                })
            except ValueError:
                pass

    if not indices:
        return None, "casabourse fallback: could not parse index values from HTML"
    return indices, None


# ── Sorting ───────────────────────────────────────────────────────────────────

def _sort_stocks(items: list[dict], top_by: str) -> list[dict]:
    key = (top_by or "marketcap").lower()
    if key == "volume":
        sort_key = lambda x: float(x.get("volume24h") or x.get("volume") or 0)
    elif key in ("variation", "abs_variation"):
        sort_key = lambda x: abs(float(x.get("variationPercent") or x.get("change") or 0))
    else:
        sort_key = lambda x: float(x.get("marketCap") or x.get("market_cap") or 0)
    return sorted(items, key=sort_key, reverse=True)


def _normalise_stock(row: dict) -> dict:
    """Map Drahmi v1 stock fields to a canonical output shape."""
    sector = row.get("sector")
    sector_name = (
        sector.get("name") if isinstance(sector, dict)
        else sector if isinstance(sector, str)
        else None
    )
    return {
        "ticker":            row.get("ticker"),
        "name":              row.get("name"),
        "price":             row.get("price"),
        "variation_percent": row.get("variationPercent"),
        "market_cap":        row.get("marketCap"),
        "volume_24h":        row.get("volume24h"),
        "sector":            sector_name,
    }


# ── Main snapshot builder ─────────────────────────────────────────────────────

def build_market_snapshot(top_n: int = 10, top_by: str = "marketcap") -> dict[str, Any]:
    """
    Build a market snapshot dict with:
      - market_status : open/closed from Drahmi
      - indices       : MASI & MASI20 (+ all indices)
      - top_actions   : top-N stocks ranked by top_by
      - errors        : list of non-fatal errors encountered
      - partial       : True if any source failed

    Falls back to casablancabourse.com scraper for indices if Drahmi fails.
    """
    errors: list[str] = []
    top_n = max(1, min(int(top_n), 25))

    # 1. Market status (open/closed)
    market_status, err = fetch_drahmi_market_status()
    if err:
        errors.append(err)
        market_status = None

    # 2. Indices — try Drahmi first, fall back to scraper
    indices, err = fetch_drahmi_indices()
    indices_source = f"{DRAHMI_API_BASE}/indices"
    if err:
        errors.append(f"[drahmi] {err}")
        logger.warning("Drahmi indices failed, trying casabourse fallback…")
        indices, err2 = fetch_casabourse_indices()
        indices_source = f"{CASABOURSE_BASE}/ (scraper fallback)"
        if err2:
            errors.append(f"[fallback] {err2}")

    # 3. Stocks — Drahmi only (no public scraper with enough detail)
    raw_stocks, err = fetch_drahmi_stocks()
    if err:
        errors.append(f"[drahmi] {err}")
        top_stocks: list[dict] = []
    else:
        ranked    = _sort_stocks(raw_stocks, top_by)
        top_stocks = [_normalise_stock(r) for r in ranked[:top_n]]

    # 4. Pull out MASI / MASI20 for convenience
    masi = masi20 = None
    if indices:
        for x in indices:
            c = (x.get("code") or "").upper()
            if c == "MASI":
                masi = x
            elif c in ("MASI20", "MASI 20"):
                masi20 = x

    return {
        "fetched_at": _utc_now_iso(),
        "disclaimer": (
            "Données indicatives. Cours et indices issus de l'API Drahmi (non officielle) "
            "ou du site CasablancaBourse.com (données retardées 15 min). "
            "Vérifier sur https://www.casablanca-bourse.com/fr en cas de doute. "
            "Pas un conseil en investissement."
        ),
        "market_status": market_status,
        "indices": {
            "source":     indices_source,
            "masi":       masi,
            "masi20":     masi20,
            "all":        indices,
        },
        "top_actions": {
            "source":     f"{DRAHMI_API_BASE}/stocks",
            "top_by":     (top_by or "marketcap").lower(),
            "count":      len(top_stocks),
            "items":      top_stocks,
        },
        "errors":  errors,
        "partial": bool(errors),
    }


# ── Quick CLI test ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import pprint
    logging.basicConfig(level=logging.INFO)
    snapshot = build_market_snapshot(top_n=5, top_by="marketcap")
    pprint.pprint(snapshot)
