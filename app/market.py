import json
from datetime import datetime, timezone
from typing import Any

import requests

CSE_JSONAPI_BASE = "https://api.casablanca-bourse.com/fr/api"
DRAHMI_API_BASE = "https://api.drahmi.app/api"

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CasaBourseAPI/1.0; +https://www.casablanca-bourse.com/fr)",
    "Accept": "application/json",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_cse_session() -> tuple[dict[str, Any] | None, str | None]:
    """
    Official Bourse de Casablanca session metadata from the public JSON:API
    (CMS-backed live-market header on the indices page).
    """
    url = f"{CSE_JSONAPI_BASE}/node/bourse_indice"
    try:
        r = requests.get(
            url,
            params={"page[limit]": 1},
            headers={**HTTP_HEADERS, "Accept": "application/vnd.api+json"},
            timeout=20,
        )
        r.raise_for_status()
        payload = r.json()
    except (requests.RequestException, ValueError) as e:
        return None, f"cse_session: {e}"

    rows = payload.get("data")
    if not rows:
        return None, "cse_session: empty bourse_indice response"

    node = rows[0] if isinstance(rows, list) else rows
    blocks = (node.get("attributes") or {}).get("internal_blocks") or []
    seance = _parse_live_market_seance(blocks)
    if not seance:
        return None, "cse_session: could not parse seance from CMS blocks"

    return {
        "source": "casablanca_bourse_jsonapi",
        "source_url": url,
        "seance_date": seance.get("seance"),
        "session_state": seance.get("etat_seance"),
        "scheduled_open_local": seance.get("heure_ouverture"),
    }, None


def _parse_live_market_seance(blocks: list) -> dict | None:
    for b in blocks:
        c = b.get("content") or {}
        if c.get("widget_id") != "bourse_data_listing:live-market-header":
            continue
        raw = c.get("widget_data") or "{}"
        try:
            wd = json.loads(raw)
        except json.JSONDecodeError:
            continue
        comps = wd.get("components") or []
        if not comps:
            continue
        seance = comps[0].get("seance")
        if isinstance(seance, dict):
            return seance
    return None


def fetch_drahmi_indices() -> tuple[list[dict[str, Any]] | None, str | None]:
    url = f"{DRAHMI_API_BASE}/indices"
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        return None, f"drahmi_indices: {e}"

    if not isinstance(data, list):
        return None, "drahmi_indices: unexpected payload"

    out = []
    for row in data:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "code": row.get("code"),
                "name": row.get("name"),
                "value": row.get("value"),
                "change_percent": row.get("changePercent"),
                "change_value": row.get("changeValue"),
                "updated_at": row.get("updatedAt"),
            }
        )
    return out, None


def fetch_drahmi_stocks_page() -> tuple[list[dict[str, Any]] | None, str | None]:
    url = f"{DRAHMI_API_BASE}/stocks"
    try:
        r = requests.get(
            url,
            params={"page": 1, "pageSize": 100},
            headers=HTTP_HEADERS,
            timeout=25,
        )
        r.raise_for_status()
        payload = r.json()
    except (requests.RequestException, ValueError) as e:
        return None, f"drahmi_stocks: {e}"

    items = payload.get("items")
    if not isinstance(items, list):
        return None, "drahmi_stocks: missing items[]"

    return items, None


def _sort_stocks(items: list[dict], top_by: str) -> list[dict]:
    key = (top_by or "marketcap").lower()
    if key == "volume":
        sort_key = lambda x: float(x.get("volume24h") or 0)
    elif key in ("variation", "abs_variation"):
        sort_key = lambda x: abs(float(x.get("variationPercent") or 0))
    else:
        sort_key = lambda x: float(x.get("marketCap") or 0)

    return sorted(items, key=sort_key, reverse=True)


def build_market_snapshot(top_n: int = 10, top_by: str = "marketcap") -> dict[str, Any]:
    errors: list[str] = []
    top_n = max(1, min(int(top_n), 25))

    session, err = fetch_cse_session()
    if err:
        errors.append(err)

    indices, err = fetch_drahmi_indices()
    if err:
        errors.append(err)

    raw_stocks, err = fetch_drahmi_stocks_page()
    if err:
        errors.append(err)
        top_stocks: list[dict] = []
    else:
        ranked = _sort_stocks(raw_stocks, top_by)
        top_stocks = []
        for row in ranked[:top_n]:
            top_stocks.append(
                {
                    "ticker": row.get("ticker"),
                    "name": row.get("name"),
                    "price": row.get("price"),
                    "variation_percent": row.get("variationPercent"),
                    "market_cap": row.get("marketCap"),
                    "volume_24h": row.get("volume24h"),
                    "sector": (row.get("sector") or {}).get("name")
                    if isinstance(row.get("sector"), dict)
                    else None,
                }
            )

    masi = None
    masi20 = None
    if indices:
        for x in indices:
            c = (x.get("code") or "").upper()
            if c == "MASI":
                masi = x
            elif c == "MASI20":
                masi20 = x

    return {
        "fetched_at": _utc_now_iso(),
        "disclaimer": (
            "Données indicatives: séance et entête issus du portail officiel (JSON:API). "
            "Cours et indices proviennent de l’API publique Drahmi (non officielle); "
            "vérifier sur https://www.casablanca-bourse.com/fr en cas de doute. "
            "Pas un conseil en investissement."
        ),
        "session_officielle": session,
        "indices": {
            "source": "drahmi_public_api",
            "source_url": f"{DRAHMI_API_BASE}/indices",
            "masi": masi,
            "masi20": masi20,
            "all": indices,
        },
        "top_actions": {
            "source": "drahmi_public_api",
            "source_url": f"{DRAHMI_API_BASE}/stocks",
            "top_by": (top_by or "marketcap").lower(),
            "count": len(top_stocks),
            "items": top_stocks,
        },
        "errors": errors,
        "partial": bool(errors),
    }
