"""
main.py — Casa Bourse FastAPI app
==================================
Endpoints:
  GET /health               — liveness probe
  GET /status               — app metadata
  GET /market/meta          — static market reference (hours, links)
  GET /market/snapshot      — MASI, MASI20, top stocks (TradingView)
  GET /news                 — latest Medias24 articles
  GET /signals              — sentiment score + price trend heuristic
  GET /top-opportunities    — top buy/sell tickers by sentiment score
"""

import logging
from typing import Optional, List, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

# Internal modules — adjust import paths if your project layout differs
from app.market import build_market_snapshot, fetch_top_performers, generate_market_analysis, get_masi_performance
from app.broker import get_stock_details
from app.market_meta import MARKET_META
from app.scraper import get_articles, estimate_price_trend

logger = logging.getLogger(__name__)


# ── Lifespan (replaces deprecated @app.on_event) ──────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Casa Bourse API starting up…")
    yield
    logger.info("Casa Bourse API shutting down…")


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Casa Bourse API",
    version="2.0.0",
    description=(
        "Marché Casablanca : instantané (MASI / valeurs) + actualités Medias24 "
        "et signaux heuristiques. "
        "Le champ `prix_estime` sur /signals est un agrégat de sentiment sur titres "
        "détectés dans les titres, **pas** un cours officiel."
    ),
    lifespan=lifespan,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_get_articles(limit: int) -> list[dict]:
    """Wrap get_articles so a scraper failure raises a clean 503."""
    try:
        return get_articles(limit)
    except Exception as exc:
        logger.error("scraper error: %s", exc)
        raise HTTPException(
            status_code=503,
            detail=f"Scraper indisponible : {exc}",
        )


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["infra"], summary="Liveness probe")
def health():
    return {"status": "ok"}


@app.get("/status", tags=["infra"], summary="App metadata")
def status():
    return {
        "etat": "actif",
        "marché": "Bourse de Casablanca",
        "mode": "news + sentiment + signaux + marché (snapshot)",
        "version": app.version,
    }


@app.get(
    "/market/meta",
    tags=["marché"],
    summary="Référence marché (horaires, liens officiels)",
)
def market_meta():
    return {"source": "static", "data": MARKET_META}


@app.get(
    "/market/snapshot",
    tags=["marché"],
    summary="MASI, MASI20, top des valeurs",
    responses={
        200: {"description": "Snapshot complet (peut être partiel si une source échoue)"},
        503: {"description": "Toutes les sources de données sont indisponibles"},
    },
)
def market_snapshot(
    top_n: int = Query(default=10, ge=1, le=25, description="Nombre de valeurs à retourner"),
    top_by: str = Query(
        default="marketcap",
        pattern="^(marketcap|volume|abs_variation)$",
        description="Critère de tri : marketcap | volume | abs_variation",
    ),
):
    """
    - **Indices & cours** : Données TradingView (temps réel indicatif).
    - **Fallback** : scraper `casablancabourse.com` pour les indices si TradingView échoue.
    - **top_by** : `marketcap` (défaut), `volume`, `abs_variation` (tri par |variation %|).
    - Le champ `partial: true` indique qu'une ou plusieurs sources ont échoué.
    """
    try:
        snapshot = build_market_snapshot(top_n=top_n, top_by=top_by)
    except Exception as exc:
        logger.exception("build_market_snapshot raised unexpectedly")
        raise HTTPException(status_code=503, detail=str(exc))

    # If every data source failed, return 503 rather than an empty 200
    if snapshot.get("partial") and not snapshot.get("indices", {}).get("all") and not snapshot.get("top_actions", {}).get("items"):
        return JSONResponse(status_code=503, content=snapshot)

    return snapshot


@app.get(
    "/news",
    tags=["actualités"],
    summary="Derniers articles Medias24",
)
def news(
    limit: int = Query(default=10, ge=1, le=50, description="Nombre d'articles"),
):
    data = _safe_get_articles(limit)
    return {
        "source": "medias24",
        "count": len(data),
        "data": data,
    }


@app.get(
    "/signals",
    tags=["signaux"],
    summary="Sentiment agrégé + tendance heuristique",
)
def signals(
    limit: int = Query(default=10, ge=1, le=50, description="Nombre d'articles analysés"),
):
    """
    Agrège les scores de sentiment des `limit` derniers articles.
    `prix_estime` est une heuristique basée sur le sentiment — **pas** un cours officiel.
    """
    data = _safe_get_articles(limit)

    total_score = sum(x.get("score", 0) for x in data)

    try:
        price_trend = estimate_price_trend(data)
    except Exception as exc:
        logger.error("estimate_price_trend error: %s", exc)
        price_trend = None

    if total_score > 0:
        tendance = "haussière"
    elif total_score < 0:
        tendance = "baissière"
    else:
        tendance = "neutre"

    return {
        "tendance_globale": tendance,
        "score_total": total_score,
        "prix_estime": price_trend,
        "articles_analyses": len(data),
        "details": data,
    }


@app.get(
    "/top-opportunities",
    tags=["signaux"],
    summary="Top titres par performance (Jour, Semaine, Mois)",
)
def top_opportunities(
    period: str = Query(
        default="day",
        pattern="^(day|week|month)$",
        description="Période de performance : day | week | month"
    ),
    min_volume: int = Query(default=1000, ge=0, description="Volume minimum pour la liquidité"),
):
    """
    Identifie les meilleures performances sur le marché selon la période choisie.
    - **day** : Top 3 du jour
    - **week** : Top 10 de la semaine
    - **month** : Top 20 du mois
    Inclus une analyse heuristique et des conseils en français.
    """
    limit_map = {"day": 3, "week": 10, "month": 20}
    limit = limit_map.get(period, 3)
    
    try:
        masi_perf = get_masi_performance() if period == "day" else 0
        items, err = fetch_top_performers(period=period, limit=limit, min_volume=min_volume)
        if err:
            raise HTTPException(status_code=503, detail=f"Erreur TradingView: {err}")
            
        # Add relative performance calculation if not already there
        for item in items:
            if "rel_perf" not in item:
                item["rel_perf"] = (item.get(f"change_{period}") or 0) - masi_perf
                
        analysis = generate_market_analysis(items, period, masi_perf=masi_perf)
        
        return {
            "period": period,
            "masi_performance": masi_perf,
            "count": len(items),
            "top_performers": items,
            "marche_analyse": analysis["analyse"],
            "conseil_investissement": analysis["conseil"],
            "source": "tradingview",
            "disclaimer": "Ces analyses sont générées par heuristique et ne constituent pas un conseil financier officiel."
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("top_opportunities failed")
        raise HTTPException(status_code=500, detail=str(exc))
@app.get(
    "/stock/details",
    tags=["courtage"],
    summary="Détails profonds du broker (Carnet d'ordres, Transactions)",
)
def stock_details(
    ticker: Optional[str] = Query(None, description="Le ticker de l'action (ex: IAM, ATW, SODEP)"),
):
    """
    Retourne les données temps-réel (Analyse, Carnet d'ordres, Transactions, Graphique)
    pour une analyse technique et de flux approfondie.
    """
    if not ticker:
        raise HTTPException(
            status_code=400, 
            detail="EROC: Ticker manquant. Veuillez appeler getStockDetails(ticker='SYMBOLE')."
        )

    data, err = get_stock_details(ticker.upper())
    if err:
        raise HTTPException(status_code=404, detail=err)
    
    # 1. Volume Imbalance Calculation
    broker = data["details_broker"]
    carnet = broker["carnet_ordres"]
    bid_vol = sum(b["quantity"] for b in carnet["bid"])
    ask_vol = sum(a["quantity"] for a in carnet["ask"])
    
    pressure_hint = "Équilibre relatif"
    if bid_vol > ask_vol * 1.5:
        pressure_hint = "Forte pression acheteuse (Mur d'achat)"
    elif ask_vol > bid_vol * 1.5:
        pressure_hint = "Forte pression vendeuse (Résistance)"

    # 2. Combined technical hint
    technicals = data.get("analyse_generale")
    rsi_hint = ""
    if technicals and technicals.get("rsi"):
        rsi = technicals["rsi"]
        if rsi > 70: rsi_hint = " | ATTENTION: Surachat technique (RSI > 70)"
        elif rsi < 30: rsi_hint = " | OPPORTUNITÉ: Survente technique (RSI < 30)"

    # Flatten slightly for agent ease
    return {
        "ticker": data["ticker"],
        "lid": data["lid"],
        "smart_analysis_hint": f"{pressure_hint}{rsi_hint}",
        "analyse_generale": data["analyse_generale"],
        "graphique_intraday": broker["graphique"],
        "cotations": broker["cotations"],
        "carnet_ordres": broker["carnet_ordres"],
        "derniere_transactions": broker["transactions"],
        "source": data["source"]
    }
