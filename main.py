"""
main.py — Casa Bourse FastAPI app
==================================
Endpoints:
  GET /health               — liveness probe
  GET /status               — app metadata
  GET /market/meta          — static market reference (hours, links)
  GET /market/snapshot      — MASI, MASI20, top stocks (Drahmi + fallback)
  GET /news                 — latest Medias24 articles
  GET /signals              — sentiment score + price trend heuristic
  GET /top-opportunities    — top buy/sell tickers by sentiment score
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

# Internal modules — adjust import paths if your project layout differs
from app.market import build_market_snapshot
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
    - **Indices & cours** : API Drahmi (`api.drahmi.app/api/v1`) — données indicatives.
    - **Fallback** : scraper `casablancabourse.com` pour les indices si Drahmi échoue.
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
    summary="Top titres achat / vente par score de sentiment",
)
def top_opportunities(
    limit: int = Query(default=10, ge=1, le=50, description="Nombre d'articles analysés"),
    top_k: int = Query(default=3, ge=1, le=10, description="Nombre de titres par catégorie"),
):
    """
    Identifie les tickers les plus mentionnés positivement (achat) ou négativement (vente).
    Basé sur le sentiment des articles — **pas** un conseil en investissement.
    """
    data = _safe_get_articles(limit)

    asset_scores: dict[str, int] = {}
    for item in data:
        for asset in item.get("assets", []):      # safe: .get() avoids KeyError
            asset_scores[asset] = asset_scores.get(asset, 0) + item.get("score", 0)

    sorted_assets = sorted(asset_scores.items(), key=lambda x: x[1], reverse=True)

    return {
        "meilleures_opportunites_achat": [
            {"ticker": a, "score": s} for a, s in sorted_assets if s > 0
        ][:top_k],
        "meilleures_opportunites_vente": [
            {"ticker": a, "score": s} for a, s in sorted_assets if s < 0
        ][:top_k],
        "articles_analyses": len(data),
    }
