from fastapi import FastAPI
from app.scraper import get_articles, estimate_price_trend

app = FastAPI(title="Casa Bourse API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/news")
def news(limit: int = 10):
    data = get_articles(limit)
    return {
        "source": "medias24",
        "count": len(data),
        "data": data
    }


@app.get("/signals")
def signals(limit: int = 10):
    data = get_articles(limit)

    total_score = sum(x["score"] for x in data)

    price_trend = estimate_price_trend(data)

    return {
        "tendance_globale": (
            "haussière" if total_score > 0 else
            "baissière" if total_score < 0 else
            "neutre"
        ),
        "score_total": total_score,
        "prix_estime": price_trend,
        "details": data
    }


@app.get("/top-opportunities")
def top_opportunities(limit: int = 10):
    data = get_articles(limit)

    asset_scores = {}

    for item in data:
        for asset in item["assets"]:
            asset_scores.setdefault(asset, 0)
            asset_scores[asset] += item["score"]

    sorted_assets = sorted(asset_scores.items(), key=lambda x: x[1], reverse=True)

    best_buy = [a for a in sorted_assets if a[1] > 0][:3]
    best_sell = [a for a in sorted_assets if a[1] < 0][:3]

    return {
        "meilleures_opportunites_achat": best_buy,
        "meilleures_opportunites_vente": best_sell
    }


@app.get("/status")
def status():
    return {
        "etat": "actif",
        "marché": "Bourse de Casablanca",
        "mode": "news + sentiment + signaux"
    }