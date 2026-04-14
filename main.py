from fastapi import FastAPI
from app.scraper import get_articles

app = FastAPI(title="Casa Bourse API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/news")
def news(limit: int = 10):
    data = get_articles(limit)
    return {
        "source": "medias24",
        "nombre_d_articles": len(data),
        "articles": data
    }


@app.get("/signals")
def signals(limit: int = 10):
    data = get_articles(limit)

    # simple ranking
    sorted_data = sorted(data, key=lambda x: x["score"], reverse=True)

    return {
        "buy_signals": [d for d in sorted_data if d["score"] > 0][:5],
        "sell_signals": [d for d in sorted_data if d["score"] < 0][:5],
        "neutral": [d for d in sorted_data if d["score"] == 0][:5]
    }

@app.get("/signals-explained")
def signals():
    data = get_articles()

    sorted_data = sorted(data, key=lambda x: x["score"], reverse=True)

    return {
        "signal_achat": [x for x in sorted_data if x["score"] > 0][:5],
        "signal_vente": [x for x in sorted_data if x["score"] < 0][:5],
        "neutre": [x for x in sorted_data if x["score"] == 0][:5],
        "explication": "Score positif = tendance haussière, score négatif = tendance baissière"
    }

@app.get("/status")
def status():
    return {
        "etat": "actif",
        "marché": "Bourse de Casablanca",
        "langue": "fr",
        "mode": "news + sentiment + signaux"
    }