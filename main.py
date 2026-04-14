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
        "count": len(data),
        "data": data
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