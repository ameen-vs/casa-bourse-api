import requests
from bs4 import BeautifulSoup

URL = "https://medias24.com/categorie/leboursier/actus/"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def detect_assets(title):
    assets = []

    keywords = {
        "MASI": ["masi", "bourse"],
        "ATW": ["attijari"],
        "BCP": ["bcp", "banque populaire"],
        "MNG": ["managem"],
        "IAM": ["maroc telecom"]
    }

    for asset, words in keywords.items():
        for w in words:
            if w in title.lower():
                assets.append(asset)
                break

    return list(set(assets))


def simple_sentiment(title):
    title = title.lower()

    positive = ["hausse", "augmente", "gain", "croissance", "positif", "progression"]
    negative = ["baisse", "chute", "perte", "recul", "crise", "déclin"]

    score = 0

    for w in positive:
        if w in title:
            score += 1

    for w in negative:
        if w in title:
            score -= 1

    if score > 0:
        return "positive", score
    elif score < 0:
        return "negative", score
    else:
        return "neutral", 0


def get_articles(limit=10):
    try:
        r = requests.get(URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Request error:", e)
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    articles = []
    seen = set()

    for a in soup.select("a"):
        title = a.get_text(strip=True)
        href = a.get("href")

        if not title or not href:
            continue

        if "medias24.com" not in href:
            continue

        if len(title) < 30:
            continue

        if href in seen:
            continue

        seen.add(href)

        sentiment, score = simple_sentiment(title)

        articles.append({
            "titre": title,
            "lien": href,
            "actifs": detect_assets(title),
            "sentiment": sentiment,
            "score": score
        })

    return articles[:limit]