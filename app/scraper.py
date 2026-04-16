import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

URL = "https://medias24.com/categorie/leboursier/actus/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

# ... (other functions remain same: detect_assets, simple_sentiment, estimate_price_trend)


def detect_assets(title):
    assets = []

    keywords = {
        "MASI": ["masi", "bourse"],
        "ATW": ["attijari"],
        "BCP": ["bcp", "banque populaire"],
        "MNG": ["managem"],
        "IAM": ["maroc telecom"]
    }

    title_lower = title.lower()

    for asset, words in keywords.items():
        for w in words:
            if w in title_lower:
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


def estimate_price_trend(articles):
    trend = {}

    for a in articles:
        for asset in a["assets"]:
            trend.setdefault(asset, 0)
            trend[asset] += a["score"]

    result = {}

    for asset, score in trend.items():
        if score > 0:
            direction = "hausse"
        elif score < 0:
            direction = "baisse"
        else:
            direction = "stable"

        result[asset] = {
            "direction": direction,
            "force": abs(score)
        }

    return result


def get_articles(limit=10):
    """
    Scrape articles from Medias24 'Le Boursier' section.
    Tries to capture Title, URL, and a short Snippet for context.
    """
    try:
        r = requests.get(URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        logger.error("Scraper request error: %s", e)
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    articles = []
    seen = set()

    # Medias24 'Le Boursier' section uses h1.title-actus and div.description-recent
    # We also check for the older .td-module-container just in case.
    for container in soup.select(".holde-actus-info, .description-recent, article, .td-module-container"):
        title_tag = container.select_one("h1.title-actus a, .td-module-title a, h3 a, h2 a, a")
        if not title_tag:
            continue
            
        title = title_tag.get_text(strip=True)
        href = title_tag.get("href")
        
        # Determine snippet - often a sibling div or inside description-recent
        snippet = ""
        snippet_tag = container.select_one("div.description-recent a, .td-excerpt, p")
        if snippet_tag:
            snippet = snippet_tag.get_text(strip=True)

        if not title or not href or href in seen:
            continue
        
        # Filter for article-like links (usually have date patterns or specific paths)
        if "medias24.com" not in href or len(title) < 20:
            continue

        seen.add(href)
        sentiment, score = simple_sentiment(title + " " + snippet)

        articles.append({
            "title": title,
            "url": href,
            "snippet": snippet,
            "assets": detect_assets(title),
            "sentiment": sentiment,
            "score": score
        })

        if len(articles) >= limit:
            break

    # Final fallback: if still empty, find any link with a date-like pattern in URL
    if not articles:
        import re
        for a in soup.find_all("a", href=True):
            href = a.get("href")
            title = a.get_text(strip=True)
            if re.search(r"/\d{4}/\d{2}/\d{2}/", href) and len(title) > 30 and href not in seen:
                seen.add(href)
                articles.append({
                    "title": title,
                    "url": href,
                    "snippet": "",
                    "assets": detect_assets(title),
                    "sentiment": "neutral",
                    "score": 0
                })
                if len(articles) >= limit: break

    return articles