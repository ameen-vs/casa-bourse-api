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

    # Broad container search:
    for container in soup.select(".td-module-container, .td_module_wrap, article, .td-block-span12"):
        title_tag = container.select_one(".td-module-title a, h3 a, h2 a, .entry-title a")
        if not title_tag:
            # Fallback for direct links
            title_tag = container.select_one("a[href*='medias24.com']")
            if not title_tag or len(title_tag.get_text(strip=True)) < 20: 
                continue
            
        title = title_tag.get_text(strip=True)
        href = title_tag.get("href")
        
        # Try to find a snippet
        snippet_tag = container.select_one(".td-excerpt, p, .entry-content p")
        snippet = snippet_tag.get_text(strip=True) if snippet_tag else ""

        if not title or not href or href in seen:
            continue
        
        if "medias24.com" not in href:
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

    return articles