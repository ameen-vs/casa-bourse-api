from app.scraper import get_articles

articles = get_articles()

for a in articles:
    print(a)