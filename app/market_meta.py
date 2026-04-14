"""Static reference facts for the Casablanca cash market (official portal)."""

MARKET_META = {
    "exchange": "Bourse de Casablanca",
    "official_portal_fr": "https://www.casablanca-bourse.com/fr",
    "official_api_host": "https://api.casablanca-bourse.com",
    "timezone": "Africa/Casablanca",
    "cash_session_local": {
        "pre_open": "09:00",
        "continuous": "09:30–15:30",
        "closing_auction": "15:30",
        "weekdays": "Sunday–Thursday (no daylight saving since 2018)",
    },
    "notes": (
        "Les horaires peuvent varier les jours fériés; voir la page « Jours fériés » sur le site officiel."
    ),
}
