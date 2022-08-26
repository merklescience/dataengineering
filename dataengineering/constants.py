from decouple import config

class ServerEnv:

    LOCAL = "local"
    DEV = "dev"
    PRODUCTION = "production"

class CoinPriceEnv:

    PRICING_SERVICE_URL = config.get("PRICING_SERVICE_URL")
    PRICING_SERVICE_TOKEN = config.get("PRICING_SERVICE_TOKEN")
