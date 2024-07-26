import requests
import os


def scrape(scrape_url, autoparse=False, binary=False, ultra_premium=False, timeout=20):
    scraperapi_key = os.getenv("SCRAPERAPI_KEY")
    params = {
        "api_key": scraperapi_key,
        "url": scrape_url,
        "autoparse": autoparse,
        "binary_target": binary,
        "ultra_premium": ultra_premium,
    }
    return requests.get("http://api.scraperapi.com", params=params, timeout=timeout)
