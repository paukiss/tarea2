ALLOWED_DOMAINS = [
    "eldeber.com.bo",
    "lostiempos.com",
    "ahoraelpueblo.bo"
]

ELDEBER_START_URL = "https://eldeber.com.bo/economia/1"
LOSTIEMPOS_START_URL = "https://www.lostiempos.com/ultimas-noticias"
AHORAELPUEBLO_START_URL = "https://ahoraelpueblo.bo/index.php/nacional/economia?start=5"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0'
}

# scrapy runspider jobspider.py 