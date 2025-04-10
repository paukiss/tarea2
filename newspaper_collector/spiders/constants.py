# constantes.py

ALLOWED_DOMAINS = [
    "eldeber.com.bo",
    "lostiempos.com",
    "ahoraelpueblo.bo"
]

ELDEBER_SECTIONS = {
    "pais": "https://eldeber.com.bo/pais/{page}/",
    "opinion": "https://eldeber.com.bo/opinion/{page}/",
    "santa-cruz": "https://eldeber.com.bo/santa-cruz/{page}/",
    "mundo": "https://eldeber.com.bo/mundo/{page}/",
    "sports": "https://eldeber.com.bo/sports/{page}/",
    "educacion-y-sociedad": "https://eldeber.com.bo/educacion-y-sociedad/{page}/",
    "ultimas-noticias": "https://eldeber.com.bo/ultimas-noticias/{page}/",
    "economia": "https://eldeber.com.bo/economia/{page}/"
}
ELDEBER_PAGES_TO_SCRAPE = 10 

LOSTIEMPOS_START_URL = "https://www.lostiempos.com/ultimas-noticias"
LOSTIEMPOS_PAGES_TO_SCRAPE = 10

AHORAELPUEBLO_SECTIONS = {
    "seguridad": "https://ahoraelpueblo.bo/index.php/nacional/seguridad?start={start}",
    "sociedad": "https://ahoraelpueblo.bo/index.php/nacional/sociedad?start={start}",
    "deportes": "https://ahoraelpueblo.bo/index.php/nacional/deportes?start={start}",
    "culturas": "https://ahoraelpueblo.bo/index.php/nacional/culturas?start={start}",
    "politica": "https://ahoraelpueblo.bo/index.php/nacional/politica?start={start}",
    "economia": "https://ahoraelpueblo.bo/index.php/nacional/economia?start={start}"
}
AHORAELPUEBLO_PAGE_INCREMENT = 5 
AHORAELPUEBLO_PAGES_TO_SCRAPE = 10 

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