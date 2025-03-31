import scrapy
from newspaper_collector.items import NewspaperItem
import uuid 
from data_web import BASE_URLS, ALLOWED_DOMAINS
import time 

class NewspaperspiderSpider(scrapy.Spider):
    name = "newspaperspider"
    allowed_domains = ALLOWED_DOMAINS
    start_urls = []

    def start_requests(self):
        # Generate initial requests with proper user agent headers.
        for url in BASE_URLS:
            if "trabajito" in url:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    headers=HEADERS
                )
            else:
                for city in CITIES:
                    # Construye la URL completa
                    full_url = f"{url}{city}"
                    # Realiza la solicitud con encabezados
                    yield scrapy.Request(
                        url=full_url,
                        callback=self.parse,
                        headers=HEADERS,
                        meta={"city": city}
                    )
            time.sleep(1)


    def parse(self, response):
        pass
