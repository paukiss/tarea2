import scrapy
import uuid
from datetime import datetime
import json
import os

class NewspaperSpider(scrapy.Spider):
    name = "newspaper_spider"
    allowed_domains = ["eldeber.com.bo", "lostiempos.com", "ahoraelpueblo.bo"]

    custom_settings = {
        'FEED_FORMAT': 'json',
        'FEED_URI': 'news_output.json',
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'LOG_LEVEL': 'ERROR'
    }

    def start_requests(self):
        urls = [
            ("https://eldeber.com.bo/economia", 1, "eldeber"),
            ("https://www.lostiempos.com/actualidad/economia", 1, "lostiempos"),
            ("https://ahoraelpueblo.bo/index.php/nacional/economia?start=5", 5, "ahoraelpueblo")
        ]
        for url, page, source in urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={'page': page, 'source': source})

    def parse(self, response):
        source = response.meta['source']
        if "eldeber" in source:
            yield from self.parse_eldeber(response)
        elif "lostiempos" in source:
            yield from self.parse_lostiempos(response)
        elif "ahoraelpueblo" in source:
            yield from self.parse_ahoraelpueblo(response)

    def parse_eldeber(self, response):
        page = response.meta['page']
        noticias = response.xpath('//div[contains(@class, "titulo-teaser-2col")]/a/h2/ancestor::article')
        for noticia in noticias:
            yield self.build_item(
                titulo=noticia.xpath('.//div[contains(@class, "titulo-teaser-2col")]/a/h2/text()').get(""),
                descripcion=noticia.xpath('.//div[contains(@class, "entradilla-teaser-2col")]/div/p/text()').get(""),
                fecha=noticia.xpath('.//div[contains(@class, "fecha-teaser-2col")]/div/time/text()').get(""),
                seccion=noticia.xpath('.//div[contains(@class, "seccion-teaser-2col")]/div/div/a/text()').get(""),
                url=response.urljoin(noticia.xpath('.//div[contains(@class, "titulo-teaser-2col")]/a/@href').get(""))
            )
        if page < 5:
            next_page = page + 1
            yield scrapy.Request(
                url=f"https://eldeber.com.bo/economia/{next_page}",
                callback=self.parse,
                meta={'page': next_page, 'source': 'eldeber'}
            )

    def parse_lostiempos(self, response):
        page = response.meta['page']
        noticias = response.xpath('//section[contains(@class, "pane-views-panes")]//div[contains(@class, "views-row")]')
        for noticia in noticias:
            yield self.build_item(
                titulo=noticia.xpath('.//div[contains(@class, "views-field-title")]/a/text()').get(""),
                descripcion=noticia.xpath('.//div[contains(@class, "views-field-field-noticia-sumario")]/span/text()').get(""),
                fecha=noticia.xpath('.//span[contains(@class, "views-field-field-noticia-fecha")]/span/text()').get(""),
                seccion=noticia.xpath('.//span[contains(@class, "views-field-seccion")]/span/a/text()').get(""),
                url=response.urljoin(noticia.xpath('.//div[contains(@class, "views-field-title")]/a/@href').get(""))
            )
        if page < 8:
            next_page_href = response.xpath('//li[contains(@class, "pager-next")]/a/@href').get()
            if next_page_href:
                yield scrapy.Request(
                    url=response.urljoin(next_page_href),
                    callback=self.parse,
                    meta={'page': page + 1, 'source': 'lostiempos'}
                )

    def parse_ahoraelpueblo(self, response):
        page = response.meta['page']
        noticias = response.xpath('//*[@id="sp-component"]/div/div[2]/div[2]/div/div/div')
        for noticia in noticias:
            yield self.build_item(
                titulo=noticia.xpath('.//div[2]/div[1]/h2/a/text()').get(""),
                descripcion=noticia.xpath('.//div[contains(@class, "entradilla-teaser-2col")]/div/p/text()').get(""),
                fecha=noticia.xpath('.//time/@datetime').get(""),
                seccion=noticia.xpath('.//div[2]/div[2]/span[1]/a/text()').get(""),
                url=response.urljoin(noticia.xpath('.//div[2]/div[1]/h2/a/@href').get(""))
            )
        if page < 30:
            next_page = page + 5
            yield scrapy.Request(
                url=f"https://ahoraelpueblo.bo/index.php/nacional/economia?start={next_page}",
                callback=self.parse,
                meta={'page': next_page, 'source': 'ahoraelpueblo'}
            )

    def build_item(self, titulo, descripcion, fecha, seccion, url):
        return {
            "data_id": str(uuid.uuid4()),
            "titulo": titulo.strip(),
            "descripcion": descripcion.strip(),
            "fecha": fecha.strip(),
            "seccion": seccion.strip(),
            "url": url,
            "date_saved": datetime.now().isoformat()
        }

