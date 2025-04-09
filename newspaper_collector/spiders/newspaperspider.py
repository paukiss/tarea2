import scrapy
from newspaper_collector.items import NewspaperItem
import uuid
import time
from datetime import datetime

# Importar las constantes desde constants.py
from newspaper_collector.spiders.constants import (
    ALLOWED_DOMAINS,
    ELDEBER_START_URL,
    LOSTIEMPOS_START_URL,
    AHORAELPUEBLO_START_URL
)


class NewspaperSpider(scrapy.Spider):
    name = "newspaper_spider"
    allowed_domains = ALLOWED_DOMAINS  # Se obtienen de constants.py

    def start_requests(self):
        """
        Lanzamos las URLs iniciales para cada periódico.
        Ajusta o agrega más URLs según tu necesidad.
        """
        # Eldeber (comienza en la página 1)
        yield scrapy.Request(
            url=ELDEBER_START_URL,
            callback=self.parse,
            meta={'page': 1, 'source': 'eldeber'}
        )

        # Los Tiempos (sección de "últimas noticias")
        yield scrapy.Request(
            url=LOSTIEMPOS_START_URL,
            callback=self.parse,
            meta={'page': 1, 'source': 'lostiempos'}
        )

        # Ahora El Pueblo (comienza en start=5, irá saltando de 5 en 5)
        yield scrapy.Request(
            url=AHORAELPUEBLO_START_URL,
            callback=self.parse,
            meta={'page': 5, 'source': 'ahoraelpueblo'}
        )

        time.sleep(1)  # Solo para ilustrar; en Scrapy no se recomienda usar sleep.

    def parse(self, response):
        if response.status != 200:
            self.logger.warning(f"Failed to fetch {response.url}: {response.status}")
            return

        source = response.meta.get("source", "")
        try:
            if "eldeber" in source or "eldeber" in response.url:
                yield from self.parse_eldeber(response)
            elif "lostiempos" in source or "lostiempos" in response.url:
                yield from self.parse_lostiempos(response)
            elif "ahoraelpueblo" in source or "ahoraelpueblo" in response.url:
                yield from self.parse_ahoraelpueblo(response)
            else:
                self.logger.warning(f"Fuente desconocida para URL: {response.url}")
        except Exception as e:
            self.logger.error(f"Error procesando {source} - {response.url}: {e}", exc_info=True)


    def parse_eldeber(self, response):
        """
        Lógica de parseo específica para Eldeber.
        Extrae noticias y controla la paginación (hasta página 5 en este ejemplo).
        """
        page = response.meta.get("page", 1)

        # Selecciona los bloques de noticia
        noticias = response.xpath('//div[contains(@class, "titulo-teaser-2col")]/a/h2/ancestor::article')
        for noticia in noticias:
            item = NewspaperItem()

            titulo = noticia.xpath('.//div[contains(@class, "titulo-teaser-2col")]/a/h2/text()').get(default="").strip()
            descripcion = " ".join(noticia.xpath('.//div[contains(@class, "entradilla-teaser-2col")]//text()').getall()).strip()
            fecha = noticia.xpath('.//div[contains(@class, "fecha-teaser-2col")]/div/time/text()').get(default="").strip()
            seccion = noticia.xpath('.//div[contains(@class, "seccion-teaser-2col")]/div/div/a/text()').get(default="").strip()
            relative_url = noticia.xpath('.//div[contains(@class, "titulo-teaser-2col")]/a/@href').get(default="")

            item["data_id"] = str(uuid.uuid4())
            item["titulo"] = titulo
            item["descripcion"] = descripcion
            item["fecha"] = fecha
            item["seccion"] = seccion
            item["url"] = response.urljoin(relative_url)
            item["date_saved"] = datetime.now().isoformat()

            yield item

        # Control de paginación hasta la página 5
        if page < 50:
            next_page = page + 1
            next_page_url = f"https://eldeber.com.bo/economia/{next_page}"
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'page': next_page, 'source': 'eldeber'}
            )

    def parse_lostiempos(self, response):
        """
        Lógica de parseo específica para Los Tiempos.
        Extrae las noticias y controla la paginación (hasta la página 8 en este ejemplo).
        """
        page = response.meta.get("page", 1)

        # Extrae los bloques de noticias
        noticias = response.xpath('//section[contains(@class, "pane-views-panes")]//div[contains(@class, "views-row")]')
        for noticia in noticias:
            item = NewspaperItem()

            titulo = noticia.xpath('.//div[contains(@class, "views-field-title term")]/a/text()').get(default="Sin título").strip()
            resumen = noticia.xpath('.//div[contains(@class, "views-field-field-noticia-sumario")]/span/text()').get(default="Sin resumen").strip()
            fecha = noticia.xpath('.//span[contains(@class, "views-field-field-noticia-fecha")]/span/text()').get(default="Sin fecha").strip()
            seccion = noticia.xpath('.//span[contains(@class, "views-field-seccion")]/span/a/text()').get(default="Sin sección").strip()
            url_noticia = noticia.xpath('.//div[contains(@class, "views-field-title term")]/a/@href').get(default="")

            item["data_id"] = str(uuid.uuid4())
            item["titulo"] = titulo
            item["descripcion"] = resumen
            item["fecha"] = fecha
            item["seccion"] = seccion
            item["url"] = response.urljoin(url_noticia)
            item["date_saved"] = datetime.now().isoformat()

            yield item

        # Paginación para Los Tiempos hasta la página 8
        if page < 8:
            next_page_href = response.xpath('//li[contains(@class, "pager-next")]/a/@href').get()
            if next_page_href:
                next_page_url = response.urljoin(next_page_href)
                yield scrapy.Request(
                    url=next_page_url,
                    callback=self.parse,
                    meta={'page': page + 1, 'source': 'lostiempos'}
                )

    def parse_ahoraelpueblo(self, response):
        """
        Lógica de parseo específica para Ahora El Pueblo.
        Avanza en saltos de 5 y se detiene cuando la paginación excede start=30.
        """
        try:
            noticias = response.xpath('//*[@id="sp-component"]/div/div[2]/div[2]/div/div/div')
            for noticia in noticias:
                try:
                    item = NewspaperItem()

                    item["data_id"] = str(uuid.uuid4())
                    item["titulo"] = noticia.xpath('.//div[2]/div[1]/h2/a/text()').get(default="").strip()
                    item["descripcion"] = " ".join(noticia.xpath('.//div[contains(@class, "article-introtext")]//text()').getall()).strip()
                    item["fecha"] = noticia.xpath('.//time/@datetime').get()
                    item["seccion"] = noticia.xpath('.//div[2]/div[2]/span[1]/a/text()').get(default="Sin sección")
                    
                    relative_url = noticia.xpath('.//div[2]/div[1]/h2/a/@href').get()
                    item["url"] = response.urljoin(relative_url)
                    item["date_saved"] = datetime.now().isoformat()

                    yield item

                except Exception as e:
                    self.logger.error(f"Error procesando una noticia: {e}")

            current_page = response.meta.get('page', 5)
            next_page = current_page + 5

            # Continuamos hasta start=30 como máximo
            if next_page <= 100:
                next_page_url = f"https://ahoraelpueblo.bo/index.php/nacional/economia?start={next_page}"
                yield scrapy.Request(
                    url=next_page_url,
                    callback=self.parse_ahoraelpueblo,
                    meta={'page': next_page}
                )

        except Exception as e:
            self.logger.error(f"Error en parse_ahoraelpueblo: {e}")
